// Package router is the delivery half of the mesh: it turns "the agent said
// something" into "the surface shows it".
//
// Delivery is a subscription, not a return value. A run publishes its events to
// the bus once and hands nothing back to whoever tagged it; the router is just
// another subscriber, and it renders what it sees onto whichever surfaces the
// run's routes named. That indirection is the whole reason origin and
// destination are independent: a tag raised in GitHub can answer into Jira
// because nothing on the path from trigger to sink ever holds both ends at
// once. They share a run id and nothing else.
//
// The router owns exactly one piece of state, the map from a run to the routes
// it was accepted with and the channel its events are published on, and it is
// deliberately not durable. It subscribes once per (tenant, channel) that a
// registered run publishes on, and drops the subscription when the last run on
// it is released. The bus owns
// sequencing and retention, the connector owns how a surface is written, and
// anchor's ledger owns what happened. Losing a router loses in-flight
// renders, not runs: the runtime registers a run's routes at accept time and
// registers them again when it replays.
//
// # Lanes, and why they exist
//
// Delivery is N:M. One event fans out to every route that wants its kind, and
// one sink serves every run that names it. Both directions want concurrency,
// and exactly one pairing must not have it: events for one run going to one
// target are carried by a single goroutine reading a FIFO queue — a lane — so
// they arrive in Seq order. That is not fussiness. A Slack sink renders a run
// by editing one message, and two edits that overtake each other leave the
// wrong final text on the surface permanently, long after the run that
// produced them is forgotten. Across lanes there is no order to keep and none
// is imposed, so a stalled Jira sink cannot delay a webhook and one run's slow
// surface cannot delay another run's.
//
// Two routes naming the same target are one lane, and an event both of them
// select is delivered once. The target is the surface, and rendering it twice
// would show the work twice.
//
// # Transient and permanent
//
// The bus is at-least-once and sinks are required to tolerate redelivery, but
// at-least-once still has to mean at least once: a sink that returns a 503 is
// retried with bounded exponential backoff. What is never retried is a failure
// no amount of waiting can fix — connector.ErrUndeliverable and
// connector.ErrUnsupported (a cron schedule has no surface to render into, and
// a route to one is not a fault to recover from), connector.ErrNotFound, and a
// panic, which is a defect in the sink rather than congestion in front of it.
// A permanent failure retires that one delivery; the lane moves on to the next
// event rather than stranding the run behind a target that will never accept
// it.
//
// # Failure is loud
//
// Every delivery this package gives up on is logged at error level with the
// run, the target, the sequence, the kind and the attempt count, because a
// silently dropped delivery is the worst failure this package could have: the
// agent did the work, the ledger says so, and the human waiting on the surface
// sees nothing. Events for runs this router has no routes for are the one
// exception and are logged at debug level: several runs share a channel, and
// another node's runs on it are not this router's to deliver.
package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/event"
)

// Errors reported by the router. Callers match with errors.Is.
var (
	// ErrInvalid reports a registration the router cannot serve.
	ErrInvalid = errors.New("router: invalid")
	// ErrPanic reports a sink that panicked. It is permanent: a panic is a
	// defect in the sink, not congestion in front of it, and retrying a defect
	// only multiplies it.
	ErrPanic = errors.New("router: sink panicked")
	// ErrRunning reports a second concurrent Run on one router.
	ErrRunning = errors.New("router: already running")
)

// Defaults for the options below.
const (
	// DefaultBacklog is the per-lane queue depth: how many events for one
	// (run, target) pair may be waiting while that sink works.
	DefaultBacklog = 256
	// DefaultConcurrency bounds Deliver calls in flight across every lane.
	DefaultConcurrency = 64
	// DefaultAttempts is how many times one event is offered to one sink
	// before the delivery is retired, counting the first try.
	DefaultAttempts = 4
	// DefaultBackoff is the wait before the second attempt; it doubles from
	// there.
	DefaultBackoff = 100 * time.Millisecond
	// DefaultMaxBackoff caps the wait between attempts.
	DefaultMaxBackoff = 5 * time.Second
	// DefaultTimeout bounds one Deliver call. It exists so a sink that ignores
	// its context cannot hold a lane, or shutdown, open forever.
	DefaultTimeout = 30 * time.Second
)

// Resolver resolves a delivery target to the sink that can render onto it.
// *connector.Registry is what a deployment normally passes; the interface
// exists so a router can be pointed at a resolver that decides per tenant or
// per workspace without the registry growing that concern.
type Resolver interface {
	Sink(target address.Address) (connector.Sink, error)
}

var _ Resolver = (*connector.Registry)(nil)

// Option configures a Router. Non-positive and nil values are ignored, so a
// caller can pass a zero-valued config through without erasing the defaults.
type Option func(*Router)

// WithBacklog sets the per-lane queue depth (default 256). It is the only
// back-pressure a sink gets: when a lane's queue is full the event is dropped
// for that target and logged at error level, because the alternative is one
// unresponsive surface stalling every other surface and every other run.
func WithBacklog(n int) Option {
	return func(r *Router) {
		if n > 0 {
			r.backlog = n
		}
	}
}

// WithConcurrency bounds Deliver calls in flight across all lanes (default 64).
// A lane holds a slot only for one attempt and never across a backoff wait, so
// a sink that is retrying does not occupy the budget while it waits.
func WithConcurrency(n int) Option {
	return func(r *Router) {
		if n > 0 {
			r.concurrency = n
		}
	}
}

// WithAttempts sets how many times one event is offered to one sink before the
// delivery is retired, counting the first try (default 4). A permanent failure
// ends the delivery regardless of what is left.
func WithAttempts(n int) Option {
	return func(r *Router) {
		if n > 0 {
			r.attempts = n
		}
	}
}

// WithBackoff sets the wait before the second attempt and the cap it doubles
// towards (defaults 100ms and 5s). The schedule carries no jitter: lanes are
// independent and start their attempts at whatever moment their own event
// arrived, so there is no herd to spread.
func WithBackoff(base, max time.Duration) Option {
	return func(r *Router) {
		if base > 0 {
			r.backoff = base
		}
		if max > 0 {
			r.maxBackoff = max
		}
	}
}

// WithTimeout bounds one Deliver call (default 30s). Zero or negative disables
// it, which makes the router's shutdown only as prompt as its slowest sink.
func WithTimeout(d time.Duration) Option {
	return func(r *Router) { r.timeout = d }
}

// WithLogger sets the logger (default slog.Default). Permanent failures are
// logged at error level and dropped events for unrouted runs at debug level,
// so a deployment that wants to see the latter must enable debug.
func WithLogger(l *slog.Logger) Option {
	return func(r *Router) {
		if l != nil {
			r.log = l
		}
	}
}

// Router subscribes to the bus and delivers each event to the sinks a run's
// routes selected. One Router serves every run on a node; it is safe for
// concurrent use, and Register and Release may be called before, during and
// after Run.
type Router struct {
	bus   bus.Bus
	sinks Resolver
	log   *slog.Logger

	backlog     int
	concurrency int
	attempts    int
	backoff     time.Duration
	maxBackoff  time.Duration
	timeout     time.Duration

	// sem bounds Deliver calls in flight across every lane.
	sem chan struct{}

	// mu guards runs, channels, live, running and every lane's queue. Offers
	// are made under it so that a lane cannot be closed by Release between
	// the lookup and the send.
	mu   sync.Mutex
	runs map[string]*run
	// channels is every (tenant, channel) a registered run publishes on,
	// with how many runs do and, while Run is active, its subscriber.
	channels map[channelKey]*listener
	// runCtx is the context of the active Run, which every listener runs
	// under. failed reports a listener whose subscription cannot recover.
	runCtx context.Context
	failed chan error
	// live is every surface a lane goroutine is still working on, keyed by
	// (run, target), and it outlives r.runs: a released or re-registered run
	// drops its lanes from st.lanes immediately, while the goroutines drain on.
	// A new lane for a surface already in here chains behind the one it finds,
	// which is what keeps one surface to one goroutine across replacement.
	live    map[string]*lane
	running bool
	// wg tracks lane goroutines. Add is only ever called from the dispatch
	// loop, and running stays true until Wait has returned, so no other Run
	// can be dispatching while this one waits.
	wg sync.WaitGroup
}

// New returns a router that reads b and delivers through the sinks that resolve
// names. It starts nothing: call Run.
func New(b bus.Bus, sinks Resolver, opts ...Option) *Router {
	if b == nil || sinks == nil {
		panic("router: New requires a bus and a resolver")
	}
	r := &Router{
		bus:         b,
		sinks:       sinks,
		log:         slog.Default(),
		backlog:     DefaultBacklog,
		concurrency: DefaultConcurrency,
		attempts:    DefaultAttempts,
		backoff:     DefaultBackoff,
		maxBackoff:  DefaultMaxBackoff,
		timeout:     DefaultTimeout,
		runs:        make(map[string]*run),
		channels:    make(map[channelKey]*listener),
		live:        make(map[string]*lane),
	}
	for _, opt := range opts {
		opt(r)
	}
	r.sem = make(chan struct{}, r.concurrency)
	return r
}

// run is one run's delivery state: the routes it was accepted with, the lanes
// serving them, and how far its stream has been dispatched.
type run struct {
	at    channelKey
	bound []binding
	lanes map[string]*lane
	// seq is the highest sequence dispatched for this run. The bus is
	// at-least-once and a resubscribe replays whatever it still retains, so
	// the router keeps the per-run cursor the bus tells consumers to keep.
	seq uint64
}

// binding is one route with its target already resolved. Resolving at
// registration rather than per event keeps a misconfigured route to one log
// line instead of one per event, and keeps the dispatch loop free of a lookup
// that cannot change while the run lives.
type binding struct {
	route envelope.Route
	sink  connector.Sink
}

// Register records the routes a run delivers on, and the tenant and channel its
// events are published on. The runtime calls it at accept time, before it
// publishes the run's first event: an event that arrives for a run with no
// routes has nowhere to go and is dropped.
//
// Registering a run again replaces its routes and keeps its cursor, which is
// what replay needs — anchor re-runs a workflow from the top, and the routes
// it re-registers are the same ones it pinned. Lanes for targets that are no
// longer routed drain and exit.
//
// A route whose target no sink can receive is dropped here and logged, not
// returned as an error: it is permanent and knowable now (a cron schedule has
// no surface), and a run whose events cannot be rendered anywhere still runs
// and still publishes to the bus. Register reports ErrInvalid only for input
// the router could never make sense of: no run id, tenant or channel, or a
// route with no target.
func (r *Router) Register(runID, tenant, channel string, routes []envelope.Route) error {
	switch {
	case runID == "":
		return fmt.Errorf("%w: register with no run id", ErrInvalid)
	case tenant == "":
		return fmt.Errorf("%w: run %q names no tenant", ErrInvalid, runID)
	case channel == "" || event.IsControl(channel):
		return fmt.Errorf("%w: run %q is not on a channel the router can listen to: %q", ErrInvalid, runID, channel)
	}
	at := channelKey{tenant: tenant, channel: channel}
	bound := make([]binding, 0, len(routes))
	for i, route := range routes {
		if err := route.Validate(); err != nil {
			return fmt.Errorf("%w: run %q route %d: %w", ErrInvalid, runID, i, err)
		}
		sink, err := r.sinks.Sink(route.Target)
		if err != nil {
			r.log.Error("router: route has no sink, its events cannot be delivered",
				"run", runID,
				"target", route.Target.String(),
				"connector", route.Target.Connector,
				"error", err)
			continue
		}
		bound = append(bound, binding{route: route, sink: sink})
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.runs[runID]
	if st == nil {
		st = &run{at: at, lanes: make(map[string]*lane)}
		r.runs[runID] = st
		r.joinLocked(at)
	} else if st.at != at {
		r.leaveLocked(st.at)
		st.at = at
		r.joinLocked(at)
	}
	st.bound = bound
	for key, ln := range st.lanes {
		if !routed(bound, key) {
			ln.close()
			delete(st.lanes, key)
		}
	}
	return nil
}

// Release forgets a run's routes. Its lanes drain: whatever is already queued
// is still delivered, and each lane's goroutine exits once it is. Release does
// not wait for that — a finished run must not be able to block on the surface
// it was rendering into — but Run does not return until every lane it started
// has exited, so a Run that has returned has left nothing behind.
//
// Events that arrive for the run after Release are dropped, which is the
// correct reading of a released run: nothing is listening for it here any more.
func (r *Router) Release(runID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.runs[runID]
	if st == nil {
		return
	}
	delete(r.runs, runID)
	r.leaveLocked(st.at)
	for _, ln := range st.lanes {
		ln.close()
	}
}

// Run subscribes to the bus and dispatches until ctx is cancelled. It holds
// one subscription per (tenant, channel) a registered run publishes on,
// starting one when a run registers on a new channel and ending it when the
// last run on it is released. It returns nil on a clean stop and an error only
// if a subscription failed in a way resubscribing cannot fix.
//
// Falling behind the bus is not such a failure: ErrSlowConsumer is answered by
// resubscribing, and the per-run cursor the router keeps means the replay that
// follows costs duplicates rather than double deliveries.
//
// Only one Run may be active per router; a second returns ErrRunning. When Run
// returns, every goroutine it started has exited.
func (r *Router) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return ErrRunning
	}
	r.running = true
	r.runCtx = ctx
	r.failed = make(chan error, 1)
	for key, l := range r.channels {
		r.listenLocked(key, l)
	}
	failed := r.failed
	r.mu.Unlock()

	defer func() {
		cancel()
		r.mu.Lock()
		r.runCtx = nil
		for _, l := range r.channels {
			l.stop = nil
		}
		// Cancellation abandons what is queued rather than draining it, and
		// the lanes are forgotten as well as closed: a run stays registered
		// across a restart, but its lanes belong to the Run that started
		// them.
		for _, st := range r.runs {
			for key, ln := range st.lanes {
				ln.close()
				delete(st.lanes, key)
			}
		}
		r.mu.Unlock()

		// The router stays claimed until every goroutine has exited, so a
		// second Run cannot call wg.Add while this one waits.
		r.wg.Wait()
		r.mu.Lock()
		r.running = false
		r.mu.Unlock()
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-failed:
		return err
	}
}

// channelKey is one channel of one tenant.
type channelKey struct{ tenant, channel string }

// listener is the subscription to one channel: how many registered runs
// publish on it, and the function that ends its subscriber while Run is
// active.
type listener struct {
	runs int
	stop context.CancelFunc
}

// joinLocked counts a run on a channel, subscribing to it if it is new and
// Run is active. Callers hold r.mu.
func (r *Router) joinLocked(key channelKey) {
	l := r.channels[key]
	if l == nil {
		l = &listener{}
		r.channels[key] = l
	}
	l.runs++
	if r.runCtx != nil && l.stop == nil {
		r.listenLocked(key, l)
	}
}

// leaveLocked uncounts a run on a channel, ending its subscription once no
// registered run publishes on it. Callers hold r.mu.
func (r *Router) leaveLocked(key channelKey) {
	l := r.channels[key]
	if l == nil {
		return
	}
	l.runs--
	if l.runs > 0 {
		return
	}
	if l.stop != nil {
		l.stop()
	}
	delete(r.channels, key)
}

// listenLocked starts the subscriber of one channel under the active Run.
// Callers hold r.mu.
func (r *Router) listenLocked(key channelKey, l *listener) {
	ctx, stop := context.WithCancel(r.runCtx)
	l.stop = stop
	failed := r.failed
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer stop()
		if err := r.listen(ctx, key); err != nil {
			select {
			case failed <- err:
			default:
			}
		}
	}()
}

// subscriptionFor is what the router listens to on one channel: every run
// event, the opt-in families included, since a route may select them.
func subscriptionFor(key channelKey) bus.Subscription {
	return bus.Subscription{
		Tenant:   key.tenant,
		Channels: []string{key.channel},
		Types:    []string{"run", "run.usage", "run.context"},
	}
}

// listen reads one channel until ctx ends, resubscribing after falling
// behind. It returns an error only for a subscription that cannot recover.
func (r *Router) listen(ctx context.Context, key channelKey) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		stream, err := r.bus.Subscribe(ctx, subscriptionFor(key))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("router: subscribe %s: %w", key.channel, err)
		}
		err = r.consume(ctx, key, stream)
		_ = stream.Close()

		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, bus.ErrClosed):
			// Nobody here closed it, so something else decided delivery is
			// over. Say so: a router that stops quietly looks exactly like an
			// agent with nothing to say.
			r.log.Warn("router: bus stream ended, delivery has stopped",
				"channel", key.channel, "error", err)
			return nil
		case errors.Is(err, bus.ErrSlowConsumer):
			r.log.Warn("router: fell behind the bus, resubscribing",
				"channel", key.channel, "error", err)
		default:
			return fmt.Errorf("router: recv on %s: %w", key.channel, err)
		}
	}
}

// consume reads one subscription to exhaustion. It returns the error that ended
// it, which is always non-nil: a stream ends by failing.
func (r *Router) consume(ctx context.Context, key channelKey, stream bus.Stream) error {
	for {
		e, err := stream.Recv(ctx)
		if err != nil {
			return err
		}
		// A prefix subscription also sees a channel beneath this one, which
		// has a listener of its own; each event is dispatched once, by the
		// listener of its own channel.
		if e.Channel != key.channel || e.Tenant != key.tenant {
			continue
		}
		r.dispatch(ctx, e)
	}
}

// dispatch hands one event to every lane that wants it. It holds r.mu for the
// whole fan-out: the offers are non-blocking sends, and doing them under the
// same lock that Release closes lanes with is what makes "deliver to a lane
// that was just released" impossible rather than merely unlikely.
func (r *Router) dispatch(ctx context.Context, e event.Envelope) {
	if event.IsControl(e.Channel) || event.Family(e.Type) != "run" {
		// A request addressed to a run, not something a run produced. It is
		// for the worker executing the run and is never delivered to a
		// surface.
		return
	}
	runID := e.Run

	r.mu.Lock()
	defer r.mu.Unlock()

	st := r.runs[runID]
	if st == nil || st.at.tenant != e.Tenant {
		r.log.Debug("router: event for a run this router has no routes for, dropped",
			"run", runID, "agent", e.Agent, "seq", e.Seq, "type", e.Type)
		return
	}
	if e.Seq != 0 && e.Seq <= st.seq {
		return // a redelivery or a replay: already dispatched
	}
	st.seq = e.Seq

	var offered []*lane
	for _, b := range st.bound {
		if !b.route.Wants(e.Type) {
			continue
		}
		ln := r.laneFor(ctx, st, runID, b)
		if contains(offered, ln) {
			continue // two routes, one surface, one delivery
		}
		offered = append(offered, ln)
		ln.offer(e)
	}
}

// laneFor returns the lane for b's target, starting it on the first event that
// selects it: a run that never emits a kind a route wants never costs a
// goroutine. A lane whose predecessor on the same surface is still draining
// chains behind it, so the surface keeps one writer. Callers hold r.mu.
func (r *Router) laneFor(ctx context.Context, st *run, runID string, b binding) *lane {
	key := b.route.Target.String()
	if ln := st.lanes[key]; ln != nil {
		return ln
	}
	ln := &lane{
		r:      r,
		key:    surface(runID, key),
		runID:  runID,
		target: b.route.Target,
		sink:   b.sink,
		queue:  make(chan event.Envelope, r.backlog),
		done:   make(chan struct{}),
	}
	if prev := r.live[ln.key]; prev != nil {
		ln.after = prev.done
	}
	r.live[ln.key] = ln
	st.lanes[key] = ln
	r.wg.Add(1)
	go ln.serve(ctx)
	return ln
}

// surface names one (run, target) pair. The separator is a byte no address and
// no run id can carry, so two surfaces cannot collide by concatenation.
func surface(runID, target string) string { return runID + "\x00" + target }

// acquire takes a slot in the delivery budget.
func (r *Router) acquire(ctx context.Context) error {
	select {
	case r.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Router) release() { <-r.sem }

// routed reports whether key is still the target of one of these bindings.
func routed(bound []binding, key string) bool {
	for _, b := range bound {
		if b.route.Target.String() == key {
			return true
		}
	}
	return false
}

// contains reports whether ln has already been offered this event. Runs carry a
// handful of routes, so a scan beats a set.
func contains(lanes []*lane, ln *lane) bool {
	for _, have := range lanes {
		if have == ln {
			return true
		}
	}
	return false
}
