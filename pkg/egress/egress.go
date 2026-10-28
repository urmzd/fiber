// Package egress is the delivery half of the mesh: it turns "the agent said
// something" into "the surface shows it".
//
// Every egress adapter is a bus subscriber. A Sink that does not name its own
// subscriptions receives the run events on its own scheme's channels
// (slack://, github://, jira://) and renders each one on the channel it was
// published on, which is the channel of the event that started the run. A
// Sink that implements connector.Subscriber, such as the outbound webhook,
// names its channels, its type prefixes and its target. Nothing is registered
// per run and nothing chooses a surface: delivery follows the channel.
//
// Types are type prefixes ("run"), so an opt-in family ("run.usage",
// "run.context") reaches an adapter only when it names it. A control channel
// is never delivered to a surface.
//
// Egress owns no durable state. The bus owns sequencing and retention, the
// connector owns how a surface is written, and anchor's ledger owns what
// happened. On (re)subscribing, events older than the maximum age are
// skipped, so a restart does not render a day of retained runs again, and a
// sequence a run already reached is never delivered twice by one Engine.
//
// # Lanes, and why they exist
//
// Events for one run going to one target are carried by a single goroutine
// reading a FIFO queue, a lane, so they arrive in sequence order. That is not
// fussiness. A Slack sink renders a run by editing one message, and two edits
// that overtake each other leave the wrong final text on the surface
// permanently. Across lanes there is no order to keep and none is imposed, so a
// stalled Jira sink cannot delay a webhook and one run's slow surface cannot
// delay another run's. A lane ends when its run ends, or when it has been idle
// for a while; the next event for the run starts a new one.
//
// # Transient and permanent
//
// A sink that returns a transient error is retried with bounded exponential
// backoff. What is never retried is a failure no amount of waiting can fix:
// connector.ErrUndeliverable, connector.ErrUnsupported, connector.ErrNotFound,
// and a panic, which is a defect in the sink rather than congestion in front
// of it. A permanent failure retires that one delivery; the lane moves on.
//
// # Failure is loud
//
// Every delivery this package gives up on is logged at error level with the
// run, the target, the sequence, the type and the attempt count, because a
// silently dropped delivery is the worst failure this package could have: the
// agent did the work, the ledger says so, and the human waiting on the surface
// sees nothing.
package egress

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/event"
)

// Errors reported by egress. Callers match with errors.Is.
var (
	// ErrInvalid reports a configuration egress cannot serve.
	ErrInvalid = errors.New("egress: invalid")
	// ErrPanic reports a sink that panicked while delivering.
	ErrPanic = errors.New("egress: sink panicked")
	// ErrRunning reports a second Run for a tenant that is already running.
	ErrRunning = errors.New("egress: already running")
)

// Defaults.
const (
	// DefaultBacklog is the queue depth of one lane.
	DefaultBacklog = 256
	// DefaultConcurrency bounds sink calls in flight at once.
	DefaultConcurrency = 64
	// DefaultAttempts is how many times one event is offered to a sink.
	DefaultAttempts = 4
	// DefaultBackoff is the wait before the second attempt; it doubles.
	DefaultBackoff = 100 * time.Millisecond
	// DefaultMaxBackoff caps the wait between attempts.
	DefaultMaxBackoff = 5 * time.Second
	// DefaultTimeout bounds one sink call.
	DefaultTimeout = 30 * time.Second
	// DefaultMaxAge is how old an event may be and still be delivered.
	DefaultMaxAge = 15 * time.Minute
	// DefaultIdle is how long a lane waits for its run's next event.
	DefaultIdle = time.Minute
	// DefaultMaxRuns bounds how many runs' sequence cursors are kept.
	DefaultMaxRuns = 4096
)

// RunTypes is what a sink with no subscriptions of its own receives: every
// run type that is not opt-in.
var RunTypes = []string{"run"}

// Resolver is where egress finds its adapters. *connector.Registry is one.
type Resolver interface {
	Sinks() []connector.Sink
}

var _ Resolver = (*connector.Registry)(nil)

// Option configures an Engine.
type Option func(*Engine)

// WithBacklog sets the queue depth of one lane.
func WithBacklog(n int) Option {
	return func(e *Engine) {
		if n > 0 {
			e.backlog = n
		}
	}
}

// WithConcurrency bounds sink calls in flight at once.
func WithConcurrency(n int) Option {
	return func(e *Engine) {
		if n > 0 {
			e.concurrency = n
		}
	}
}

// WithAttempts sets how many times one event is offered to a sink.
func WithAttempts(n int) Option {
	return func(e *Engine) {
		if n > 0 {
			e.attempts = n
		}
	}
}

// WithBackoff sets the retry backoff, doubling from base to max.
func WithBackoff(base, max time.Duration) Option {
	return func(e *Engine) {
		if base > 0 {
			e.backoff = base
		}
		if max > 0 {
			e.maxBackoff = max
		}
	}
}

// WithTimeout bounds one sink call. Zero or less means no bound.
func WithTimeout(d time.Duration) Option {
	return func(e *Engine) { e.timeout = d }
}

// WithMaxAge sets how old an event may be and still be delivered.
func WithMaxAge(d time.Duration) Option {
	return func(e *Engine) {
		if d > 0 {
			e.maxAge = d
		}
	}
}

// WithIdle sets how long a lane waits for its run's next event before it
// ends.
func WithIdle(d time.Duration) Option {
	return func(e *Engine) {
		if d > 0 {
			e.idle = d
		}
	}
}

// WithClock sets the clock the maximum age is measured with.
func WithClock(now func() time.Time) Option {
	return func(e *Engine) {
		if now != nil {
			e.now = now
		}
	}
}

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option {
	return func(e *Engine) {
		if l != nil {
			e.log = l
		}
	}
}

// Engine delivers run events to egress adapters.
type Engine struct {
	bus   bus.Bus
	sinks Resolver
	log   *slog.Logger
	now   func() time.Time

	backlog     int
	concurrency int
	attempts    int
	backoff     time.Duration
	maxBackoff  time.Duration
	timeout     time.Duration
	maxAge      time.Duration
	idle        time.Duration

	sem chan struct{}

	mu      sync.Mutex
	running map[string]bool
	lanes   map[laneKey]*lane
	cursors *cursors
}

// New returns an Engine over b that delivers to the sinks r resolves.
func New(b bus.Bus, r Resolver, opts ...Option) *Engine {
	if b == nil || r == nil {
		panic("egress: New requires a bus and a resolver")
	}
	e := &Engine{
		bus:         b,
		sinks:       r,
		log:         slog.New(slog.DiscardHandler),
		now:         time.Now,
		backlog:     DefaultBacklog,
		concurrency: DefaultConcurrency,
		attempts:    DefaultAttempts,
		backoff:     DefaultBackoff,
		maxBackoff:  DefaultMaxBackoff,
		timeout:     DefaultTimeout,
		maxAge:      DefaultMaxAge,
		idle:        DefaultIdle,
		running:     map[string]bool{},
		lanes:       map[laneKey]*lane{},
		cursors:     newCursors(DefaultMaxRuns),
	}
	for _, o := range opts {
		o(e)
	}
	e.sem = make(chan struct{}, e.concurrency)
	return e
}

// Binding is one adapter's subscription, as egress serves it.
type Binding struct {
	Sink connector.Sink
	connector.Subscription
}

// Bindings returns what each sink receives: its own subscriptions when it is
// a connector.Subscriber, and otherwise RunTypes on its own scheme's
// channels, rendered on each event's channel.
func Bindings(r Resolver) []Binding {
	var out []Binding
	for _, s := range r.Sinks() {
		if sub, ok := s.(connector.Subscriber); ok {
			for _, x := range sub.Subscriptions() {
				out = append(out, Binding{Sink: s, Subscription: x})
			}
			continue
		}
		out = append(out, Binding{Sink: s, Subscription: connector.Subscription{
			Channels: []string{s.Name() + "://"},
			Types:    slices.Clone(RunTypes),
		}})
	}
	return out
}

// Run delivers tenant's run events until ctx is done, and then waits for its
// lanes to stop. A cancelled ctx is not a failure. Another Run for the same
// tenant at once is ErrRunning.
func (e *Engine) Run(ctx context.Context, tenant string) error {
	if tenant == "" {
		return fmt.Errorf("%w: no tenant", ErrInvalid)
	}
	e.mu.Lock()
	if e.running[tenant] {
		e.mu.Unlock()
		return ErrRunning
	}
	e.running[tenant] = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.running, tenant)
		e.mu.Unlock()
	}()

	// Fail closed: a subscription egress cannot serve is a configuration
	// error, not a sink that silently renders nothing.
	bindings := Bindings(e.sinks)
	for _, b := range bindings {
		if err := validBinding(b); err != nil {
			return fmt.Errorf("egress: sink %s: %w", b.Sink.Name(), err)
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	failed := make(chan error, 1)
	var wg, lanes sync.WaitGroup
	for i, b := range bindings {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.listen(ctx, &lanes, tenant, i, b); err != nil {
				select {
				case failed <- err:
				default:
				}
				cancel()
			}
		}()
	}
	var err error
	select {
	case <-ctx.Done():
	case err = <-failed:
	}
	cancel()
	wg.Wait()
	lanes.Wait()
	return err
}

func validBinding(b Binding) error {
	switch {
	case len(b.Channels) == 0:
		return fmt.Errorf("%w: no channel", ErrInvalid)
	case len(b.Types) == 0:
		return fmt.Errorf("%w: no type", ErrInvalid)
	case !b.Target.IsZero() && b.Target.Connector != b.Sink.Name():
		return fmt.Errorf("%w: target %s is not %s's", ErrInvalid, b.Target, b.Sink.Name())
	}
	return nil
}

// listen subscribes one binding and resubscribes after it falls behind.
func (e *Engine) listen(ctx context.Context, lanes *sync.WaitGroup, tenant string, id int, b Binding) error {
	sub := bus.Subscription{Tenant: tenant, Channels: slices.Clone(b.Channels), Types: slices.Clone(b.Types)}
	for {
		if ctx.Err() != nil {
			return nil
		}
		stream, err := e.bus.Subscribe(ctx, sub)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("egress: subscribe %s: %w", b.Sink.Name(), err)
		}
		err = e.consume(ctx, lanes, tenant, id, b, stream)
		_ = stream.Close()
		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, bus.ErrClosed):
			e.log.Warn("egress: bus stream ended, delivery has stopped", "sink", b.Sink.Name(), "error", err)
			return nil
		case errors.Is(err, bus.ErrSlowConsumer):
			e.log.Warn("egress: fell behind the bus, resubscribing", "sink", b.Sink.Name(), "error", err)
		default:
			return fmt.Errorf("egress: recv for %s: %w", b.Sink.Name(), err)
		}
	}
}

func (e *Engine) consume(ctx context.Context, lanes *sync.WaitGroup, tenant string, id int, b Binding, stream bus.Stream) error {
	for {
		ev, err := stream.Recv(ctx)
		if err != nil {
			return err
		}
		if ev.Tenant != tenant {
			continue
		}
		e.dispatch(ctx, lanes, id, b, ev)
	}
}

// dispatch offers one event to its lane.
func (e *Engine) dispatch(ctx context.Context, lanes *sync.WaitGroup, id int, b Binding, ev event.Envelope) {
	if event.IsControl(ev.Channel) || !selected(b, ev) {
		return
	}
	if age := e.now().Sub(ev.Time); age > e.maxAge {
		e.log.Debug("egress: event too old to deliver", "sink", b.Sink.Name(), "run", ev.Run, "seq", ev.Seq, "age", age)
		return
	}
	target := b.Target
	if target.IsZero() {
		t, err := address.Parse(ev.Channel)
		if err != nil || t.Connector != b.Sink.Name() {
			// Only a sink's own scheme is rendered on the event's channel.
			return
		}
		target = t
	}
	order := ev.Run
	if order == "" {
		order = "channel:" + ev.Channel
	}
	key := laneKey{binding: id, tenant: ev.Tenant, order: order, target: target.String()}

	e.mu.Lock()
	defer e.mu.Unlock()
	if ev.Run != "" && ev.Seq != 0 && !e.cursors.advance(key.cursor(), ev.Seq) {
		return // a redelivery or a replay: already dispatched
	}
	ln := e.lanes[key]
	if ln == nil {
		ln = &lane{
			e:      e,
			key:    key,
			sink:   b.Sink,
			target: target,
			queue:  make(chan event.Envelope, e.backlog),
		}
		e.lanes[key] = ln
		lanes.Add(1)
		go func() {
			defer lanes.Done()
			ln.serve(ctx)
		}()
	}
	ln.offer(ev)
}

// selected re-checks the subscription, so a backend that delivers more than
// was asked for never reaches a sink.
func selected(b Binding, ev event.Envelope) bool {
	return slices.ContainsFunc(b.Channels, func(p string) bool { return event.ChannelHasPrefix(ev.Channel, p) }) &&
		slices.ContainsFunc(b.Types, func(p string) bool { return event.Selects(p, ev.Type) })
}

// laneKey identifies one lane: a binding, a run (or a channel for events
// outside a run) and a target.
type laneKey struct {
	binding int
	tenant  string
	order   string
	target  string
}

func (k laneKey) cursor() string {
	return fmt.Sprintf("%d\x00%s\x00%s\x00%s", k.binding, k.tenant, k.order, k.target)
}

// terminal reports a type after which a run publishes nothing more.
func terminal(t string) bool {
	switch t {
	case event.TypeCompleted, event.TypeFailed, event.TypeStopped:
		return true
	}
	return false
}

func (e *Engine) acquire(ctx context.Context) error {
	select {
	case e.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) release() { <-e.sem }

func (e *Engine) backoffFor(attempt int) time.Duration {
	wait := e.backoff
	for i := 1; i < attempt; i++ {
		if wait >= e.maxBackoff/2 {
			return e.maxBackoff
		}
		wait *= 2
	}
	return wait
}

// cursors is the highest sequence dispatched per lane cursor, bounded by
// evicting the least recently advanced.
type cursors struct {
	max   int
	order *list.List
	byKey map[string]*list.Element
}

type cursor struct {
	key string
	seq uint64
}

func newCursors(max int) *cursors {
	return &cursors{max: max, order: list.New(), byKey: map[string]*list.Element{}}
}

// advance records seq for key and reports whether it is new.
func (c *cursors) advance(key string, seq uint64) bool {
	if el, ok := c.byKey[key]; ok {
		cur := el.Value.(*cursor)
		if seq <= cur.seq {
			return false
		}
		cur.seq = seq
		c.order.MoveToFront(el)
		return true
	}
	c.byKey[key] = c.order.PushFront(&cursor{key: key, seq: seq})
	for c.order.Len() > c.max {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.byKey, oldest.Value.(*cursor).key)
	}
	return true
}
