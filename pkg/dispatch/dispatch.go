// Package dispatch wakes agents. It listens to the global bus for one
// tenant's ingress events and starts a run for each channel member whose rule
// matches.
//
// For each event it drops:
//
//   - the subscriber that produced it, so an agent's own output never wakes
//     itself;
//   - the event at the hop limit, so two agents cannot answer each other
//     forever;
//   - every member whose Rule does not match (default deny: no membership,
//     no run).
//
// Every other member gets runtime.Accept(trigger, member). The run id is
// runtime.RunKey(tenant, event id, member), so a redelivered event joins the
// run it started and two members of one channel get two runs. Delivery is
// at-least-once and nothing here deduplicates: the run key does.
//
// slack.session.stopped.v1 starts nothing. It is mapped to a stop request
// (control.stop.v1) for each run this dispatcher started on the session's
// channel.
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/membership"
	"github.com/urmzd/fiber/pkg/runtime"
)

// ErrInvalid reports a dispatcher or tenant that cannot be served.
var ErrInvalid = errors.New("dispatch: invalid")

// Families are the ingress type prefixes a dispatcher subscribes to.
var Families = []string{"message", "slack", "github", "jira", "cron"}

// DefaultChannels are the channel prefixes a dispatcher subscribes to: the
// roots of every surface fiber has an ingress adapter for.
var DefaultChannels = []string{"slack://", "github://", "jira://", "cron://", "webhook://", "fiber://"}

// Defaults for the options below.
const (
	// DefaultMaxAge is how old an event may be when it is received and still
	// start a run. It keeps a resubscribe from waking members for what the
	// bus retained long ago.
	DefaultMaxAge = 15 * time.Minute
	// DefaultRunsPerChannel is how many recent runs per channel a stop on
	// the channel's session reaches.
	DefaultRunsPerChannel = 32
	// DefaultChannelsTracked bounds how many channels the recent runs are
	// kept for.
	DefaultChannelsTracked = 4096
)

// Runtime is what a dispatcher needs of the runtime. *runtime.Runtime
// implements it.
type Runtime interface {
	Accept(ctx context.Context, trigger event.Envelope, subscriber string) (runtime.Acceptance, error)
	Stop(ctx context.Context, req runtime.StopRequest) error
}

// Dispatcher wakes the members of a channel for the events on it.
type Dispatcher struct {
	bus      bus.Bus
	members  membership.Store
	rt       Runtime
	log      *slog.Logger
	channels []string
	maxHops  int
	maxAge   time.Duration
	now      func() time.Time

	runs *recent
}

// Option configures a Dispatcher.
type Option func(*Dispatcher)

// WithChannels replaces the channel prefixes subscribed to.
func WithChannels(prefixes ...string) Option {
	return func(d *Dispatcher) {
		if len(prefixes) > 0 {
			d.channels = slices.Clone(prefixes)
		}
	}
}

// WithMaxHops sets the hop limit (default bus.DefaultMaxHops). An event with
// this many hops starts nothing: what it starts would carry one more.
func WithMaxHops(n int) Option {
	return func(d *Dispatcher) {
		if n > 0 {
			d.maxHops = n
		}
	}
}

// WithMaxAge sets how old a received event may be and still start a run
// (default DefaultMaxAge).
func WithMaxAge(age time.Duration) Option {
	return func(d *Dispatcher) {
		if age > 0 {
			d.maxAge = age
		}
	}
}

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option {
	return func(d *Dispatcher) {
		if l != nil {
			d.log = l
		}
	}
}

// WithClock replaces the clock the age of an event is measured with.
func WithClock(now func() time.Time) Option {
	return func(d *Dispatcher) {
		if now != nil {
			d.now = now
		}
	}
}

// New returns a dispatcher. Every argument is required.
func New(b bus.Bus, members membership.Store, rt Runtime, opts ...Option) (*Dispatcher, error) {
	switch {
	case b == nil:
		return nil, fmt.Errorf("%w: no bus", ErrInvalid)
	case members == nil:
		return nil, fmt.Errorf("%w: no membership store", ErrInvalid)
	case rt == nil:
		return nil, fmt.Errorf("%w: no runtime", ErrInvalid)
	}
	d := &Dispatcher{
		bus:      b,
		members:  members,
		rt:       rt,
		log:      slog.New(slog.DiscardHandler),
		channels: slices.Clone(DefaultChannels),
		maxHops:  bus.DefaultMaxHops,
		maxAge:   DefaultMaxAge,
		now:      time.Now,
		runs:     newRecent(DefaultChannelsTracked, DefaultRunsPerChannel),
	}
	for _, o := range opts {
		o(d)
	}
	return d, nil
}

// Subscription is what the dispatcher listens to for tenant.
func (d *Dispatcher) Subscription(tenant string) bus.Subscription {
	return bus.Subscription{Tenant: tenant, Channels: slices.Clone(d.channels), Types: slices.Clone(Families)}
}

// Run dispatches tenant's events until ctx ends. It resubscribes after
// falling behind, and returns nil once ctx is done.
func (d *Dispatcher) Run(ctx context.Context, tenant string) error {
	if tenant == "" {
		return fmt.Errorf("%w: no tenant", ErrInvalid)
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		stream, err := d.bus.Subscribe(ctx, d.Subscription(tenant))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("dispatch: subscribe %s: %w", tenant, err)
		}
		err = d.consume(ctx, tenant, stream)
		_ = stream.Close()
		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, bus.ErrSlowConsumer):
			d.log.Warn("dispatch: fell behind the bus, resubscribing", "tenant", tenant, "error", err)
		case errors.Is(err, bus.ErrClosed):
			d.log.Warn("dispatch: bus stream ended, nothing is dispatched", "tenant", tenant)
			return nil
		default:
			return fmt.Errorf("dispatch: recv for %s: %w", tenant, err)
		}
	}
}

func (d *Dispatcher) consume(ctx context.Context, tenant string, stream bus.Stream) error {
	for {
		e, err := stream.Recv(ctx)
		if err != nil {
			return err
		}
		if e.Tenant != tenant {
			continue
		}
		if age := d.now().Sub(e.Time); age > d.maxAge {
			d.log.Debug("dispatch: event too old to start a run", "event", e.ID, "type", e.Type, "age", age)
			continue
		}
		if _, err := d.Handle(ctx, e); err != nil {
			d.log.Error("dispatch: event", "event", e.ID, "type", e.Type, "channel", e.Channel, "error", err)
		}
	}
}

// Handle dispatches one event and returns the runs it started or joined.
// The error joins every member that could not be accepted; the members that
// could were.
func (d *Dispatcher) Handle(ctx context.Context, e event.Envelope) ([]runtime.Acceptance, error) {
	if e.Tenant == "" || event.IsControl(e.Channel) {
		return nil, nil
	}
	if e.Type == event.TypeSlackSessionStopped {
		return nil, d.stopSession(ctx, e)
	}
	if _, ok := event.RequestOf(e.Data); !ok {
		// An event that asks nothing of an agent wakes nobody.
		return nil, nil
	}
	if e.Hops >= d.maxHops {
		d.log.Warn("dispatch: event at the hop limit starts nothing", "event", e.ID, "hops", e.Hops, "limit", d.maxHops)
		return nil, nil
	}
	members, err := d.members.Members(ctx, e.Tenant, e.Channel)
	if err != nil {
		return nil, fmt.Errorf("dispatch: members of %s: %w", e.Channel, err)
	}
	var (
		out  []runtime.Acceptance
		errs []error
	)
	for _, m := range members {
		if producedBy(e, m.Agent) || !m.Rule.Matches(m.Agent, e) {
			continue
		}
		acc, err := d.rt.Accept(ctx, e, m.Agent)
		if acc.RunID != "" {
			d.runs.add(e.Tenant, e.Channel, acc.RunID)
			out = append(out, acc)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("dispatch: accept %s for %s: %w", e.ID, m.Agent, err))
		}
	}
	return out, errors.Join(errs...)
}

// producedBy reports whether agent produced e: it is the event's agent, or
// the bot that wrote it.
func producedBy(e event.Envelope, agent string) bool {
	if e.Agent == agent {
		return true
	}
	r, _ := event.RequestOf(e.Data)
	return r.Actor.Bot && r.Actor.ID == agent
}

// stopSession asks every run started on the stopped session's channel to stop.
func (d *Dispatcher) stopSession(ctx context.Context, e event.Envelope) error {
	b, _ := e.Data.(event.SlackSessionStopped)
	by := ""
	if b.User != "" {
		by = "slack:" + b.User
	}
	var errs []error
	for _, runID := range d.runs.on(e.Tenant, e.Channel) {
		if err := d.rt.Stop(ctx, runtime.StopRequest{Tenant: e.Tenant, RunID: runID, Reason: "Stopped in Slack", By: by}); err != nil {
			errs = append(errs, fmt.Errorf("dispatch: stop %s: %w", runID, err))
		}
	}
	return errors.Join(errs...)
}

// recent remembers the runs started per channel, bounded both ways. The
// oldest channel is forgotten first, and so is the oldest run of a channel.
type recent struct {
	mu       sync.Mutex
	perChan  int
	maxChans int
	order    []chanKey
	runs     map[chanKey][]string
}

type chanKey struct{ tenant, channel string }

func newRecent(maxChans, perChan int) *recent {
	return &recent{perChan: perChan, maxChans: maxChans, runs: map[chanKey][]string{}}
}

func (r *recent) add(tenant, channel, runID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := chanKey{tenant, channel}
	ids, ok := r.runs[k]
	if !ok {
		if len(r.order) >= r.maxChans {
			delete(r.runs, r.order[0])
			r.order = r.order[1:]
		}
		r.order = append(r.order, k)
	}
	if slices.Contains(ids, runID) {
		return
	}
	ids = append(ids, runID)
	if len(ids) > r.perChan {
		ids = ids[len(ids)-r.perChan:]
	}
	r.runs[k] = ids
}

func (r *recent) on(tenant, channel string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.runs[chanKey{tenant, channel}])
}
