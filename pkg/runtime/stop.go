package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/urmzd/duraturo/pkg/run"

	"github.com/urmzd/mandatum/pkg/agentrt"
	"github.com/urmzd/mandatum/pkg/agentrt/payload"
	"github.com/urmzd/mandatum/pkg/bus"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/topic"
)

// Stopping a run.
//
// A person who presses stop wants the agent to stop working, and wants to be
// able to reply to what it had said so far. Both halves are this file's.
//
// # How the request reaches the turn
//
// The process that is asked to stop a run is usually not the one executing it:
// a deployment has several workers, and the one that leased the run is known
// only to the queue. So a stop is not a call into a worker. It is an event,
// published on the bus every process already shares, and the attempt executing
// the run subscribes to it for as long as the turn lasts.
//
// The request does not travel on the run's own topic. That topic has one
// writer, the attempt, which chooses its own sequence numbers (see seq.go),
// and the bus keeps the first write of a sequence: a second writer would land
// on numbers the attempt is about to use and displace its events. The request
// goes to the run's control topic instead (StopTopic), a stream of its own
// that is addressable from the run id alone.
//
// The bus retains a stream, and a subscription starts with what is retained.
// That one property is what settles every ordering of a request and an
// attempt:
//
//	before the turn starts   the request is waiting when the attempt
//	                         subscribes, and the turn stops before it calls
//	                         a model
//	while the turn runs      the request is delivered live
//	between two attempts     the request is waiting for the next attempt,
//	                         which stops as the first would have
//	after the run ended      nobody subscribes again, and nothing happens
//
// # Why it is the agent activity that listens
//
// duraturo replays a workflow body from the top, and a body may only decide
// things from its input and its records. Whether somebody has asked for a stop
// is neither: it changes between attempts. So the body never asks. The nested
// agent activity listens, and what it returns (a stopped Outcome, or an
// ordinary one) is recorded like any other result. A replay reads the record
// and makes the same decision, whatever has been requested since.
//
// # What a stopped run is
//
// It is over, and it did not fail. The agent activity returns a result rather
// than an error, so the ledger records the run as ended and never retries it,
// and the run's last event is lifecycle.stopped instead of completed or
// failed.
//
// # What the thread remembers
//
// The exchange is journaled as a completed run's is, so the person can reply
// to it. The assistant entry is the partial answer followed by StopMarker, or
// StopMarker alone when the turn was stopped before it said anything: a
// request with no answer after it would be a conversation the next turn could
// not make sense of. The reason for the stop is not written there. It is
// whatever the asker typed, and the journal is replayed verbatim to every
// later run in the thread.

// StopMarker is the line that ends a stopped turn's entry in its thread. It is
// a constant so that the entry is a function of the partial answer alone: the
// journal is replayed byte for byte to every later run, and nothing about a
// particular stop (who, when, why) may reach it.
const StopMarker = "[This turn was stopped before it finished.]"

// DefaultStopGrace is how long a turn is given to wrap up after it has been
// asked to stop, before it is cancelled.
const DefaultStopGrace = 10 * time.Second

// MaxStopText bounds the reason and the asker of a stop request, in bytes.
// Both are free text that ends up in an event every sink is sent.
const MaxStopText = 512

// stopVerb names the stop stream within a run's control topics.
const stopVerb = "stop"

// stopRetry is how long a turn waits before listening again after its
// subscription to the control topic failed.
const stopRetry = 250 * time.Millisecond

// ErrStopUnavailable reports a runtime that cannot carry stop requests: its
// bus can be published to and not subscribed to, so no turn it executes could
// ever hear one.
var ErrStopUnavailable = errors.New("runtime: stop is not available")

// Subscriber is the reading half of pkg/bus.Bus. A runtime whose Bus also
// implements it can stop its runs; one whose Bus does not, cannot, and says so
// with ErrStopUnavailable. Every backend in pkg/bus implements it.
type Subscriber interface {
	Subscribe(ctx context.Context, sub envelope.Subscription) (bus.Stream, error)
}

// StopRequest asks one run to stop.
type StopRequest struct {
	// Tenant is the scope the request is made under. It is not a filter the
	// caller chooses: a transport sets it from the caller's credential, and a
	// request is honoured only by a run of the same tenant.
	Tenant string
	// RunID is the run, as Acceptance.RunID reported it and as every event of
	// the run carries it. It is not the tag's id.
	RunID string
	// Reason is why, for people: it is published on the stopped event and
	// never shown to a model or written to the thread. Optional.
	Reason string
	// By is who asked. It is published on the stopped event. Optional.
	By string
}

// WithStopGrace sets how long a turn is given to wrap up after it has been
// asked to stop (default DefaultStopGrace). A turn that has not returned by
// then is cancelled and the run still ends stopped, with the text that had
// been published.
func WithStopGrace(d time.Duration) Option {
	return func(r *Runtime) {
		if d > 0 {
			r.stopGrace = d
		}
	}
}

// StopTopic is where requests to stop a run are published and where the
// attempt executing it listens: agent:_control:stop-<run>.
func StopTopic(runID string) (topic.Topic, error) {
	return topic.Control(stopVerb, runID)
}

// Stop asks a run to stop.
//
// It publishes the request and returns. It does not wait for the run and does
// not report what the run did, because it cannot know: the run may be
// executing in another process, may not have started, or may already be over.
// The outcome is on the run's own stream. A run that was stopped ends with
// lifecycle.stopped, and Result returns an Output with Stopped set; a run that
// finished first ends as it would have.
//
// A nil error therefore means the request was published, and nothing more. In
// particular Stop is the same call, with the same result, for:
//
//   - a run that does not exist
//   - a run that belongs to another tenant, which never hears the request
//   - a run that has already ended, which is not listening
//   - a run that has already been asked: the first request a run hears is the
//     one it honours, and later ones change nothing
//
// which is what makes it safe to call twice and impossible to probe with.
//
// It returns ErrInvalid for a request that names no run and
// ErrStopUnavailable when this runtime's bus cannot be subscribed to.
func (r *Runtime) Stop(ctx context.Context, req StopRequest) error {
	if r.control == nil {
		return fmt.Errorf("%w: the bus this runtime was built with cannot be subscribed to", ErrStopUnavailable)
	}
	req.RunID = strings.TrimSpace(req.RunID)
	tp, err := StopTopic(req.RunID)
	if err != nil {
		return fmt.Errorf("%w: stop: %w", ErrInvalid, err)
	}
	body, err := payload.Encode(payload.StopRequest{
		RunID:  req.RunID,
		Reason: clip(req.Reason),
		By:     clip(req.By),
	})
	if err != nil {
		return fmt.Errorf("runtime: stop %s: %w", req.RunID, err)
	}
	// Seq is left for the bus to assign: this stream has many writers and
	// none of them replays.
	e := envelope.Event{
		Topic:   tp,
		RunID:   tp.RunID(),
		Tenant:  req.Tenant,
		Agent:   topic.ControlName,
		Kind:    envelope.KindStop,
		Payload: body,
		At:      r.now().UTC(),
	}
	if err := r.bus.Publish(ctx, e); err != nil {
		return fmt.Errorf("runtime: stop %s: %w", req.RunID, err)
	}
	return nil
}

// clip trims free text from a stop request and bounds it, on a rune boundary.
func clip(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= MaxStopText {
		return s
	}
	end := MaxStopText
	for end > 0 && s[end]&0xC0 == 0x80 {
		end--
	}
	return s[:end]
}

type stopSignalKey struct{}

// StopSignal returns the channel on which the turn executing under ctx is
// asked to stop, or nil when it cannot be.
//
// It is how an Executor learns of a stop. The runtime puts the channel on the
// context it calls Execute with and sends at most one value on it. An executor
// that reads it wraps the turn up and returns an Outcome with Stopped set and
// no error. One that never reads it is cancelled through the same context once
// the grace period is over (see WithStopGrace), and the run ends stopped
// either way.
//
// It is a channel on the context rather than a method on Executor so that the
// request waits for the turn: a stop that arrives before the executor has set
// the turn up is still there when it looks.
func StopSignal(ctx context.Context) <-chan agentrt.Stop {
	ch, _ := ctx.Value(stopSignalKey{}).(chan agentrt.Stop)
	return ch
}

// execute runs the turn through the executor, listening for a stop request for
// as long as it runs.
//
// It returns what the executor returned, with one exception: a turn that was
// asked to stop and then returned an error has done what was asked, by a
// rougher route, and is reported as stopped with the text it had published.
// Retrying it would restart work a person asked to end.
func (r *Runtime) execute(ctx context.Context, in Input, req Request, emit func(context.Context, agentrt.Chunk) error) (Outcome, error) {
	if r.control == nil {
		return r.exec.Execute(ctx, req, emit)
	}

	requests, release := r.listen(ctx, in, req.RunID)
	defer release()

	signal := make(chan agentrt.Stop, 1)
	turnCtx, cancel := context.WithCancel(context.WithValue(ctx, stopSignalKey{}, signal))
	defer cancel()

	var (
		mu     sync.Mutex
		asked  *agentrt.Stop
		said   strings.Builder
		chunks int
	)
	returned := make(chan struct{})
	relayed := make(chan struct{})
	go func() {
		defer close(relayed)
		var stop agentrt.Stop
		select {
		case stop = <-requests:
		case <-returned:
			return
		}
		mu.Lock()
		asked = &stop
		mu.Unlock()
		signal <- stop // buffered, and this is the only send

		grace := time.NewTimer(r.stopGrace)
		defer grace.Stop()
		select {
		case <-grace.C:
			r.logger.Warn("mandatum/runtime: turn did not wrap up after a stop request; cancelling it",
				"run_id", req.RunID, "grace", r.stopGrace.String())
			cancel()
		case <-returned:
		}
	}()

	out, err := r.exec.Execute(turnCtx, req, func(ctx context.Context, c agentrt.Chunk) error {
		if err := emit(ctx, c); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		chunks++
		if c.Kind == envelope.KindText {
			if txt, derr := payload.Decode[payload.Text](envelope.Event{Kind: c.Kind, Payload: c.Payload}); derr == nil {
				said.WriteString(txt.Text)
			}
		}
		return nil
	})
	close(returned)
	<-relayed

	mu.Lock()
	defer mu.Unlock()
	switch {
	case err == nil:
		// Stopped or not, the executor's account stands. A turn that finished
		// before the request took effect completed, and is reported so.
		return out, nil
	case asked == nil:
		return out, err
	case ctx.Err() != nil, errors.Is(err, run.ErrSuperseded):
		// The worker is going away, or another attempt owns this run. Neither
		// is this attempt's to settle; whichever attempt runs next finds the
		// request still waiting.
		return out, err
	}
	r.logger.Warn("mandatum/runtime: turn ended with an error after a stop request; the run ends stopped",
		"run_id", req.RunID, "error", err)
	return Outcome{Text: said.String(), Chunks: chunks, Stopped: asked}, nil
}

// listen subscribes to the run's control topic and returns a channel that
// receives the first stop request the run should honour. release ends the
// subscription and waits for it to be gone.
//
// A request is honoured only if it was made under the run's own tenant. The
// bus addresses and does not authorize, so this comparison is the whole of the
// isolation: a request stamped with another tenant is skipped as though it had
// never been published, and nothing is ever said back to whoever sent it.
//
// The subscription starts from the beginning of what the bus retains, every
// time. That is what delivers a request made before this attempt existed, and
// it makes resubscribing after a failure free of bookkeeping: only the first
// matching request matters, and reading it twice is reading it once.
func (r *Runtime) listen(ctx context.Context, in Input, runID string) (<-chan agentrt.Stop, func()) {
	out := make(chan agentrt.Stop, 1)
	tp, err := StopTopic(runID)
	if err != nil {
		// Accept built this run's own topic from the same id, so this cannot
		// happen; a run that cannot be stopped is better than one that cannot
		// start.
		r.logger.Error("mandatum/runtime: stop topic is invalid", "run_id", runID, "error", err)
		return out, func() {}
	}
	sub := envelope.Subscription{Topic: tp, Filter: envelope.Filter{Kinds: []string{string(envelope.KindStop)}}}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			stop, ok, err := r.firstStop(ctx, sub, in.Tag.Tenant, runID)
			if ok {
				out <- stop // buffered, and this is the only send
				return
			}
			if ctx.Err() != nil {
				return
			}
			r.logger.Warn("mandatum/runtime: lost the stop subscription; listening again",
				"run_id", runID, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(stopRetry):
			}
		}
	}()
	return out, func() {
		cancel()
		<-done
	}
}

// firstStop reads one subscription until it yields a request this run should
// honour, or ends.
func (r *Runtime) firstStop(ctx context.Context, sub envelope.Subscription, tenant, runID string) (agentrt.Stop, bool, error) {
	stream, err := r.control.Subscribe(ctx, sub)
	if err != nil {
		return agentrt.Stop{}, false, err
	}
	defer func() { _ = stream.Close() }()
	for {
		e, err := stream.Recv(ctx)
		if err != nil {
			return agentrt.Stop{}, false, err
		}
		if e.Kind != envelope.KindStop || e.Tenant != tenant {
			continue
		}
		body, err := payload.Decode[payload.StopRequest](e)
		if err != nil || body.RunID != runID {
			// The topic already names the run. A body that disagrees with
			// it was not written by Stop, and is not obeyed.
			continue
		}
		return agentrt.Stop{Reason: body.Reason, By: body.By}, true, nil
	}
}

// stoppedText is what a stopped turn's entry in its thread says: what the
// agent had answered, then the marker, or the marker alone.
func stoppedText(partial string) string {
	partial = strings.TrimRight(partial, " \t\r\n")
	if partial == "" {
		return StopMarker
	}
	return partial + "\n\n" + StopMarker
}
