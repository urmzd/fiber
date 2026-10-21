package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/urmzd/anchor/pkg/run"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/topic"
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
// only to the queue. So a stop is not a call into a worker. It is recorded on
// the run in the ledger every worker reads (the truth), and then published on
// the bus every process shares (the fast path), where the attempt executing
// the run listens for as long as its turn lasts.
//
// The request does not travel on the run's own topic. That topic has one
// writer, the attempt, which chooses its own sequence numbers (see seq.go),
// and the bus keeps the first write of a sequence: a second writer would land
// on numbers the attempt is about to use and displace its events. The request
// goes to a stop stream of its own instead (StopTopic), keyed by the asker's
// tenant and the run id, so that one tenant's requests never share a stream
// with another's.
//
// # A request that arrives before the turn
//
// The bus is the fast path, and it only reaches a turn that is listening. A
// request made before an attempt exists (the run is queued, or between two
// attempts) has to wait for one, and it waits in two places:
//
//   - The run's ledger, when the runtime has one (WithStopLedger). Stop writes
//     the request as an event record on the run, "event:fiber.stop#0",
//     after checking that the run exists, belongs to the asker's tenant and
//     has not ended. Every attempt reads the ledger before its turn starts,
//     and a recorded request ends the turn before the executor is called. The
//     record is write-once, so the first request recorded is the one obeyed.
//     This is the durable path: nothing evicts it.
//   - The bus. A subscription starts with what the bus retains, so an attempt
//     also finds a request published before it subscribed. This is what a
//     deployment without WithStopLedger relies on, and it is bounded (see
//     "How long the bus keeps a request" below).
//
// Together they settle every ordering of a request and an attempt:
//
//	before the turn starts   recorded, or retained: the attempt ends the turn
//	                         before calling a model
//	while the turn runs      the request is delivered live
//	between two attempts     recorded, or retained: the next attempt stops
//	after the run ended      not recorded (the run has ended), and nobody
//	                         subscribes again: nothing happens
//	while parked on a person recorded, and the question is resolved as
//	                         stopped: the run is woken and ends without
//	                         resuming its turn (see park.go)
//
// # How long the bus keeps a request
//
// A request is retained under its own stream, the topic StopTopic returns. It
// does not share the run's stream, so a chatty run cannot push it out. That
// stream is bounded like any other:
//
//   - Depth: it keeps the newest bus.WithRetention requests (1024 by default).
//     Only the run's own tenant writes to it, and the newest requests are the
//     ones kept, so depth never loses a stop; it decides which reason is read.
//   - Memory: the in-memory bus keeps the bus.WithMaxRuns streams (4096 by
//     default) written to most recently, counting run streams and control
//     streams alike. A pending request is lost once that many other streams
//     have been written to since it was published, the run's own among them.
//   - Redis: a run-scoped subscription reads the stream key directly, so the
//     discovery index and its TTL do not apply. The key has no expiry; it
//     lasts until Redis evicts or loses it.
//
// Without WithStopLedger a lost request means the run finishes as though
// nobody had asked. With it, the bound only decides whether the request is
// also delivered by the fast path.
//
// # Why it is the agent activity that listens
//
// anchor replays a workflow body from the top, and a body may only decide
// things from its input and its records. Whether somebody has asked for a stop
// is neither: it changes between attempts. So the body never asks. The nested
// agent activity listens, and what it returns (a stopped Outcome, or an
// ordinary one) is recorded like any other result. A replay reads the record
// and makes the same decision, whatever has been requested since.
//
// # The commit point: stopped means cut short
//
// A turn that returned its whole answer completed, whether or not a request
// was on its way: "stopped" means cut short, and the agent loop is the judge
// of that (saige reports a turn it wrapped up, and does not report one that
// finished first). So, with WithStopLedger, once Stop has returned nil for a
// live run of the asker's: no attempt starts its turn, a running turn is asked
// to wrap up (over the bus at once, or at the next ledger poll), and the run
// ends stopped unless its turn finished its answer before the request took
// effect, in which case it ends completed. Exactly one outcome is recorded,
// and the journal, the terminal event and Result are all built from it.
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

// DefaultStopPoll is how often a running turn reads the ledger for a stop
// request, when the runtime has one (WithStopLedger). It bounds how long a
// request the bus failed to deliver takes to reach the turn.
const DefaultStopPoll = 2 * time.Second

// WithStopPoll sets how often a running turn reads the ledger for a stop
// request (default DefaultStopPoll). It has no effect without WithStopLedger.
func WithStopPoll(d time.Duration) Option {
	return func(r *Runtime) {
		if d > 0 {
			r.stopPoll = d
		}
	}
}

// DefaultStopGrace is how long a turn is given to wrap up after it has been
// asked to stop, before it is cancelled.
const DefaultStopGrace = 10 * time.Second

// MaxStopText bounds the reason and the asker of a stop request, in bytes.
// Both are free text that ends up in an event every sink is sent.
const MaxStopText = 512

// stopVerb names the stop stream within a run's control topics.
const stopVerb = "stop"

// stopRecordKey is the ledger record a stop request is written to. It has the
// shape of a anchor event record, which is the kind of record the ledger
// expects to be written from outside a run.
const (
	stopRecordName = "fiber.stop"
	stopRecordKey  = "event:" + stopRecordName + "#0"
)

// StopLedger is the part of a anchor ledger a stop is recorded in: a point
// load of a run with its records, and a write-once record. Every
// ledger.Ledger implements it.
type StopLedger interface {
	Load(ctx context.Context, runID string) (run.Run, []run.Record, error)
	Record(ctx context.Context, rec run.Record) (run.Record, error)
}

// WithStopLedger records stop requests in the run ledger as well as
// publishing them, so a request made before a turn starts is honoured however
// long the turn takes to start. Pass the ledger the runtime's anchor client
// uses. See "A request that arrives before the turn" in stop.go.
//
// Without it a request that waits for a turn is held only by the bus, and is
// lost if the bus forgets it first.
//
// The same ledger holds what an attempt that failed had spent, so the run's
// total counts it (see usage.go). Without it Output.Usage counts only the
// attempt that produced the outcome; the usage.call events still report every
// call.
func WithStopLedger(l StopLedger) Option {
	return func(r *Runtime) { r.stops = l }
}

// stopRetry is how long a turn waits before listening again after its
// subscription to the control topic failed.
const stopRetry = 250 * time.Millisecond

// ErrStopped is the cause of a context cancelled because the run it belongs to
// was stopped. A Delegator reads it with context.Cause and errors.Is, which is
// how it tells a stop apart from a worker shutting down.
var ErrStopped = errors.New("runtime: the run was stopped")

// StoppedError is the cause a Delegator's context ends with when the parent's
// turn was stopped: who asked and why, so the host can stop the run it started
// for the delegation with the same words. It matches ErrStopped.
type StoppedError struct {
	Reason string
	By     string
}

func (e *StoppedError) Error() string {
	if e.Reason == "" {
		return ErrStopped.Error()
	}
	return ErrStopped.Error() + ": " + e.Reason
}

// Unwrap makes a StoppedError match ErrStopped.
func (e *StoppedError) Unwrap() error { return ErrStopped }

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

// StopTopic is where a tenant's requests to stop a run are published and where
// the attempt executing that run, if it is the tenant's, listens:
// agent:_control:stop-<key>, the key a hash of the tenant and the run id.
//
// The tenant is part of the address so that one tenant's requests can never
// land in the stream another tenant's run listens on. A run id is a hash of a
// tenant and a tag id anyone can compute; without the tenant here, a tenant
// could publish enough requests for another tenant's run to push that tenant's
// own request out of the stream before the run read it. The tenant a request
// is published under is the caller's credential's, never something the caller
// chooses, so the stream a request lands in is fixed by who asked.
func StopTopic(tenant, runID string) (topic.Topic, error) {
	if _, err := topic.Run("probe", runID); err != nil {
		return topic.Topic{}, err
	}
	h := sha256.New()
	var n [binary.MaxVarintLen64]byte
	for _, field := range []string{tenant, runID} {
		h.Write(n[:binary.PutUvarint(n[:], uint64(len(field)))])
		h.Write([]byte(field))
	}
	return topic.Control(stopVerb, hex.EncodeToString(h.Sum(nil)[:16]))
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
// which is what makes it safe to call twice, and means its result never says
// whether a run exists. Its timing can: a run id is computable from a tenant
// and a tag id, and with WithStopLedger, Stop for an id the ledger holds reads
// and decodes that run before deciding to do nothing, while an unknown id
// returns after one missed read. A caller who can time the call precisely can
// therefore learn whether some tenant's run with that id exists, though
// nothing about it. The ledger has no tenant-keyed lookup that would make the
// two cost the same.
//
// With WithStopLedger (which a deployment should always pass) the ledger
// decides. The request is recorded on the run and then published, but only if
// the run is in that ledger, is the asker's tenant's, and has not ended.
// Otherwise nothing is written anywhere and the call still returns nil. The
// ledger must therefore be the one the runs execute from, as it is when it is
// shared Postgres or when one process accepts and executes. A ledger that
// cannot be read or written fails the call, so the caller retries; that is
// the only error that can depend on the run, and it is only ever seen by the
// run's own tenant.
//
// From the moment Stop returns nil for such a run, no attempt of it starts a
// turn, a running turn is asked to wrap up (at once over the bus, or within
// the stop poll interval if the bus lost the request), and the run ends
// stopped, unless its turn finished its answer before the request took
// effect, in which case it ends completed. See "The commit point" in stop.go.
//
// Without WithStopLedger the request is published unconditionally, to the
// asker's own stream for the run, and is as durable as the bus (see "How long
// the bus keeps a request").
//
// An empty reason becomes DefaultStopReason here, once, so every path a stop
// can take reports the same words.
//
// It returns ErrInvalid for a request that names no run and
// ErrStopUnavailable when this runtime's bus cannot be subscribed to and it
// has no ledger to record in.
func (r *Runtime) Stop(ctx context.Context, req StopRequest) error {
	if r.control == nil && r.stops == nil {
		return fmt.Errorf("%w: the bus this runtime was built with cannot be subscribed to", ErrStopUnavailable)
	}
	req.RunID = strings.TrimSpace(req.RunID)
	tp, err := StopTopic(req.Tenant, req.RunID)
	if err != nil {
		return fmt.Errorf("%w: stop: %w", ErrInvalid, err)
	}
	reason := clip(req.Reason)
	if reason == "" {
		reason = agentrt.DefaultStopReason
	}
	at := r.now().UTC()
	body, err := payload.Encode(payload.StopRequest{
		RunID:  req.RunID,
		Reason: reason,
		By:     clip(req.By),
		At:     at,
	})
	if err != nil {
		return fmt.Errorf("runtime: stop %s: %w", req.RunID, err)
	}
	if r.stops != nil {
		owned, err := r.recordStop(ctx, req.Tenant, req.RunID, body)
		if err != nil || !owned {
			// Not the asker's run to stop, or not one that is still going:
			// nothing is published, and the answer is the same nil.
			return err
		}
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
		At:      at,
	}
	if err := r.bus.Publish(ctx, e); err != nil {
		return fmt.Errorf("runtime: stop %s: %w", req.RunID, err)
	}
	return nil
}

// recordStop writes a stop request on its run in the ledger and reports
// whether the run is one the request may reach: in the ledger, the asker's
// tenant's, and not ended. Anything else writes nothing and is not an error.
//
// The record is write-once, so a run asked twice keeps the first request and
// still reports true: the bus message for the second is harmless, since the
// run honours the first request it hears. Records are only written for runs that exist, so the ledger
// holds at most one per run and lives exactly as long as the run's own
// records do.
func (r *Runtime) recordStop(ctx context.Context, tenant, runID string, body []byte) (bool, error) {
	rec, err := r.loadRun(ctx, runID)
	switch {
	case errors.Is(err, run.ErrNotFound):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("runtime: stop %s: read the run: %w", runID, err)
	}
	if rec.Status.Terminal() {
		return false, nil
	}
	var in Input
	if err := json.Unmarshal(rec.Input, &in); err != nil || in.Tag.Tenant != tenant {
		// Not a run of this runtime's, or not the asker's.
		return false, nil
	}
	stored, err := r.stops.Record(ctx, run.Record{
		RunID: runID, Key: stopRecordKey, Kind: run.KindEvent, Name: stopRecordName,
		Codec: "application/json", Status: run.RecordOK, Output: body,
	})
	switch {
	case err == nil, errors.Is(err, run.ErrAlreadyRecorded):
		// A run parked on a person is not running a turn that could hear
		// the request: its question is resolved as stopped, with the
		// request the run honours, and it is woken to end.
		if errors.Is(err, run.ErrAlreadyRecorded) {
			body = stored.Output
		}
		if err := r.stopParked(ctx, tenant, runID, body); err != nil {
			return true, err
		}
		return true, nil
	case errors.Is(err, run.ErrNotFound):
		return false, nil
	default:
		return false, fmt.Errorf("runtime: stop %s: record the request: %w", runID, err)
	}
}

// StopRecord is a stop request as the ledger holds it.
type StopRecord struct {
	Reason string
	By     string
	At     time.Time
}

// StopRequested reports the stop request recorded on a run of tenant's, for a
// host that holds the runtime and wants to say whether it has been asked
// without keeping that state itself. It is not for the public edge: unlike
// Stop it does distinguish a run that was asked from one that was not.
//
// A run that does not exist and a run of another tenant both report false,
// so a host can only ever learn about its own tenant's runs. It returns
// ErrStopUnavailable without WithStopLedger.
func (r *Runtime) StopRequested(ctx context.Context, tenant, runID string) (StopRecord, bool, error) {
	if r.stops == nil {
		return StopRecord{}, false, fmt.Errorf("%w: no stop ledger", ErrStopUnavailable)
	}
	rec, records, err := r.recordsOf(ctx, runID, true, prefixStop)
	switch {
	case errors.Is(err, run.ErrNotFound):
		return StopRecord{}, false, nil
	case err != nil:
		return StopRecord{}, false, fmt.Errorf("runtime: stop state of %s: %w", runID, err)
	}
	var in Input
	if err := json.Unmarshal(rec.Input, &in); err != nil || in.Tag.Tenant != tenant {
		return StopRecord{}, false, nil
	}
	for _, record := range records {
		if record.Key != stopRecordKey || record.Status != run.RecordOK {
			continue
		}
		var body payload.StopRequest
		if err := json.Unmarshal(record.Output, &body); err != nil || body.RunID != runID {
			continue
		}
		at := body.At
		if at.IsZero() {
			at = record.RecordedAt
		}
		return StopRecord{Reason: body.Reason, By: body.By, At: at}, true, nil
	}
	return StopRecord{}, false, nil
}

// loadRun reads a run without its records when the ledger offers that point
// read (every ledger in anchor and pkg/durable does), and with them
// otherwise.
func (r *Runtime) loadRun(ctx context.Context, runID string) (run.Run, error) {
	if g, ok := r.stops.(interface {
		GetRun(context.Context, string) (run.Run, error)
	}); ok {
		return g.GetRun(ctx, runID)
	}
	rec, _, err := r.stops.Load(ctx, runID)
	return rec, err
}

// recordedStop reports a stop request recorded on the run in the ledger.
//
// It is read at the start of the agent activity, whose result is memoized, so
// the answer it gives is recorded with that result and a replay does not ask
// again. A ledger that cannot be read is logged and treated as no request:
// the bus is still listening, and refusing to run because a read failed
// would turn a ledger hiccup into a failed run.
func (r *Runtime) recordedStop(ctx context.Context, runID string) (agentrt.Stop, bool) {
	if r.stops == nil {
		return agentrt.Stop{}, false
	}
	_, records, err := r.recordsOf(ctx, runID, false, prefixStop)
	if err != nil {
		r.logger.Warn("fiber/runtime: could not read recorded stop requests; relying on the bus",
			"run_id", runID, "error", err)
		return agentrt.Stop{}, false
	}
	for _, rec := range records {
		if rec.Key != stopRecordKey || rec.Status != run.RecordOK {
			continue
		}
		var body payload.StopRequest
		if err := json.Unmarshal(rec.Output, &body); err != nil || body.RunID != runID {
			continue
		}
		return agentrt.Stop{Reason: body.Reason, By: body.By}, true
	}
	return agentrt.Stop{}, false
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
	// A request recorded before this attempt began ends the turn before it
	// starts: no executor, no model call.
	if stop, ok := r.recordedStop(ctx, req.RunID); ok {
		return Outcome{Prompt: agentrt.RequestText(req.Text, req.Meta), Stopped: &stop, StopRecorded: true}, nil
	}
	// Requests to compact are heard for as long as the turn runs (compact.go).
	ctx, stopListening := r.listenCompact(ctx, in, req.RunID)
	defer stopListening()
	if r.control == nil && r.stops == nil {
		return r.exec.Execute(ctx, req, emit)
	}
	out, err := r.executeListening(ctx, in, req, emit)
	if err == nil && r.stops != nil {
		// Whichever way the turn ended, a request recorded on the run is
		// claimed by the workflow body, so the ledger keeps no record that
		// no call read. Stopped or not, the outcome is already decided.
		_, out.StopRecorded = r.recordedStop(ctx, req.RunID)
	}
	return out, err
}

// executeListening is execute for a runtime that can hear a stop request.
func (r *Runtime) executeListening(ctx context.Context, in Input, req Request, emit func(context.Context, agentrt.Chunk) error) (Outcome, error) {

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
	// settle ends the relay and waits for it. It is deferred as well as
	// called, so an executor that panics leaves no goroutine behind: the
	// worker recovers the panic, and nothing else would ever close returned.
	var settled sync.Once
	settle := func() {
		settled.Do(func() {
			close(returned)
			<-relayed
		})
	}
	defer settle()
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
			r.logger.Warn("fiber/runtime: turn did not wrap up after a stop request; cancelling it",
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
	settle()

	mu.Lock()
	defer mu.Unlock()
	switch {
	case err == nil:
		// The executor's account stands. A turn that returned its whole
		// answer before the request took effect completed: "stopped" means
		// cut short, and an answer that was finished was not.
		return out, nil
	case asked == nil:
		return out, err
	case ctx.Err() != nil, errors.Is(err, run.ErrSuperseded):
		// The worker is going away, or another attempt owns this run. Neither
		// is this attempt's to settle; whichever attempt runs next finds the
		// request still waiting.
		return out, err
	}
	r.logger.Warn("fiber/runtime: turn ended with an error after a stop request; the run ends stopped",
		"run_id", req.RunID, "error", err)
	// The executor never said what it sent, so the request is recorded as the
	// agent renders it before retrieval: the same words a turn stopped before
	// it began records.
	return Outcome{
		Text: said.String(), Chunks: chunks, Stopped: asked,
		Prompt: firstNonEmpty(out.Prompt, agentrt.RequestText(req.Text, req.Meta)),
	}, nil
}

// listen returns a channel that receives the first stop request the run
// should honour, from whichever of two sources has it first, and a release
// that ends both and waits for them.
//
//   - The bus: the run's control topic, subscribed from the beginning of what
//     the bus retains, every time. That delivers a request made before this
//     attempt existed, and makes resubscribing after a failure free of
//     bookkeeping: only the first matching request matters.
//   - The ledger, when the runtime has one: polled every stop poll interval,
//     so a request the bus lost still reaches a turn that is running.
//
// A bus request is honoured only if it was made under the run's own tenant.
// The bus addresses and does not authorize, so this comparison is the whole of
// the isolation: a request stamped with another tenant is skipped as though it
// had never been published. A ledger request was checked against the run's
// tenant before it was written.
func (r *Runtime) listen(ctx context.Context, in Input, runID string) (<-chan agentrt.Stop, func()) {
	out := make(chan agentrt.Stop, 1)
	var once sync.Once
	deliver := func(stop agentrt.Stop) {
		once.Do(func() { out <- stop }) // buffered, and only one send happens
	}

	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	if r.control != nil {
		tp, err := StopTopic(in.Tag.Tenant, runID)
		if err != nil {
			// Accept built this run's own topic from the same id, so this
			// cannot happen; a run that cannot be stopped is better than one
			// that cannot start.
			r.logger.Error("fiber/runtime: stop topic is invalid", "run_id", runID, "error", err)
		} else {
			sub := envelope.Subscription{Topic: tp, Filter: envelope.Filter{Kinds: []string{string(envelope.KindStop)}}}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ctx.Err() == nil {
					stop, ok, err := r.firstStop(ctx, sub, in.Tag.Tenant, runID)
					if ok {
						deliver(stop)
						return
					}
					if ctx.Err() != nil {
						return
					}
					r.logger.Warn("fiber/runtime: lost the stop subscription; listening again",
						"run_id", runID, "error", err)
					select {
					case <-ctx.Done():
						return
					case <-time.After(stopRetry):
					}
				}
			}()
		}
	}
	if r.stops != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tick := time.NewTicker(r.stopPoll)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				if stop, ok := r.recordedStop(ctx, runID); ok {
					deliver(stop)
					return
				}
			}
		}()
	}
	return out, func() {
		cancel()
		wg.Wait()
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
