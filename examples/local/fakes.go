package main

// This file holds everything the story in main.go needs but is not itself the
// story: two fake connectors, a turn runtime, a corpus, and a bus wrapper that can
// be told to fail once.
//
// They are fakes only in the sense that they do not open sockets. Each one
// implements exactly the interface the real thing implements (connector.Sink,
// turn.Runtime, native.Retriever, bus.Bus), so nothing in main.go is
// simplified for the demo. Swapping pkg/connectors/slack in for slackSink is a
// constructor change and nothing else.

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/turn"
	"github.com/urmzd/fiber/pkg/turn/native"
	ragtypes "github.com/urmzd/fiber/pkg/turn/native/ragtypes"
	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

// ── the transcript ──────────────────────────────────────────────────
//
// The two sinks below are handed the same run's events on two independent
// lanes. pkg/egress guarantees order within one (run, target) pair and
// deliberately imposes none across targets, so a slow Jira sink cannot delay a
// webhook. That is the right property for a delivery system and the wrong
// property for a document: printing straight from Deliver makes the two
// surfaces interleave differently on every run.
//
// So the sinks RECORD what they wrote to their surface, keyed by the sequence
// that caused the write, and main prints the settled transcript in
// (seq, surface) order once the run is terminal. Nothing about what each sink
// received, or when, is changed by this, only when the ink dries.

type transcript struct {
	mu    sync.Mutex
	lines []line
}

type line struct {
	seq     uint64
	surface string
	text    string
}

// record appends one line of surface output attributed to an event.
func (t *transcript) record(seq uint64, surface, format string, args ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lines = append(t.lines, line{seq: seq, surface: surface, text: fmt.Sprintf(format, args...)})
}

// flush prints everything recorded so far and forgets it. The sort is stable,
// so several lines written by one sink for one event keep the order the sink
// wrote them in.
func (t *transcript) flush() {
	t.mu.Lock()
	defer t.mu.Unlock()
	slices.SortStableFunc(t.lines, func(a, b line) int {
		if c := cmp.Compare(a.seq, b.seq); c != 0 {
			return c
		}
		return cmp.Compare(a.surface, b.surface)
	})
	for _, l := range t.lines {
		fmt.Println(l.text)
	}
	t.lines = nil
}

// ── the Slack sink ──────────────────────────────────────────────────
//
// A chat sink renders a run by posting ONE message and editing it as the answer
// arrives, which is why pkg/egress carries events for one (run, target) pair on
// a single goroutine: two edits that overtake each other would leave the wrong
// final text on the surface permanently.
//
// This fake keeps the same shape. It accumulates deltas into a message body and
// prints each edit, so the terminal shows what a Slack thread would show.

type slackSink struct {
	out *transcript

	mu      sync.Mutex
	threads map[string]*slackThread // run id -> the message being edited
}

type slackThread struct {
	target string
	edits  int
	body   strings.Builder
	notes  []event.Citation
	events int
	done   bool
}

func newSlackSink(out *transcript) *slackSink {
	return &slackSink{out: out, threads: make(map[string]*slackThread)}
}

// Name is the address scheme this connector answers to: slack://... targets
// resolve here. It is the whole of connector.Connector.
func (s *slackSink) Name() string { return "slack" }

// Subscriptions implements connector.Subscriber: every run event on the
// issue's channel, rendered into one Slack thread.
func (s *slackSink) Subscriptions() []connector.Subscription {
	return []connector.Subscription{{
		Channels: []string{issue},
		Types:    []string{"run"},
		Target:   address.MustParse("slack://T01/C02?thread=1699.001"),
	}}
}

// Deliver implements connector.Sink. It is handed every event its
// subscription selected: for this demo, every run event.
func (s *slackSink) Deliver(_ context.Context, target address.Address, e event.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	th := s.threads[e.Run]
	if th == nil {
		th = &slackThread{target: target.String()}
		s.threads[e.Run] = th
	}
	th.events++

	switch b := e.Data.(type) {
	case event.Accepted:
		th.edits++
		s.out.record(e.Seq, "slack", "    slack   post   %s", th.target)
		s.out.record(e.Seq, "slack", "    slack   edit %d  %s", th.edits, "_docs-bot is looking into this..._")

	case event.Citation:
		th.notes = append(th.notes, b)

	case event.Text:
		th.body.WriteString(b.Text)
		th.edits++
		s.out.record(e.Seq, "slack", "    slack   edit %d  %s", th.edits, quote(th.body.String()))

	case event.Stopped:
		// A stopped run is over too: its partial answer is the last edit.
		th.done = true
		s.out.record(e.Seq, "slack", "    slack   edit %d  (stopped)", th.edits+1)

	case event.Completed:
		th.done = true
		s.out.record(e.Seq, "slack", "    slack   edit %d  (final, with %d footnote(s))", th.edits+1, len(th.notes))
		for _, c := range th.notes {
			s.out.record(e.Seq, "slack", "              %s %s", c.Label, c.Source.SourceURI)
		}
	}
	return nil
}

// delivered reports how many events this sink was handed for a run, and whether
// the run's terminal event arrived.
func (s *slackSink) delivered(runID string) (n int, done bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	th := s.threads[runID]
	if th == nil {
		return 0, false
	}
	return th.events, th.done
}

// ── the webhook sink ────────────────────────────────────────────────
//
// The other half of the N:M claim. Same run, same bus, same moment, but this
// subscription asked for run.completed and nothing else, so this sink never
// learns that deltas exist. It POSTs a JSON body once and is finished.

type webhookSink struct {
	out *transcript

	mu     sync.Mutex
	posts  map[string]int // run id -> bodies posted
	kinds  map[string]int
	bodies []string
}

func newWebhookSink(out *transcript) *webhookSink {
	return &webhookSink{out: out, posts: make(map[string]int), kinds: make(map[string]int)}
}

func (w *webhookSink) Name() string { return "webhook" }

// Subscriptions implements connector.Subscriber: only the run's completion.
func (w *webhookSink) Subscriptions() []connector.Subscription {
	return []connector.Subscription{{
		Channels: []string{issue},
		Types:    []string{event.TypeCompleted},
		Target:   address.MustParse("webhook://acme/deploys"),
	}}
}

func (w *webhookSink) Deliver(_ context.Context, target address.Address, e event.Envelope) error {
	var text string
	switch b := e.Data.(type) {
	case event.Completed:
		text = b.Text
	case event.Stopped:
		text = b.Text
	}
	body, err := json.Marshal(map[string]any{
		"run":     e.Run,
		"agent":   e.Agent,
		"version": e.Version,
		"type":    e.Type,
		"text":    text,
	})
	if err != nil {
		return err
	}

	w.mu.Lock()
	w.posts[e.Run]++
	w.kinds[e.Type]++
	w.bodies = append(w.bodies, string(body))
	w.mu.Unlock()

	w.out.record(e.Seq, "webhook", "    webhook POST   https://hooks.example.test/%s/%s", target.Workspace, target.Resource())
	w.out.record(e.Seq, "webhook", "              %s", string(body))
	return nil
}

func (w *webhookSink) delivered(runID string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.posts[runID]
}

// seenKinds returns every kind this sink was ever handed, which is the evidence
// for "the webhook never saw a delta".
func (w *webhookSink) seenKinds() map[string]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]int, len(w.kinds))
	for k, v := range w.kinds {
		out[k] = v
	}
	return out
}

// ── the turn runtime ────────────────────────────────────────────────
//
// turn.Runtime is the seam between the durable turn and the agent loop. A
// deployment passes runtime.NewSandbox, which runs the turn as a playscape task
// under the NGAC policy compiled from the pinned revision's Access grant.
//
// This example passes an in-process runtime instead, for one reason: a sandbox
// is about CONTAINMENT, and containment is not what this example is trying to
// show. Everything above the seam (pinning, banding, publication, delivery,
// replay) is identical either way, because the seam is one method.
//
// What it does is real. It runs the native runtime (pkg/turn/native) on the
// pinned revision, a saige agent loop against agenttest.ScriptedProvider, so
// the deltas that become events are saige's own typed deltas, translated by the
// same code a production turn uses.

type offlineRuntime struct {
	// calls counts how often the model was actually reached. It is the whole
	// proof in section 7: anchor replays the turn, but a recorded activity
	// is served from the ledger, so this must not move on a replay.
	calls atomic.Int64

	retriever native.Retriever
	scripts   map[string][][]saigetypes.Delta // agent name -> one entry per model call
}

func newOfflineRuntime(retriever native.Retriever) *offlineRuntime {
	return &offlineRuntime{
		retriever: retriever,
		scripts:   make(map[string][][]saigetypes.Delta),
	}
}

// script sets what the scripted provider replays for an agent.
func (x *offlineRuntime) script(agent string, deltas ...[]saigetypes.Delta) {
	x.scripts[agent] = deltas
}

// Turn implements turn.Runtime.
//
// def is a resolved, PINNED version, not a name and a range. A turn runtime
// must never re-resolve it: it may be running in another process, and "which
// definition to run" is not a question it is allowed to answer.
func (x *offlineRuntime) Turn(ctx context.Context, def turn.Pinned, in turn.Input, emit func(context.Context, event.Body) error) (turn.Result, error) {
	x.calls.Add(1)
	rt := &native.Runtime{Options: []native.Option{
		// The definition says provider "offline", so the native runtime
		// resolves it to saige's agenttest.ScriptedProvider with this
		// script. Change its model.use to anthropic/<model> and the same
		// code calls a model.
		native.WithScript(x.scripts[def.Name]...),
		native.WithRetriever(x.retriever),
		native.WithLogger(quiet),
	}}
	return rt.Turn(ctx, def, in, emit)
}

func (x *offlineRuntime) modelCalls() int64 { return x.calls.Load() }

// ── the corpus ──────────────────────────────────────────────────────
//
// native.Retriever is the narrow half of saige's RAG pipeline: a turn
// retrieves, it does not ingest. A deployment with a real pipeline adapts it in
// four lines (Search, then take .Context).
//
// Citations are derived from THIS, not from the model. That is deliberate and
// it is what makes them checkable: they report the passages the turn was given,
// with resolvable provenance, rather than the ones a model claims to have used.

type corpus struct{ blocks []ragtypes.ContextBlock }

func (c corpus) Retrieve(context.Context, string) (*ragtypes.AssembledContext, error) {
	var prompt strings.Builder
	prompt.WriteString("Sources you may cite:")
	for _, b := range c.blocks {
		fmt.Fprintf(&prompt, "\n%s %s", b.Citation, b.Text)
	}
	return &ragtypes.AssembledContext{
		Prompt:     prompt.String(),
		Blocks:     c.blocks,
		TokenCount: 128,
	}, nil
}

// ── a bus that can be told to fail ──────────────────────────────────
//
// pkg/bus.Memory is a complete broker, not a stub, so there is no failure
// injection in it and there should not be. Section 7 needs one specific
// publish to fail once (that is how it forces anchor to replay a turn whose
// agent activity is already recorded), so the failure is wrapped around the
// bus instead of built into it.
//
// Only Publish is overridden. Subscribe passes through, so the observer and the
// egress read the real broker.

type flakyBus struct {
	bus.Bus

	mu   sync.Mutex
	fail map[string]int // runID + "/" + kind -> remaining failures
}

func newFlakyBus(b bus.Bus) *flakyBus {
	return &flakyBus{Bus: b, fail: make(map[string]int)}
}

// failOnce arms one failure for the next publish of kind on runID.
func (f *flakyBus) failOnce(runID, typ string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[runID+"/"+typ]++
}

func (f *flakyBus) Publish(ctx context.Context, e event.Envelope) error {
	key := e.Run + "/" + e.Type
	f.mu.Lock()
	armed := f.fail[key] > 0
	if armed {
		f.fail[key]--
	}
	f.mu.Unlock()

	if armed {
		return fmt.Errorf("simulated broker outage publishing %s seq %d", e.Type, e.Seq)
	}
	return f.Bus.Publish(ctx, e)
}

// ── the third party ─────────────────────────────────────────────────
//
// An observer holds ONE subscription to agent:<name> and receives every run of
// that agent, from every trigger, forever. It never invoked anything, nothing
// sink names it, and no run knows it exists.

type observer struct {
	mu   sync.Mutex
	seen []event.Envelope
}

// watch subscribes and collects until ctx is cancelled. It returns a channel
// closed when the subscription has ended, so main can shut down cleanly.
func (o *observer) watch(ctx context.Context, b bus.Bus, sub bus.Subscription) (<-chan struct{}, error) {
	stream, err := b.Subscribe(ctx, sub)
	if err != nil {
		return nil, err
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		defer func() { _ = stream.Close() }()
		for {
			e, err := stream.Recv(ctx)
			if err != nil {
				return
			}
			o.mu.Lock()
			o.seen = append(o.seen, e)
			o.mu.Unlock()
		}
	}()
	return stopped, nil
}

// events returns everything the observer saw for one run, in arrival order.
func (o *observer) events(runID string) []event.Envelope {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []event.Envelope
	for _, e := range o.seen {
		if e.Run == runID {
			out = append(out, e)
		}
	}
	return out
}

// total is how many events reached the bus in all, across every run. It is
// ground truth in a way runtime.Output.Events is not: Output reports what ONE
// successful pass published (accepted, opened, the chunks, completed), so for a
// run that replayed it undercounts by the interrupted attempt's output, which
// really was published and really was seen.
func (o *observer) total() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.seen)
}

// runs returns the distinct run ids the observer has seen, in first-sight
// order. Two entries means one subscription served two unrelated runs.
func (o *observer) runs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, e := range o.seen {
		if !seen[e.Run] {
			seen[e.Run] = true
			out = append(out, e.Run)
		}
	}
	return out
}

// sawKind reports whether the observer received a kind for a run.
func (o *observer) sawKind(runID, typ string) bool {
	for _, e := range o.events(runID) {
		if e.Type == typ {
			return true
		}
	}
	return false
}

// ── small helpers ───────────────────────────────────────────────────

// streamed builds one scripted model response out of several text deltas, so
// the stream arrives in pieces the way a real provider's does.
func streamed(parts ...string) []saigetypes.Delta {
	out := make([]saigetypes.Delta, 0, len(parts)+2)
	out = append(out, saigetypes.PartStart{Index: 0, Kind: saigetypes.KindText})
	for _, p := range parts {
		out = append(out, saigetypes.PartDelta{Index: 0, Text: p})
	}
	return append(out, saigetypes.PartEnd{Index: 0})
}

// quote renders a message body on one line, elided in the middle so the growth
// between edits stays visible at a glance.
func quote(s string) string {
	const width = 58
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= width {
		return `"` + s + `"`
	}
	return `"` + s[:width-20] + " … " + s[len(s)-17:] + `"`
}

// summarize renders an event's body as one short line, for the observer's
// log. Bodies are typed, so a type switch is the whole decoder.
func summarize(e event.Envelope) string {
	switch v := e.Data.(type) {
	case event.Text:
		return quote(v.Text)
	case event.Citation:
		return fmt.Sprintf("%s %s", v.Label, v.Source.SourceURI)
	case event.Accepted:
		return "version=" + e.Version
	case event.Started:
		return fmt.Sprintf("attempt=%d", v.Attempt)
	case event.Resumed:
		return fmt.Sprintf("attempt=%d", v.Attempt)
	case event.Completed:
		return strings.TrimSpace(fmt.Sprintf("attempt=%d %s", v.Attempt, quote(v.Text)))
	case event.Failed:
		return fmt.Sprintf("attempt=%d error=%s", v.Attempt, v.Error)
	default:
		return ""
	}
}
