package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/runtime"
)

// listener is an executor that waits, once it is running, for a request to
// compact, and records the first one it hears.
type listener struct {
	waiting chan struct{}
	heard   chan agentrt.CompactRequest
	once    sync.Once
}

func newListener() *listener {
	return &listener{waiting: make(chan struct{}), heard: make(chan agentrt.CompactRequest, 1)}
}

func (l *listener) Execute(ctx context.Context, req runtime.Request, _ func(context.Context, agentrt.Chunk) error) (runtime.Outcome, error) {
	signal := runtime.CompactSignal(ctx)
	l.once.Do(func() { close(l.waiting) })
	select {
	case got := <-signal:
		l.heard <- got
		return runtime.Outcome{Text: "compacted", Prompt: req.Text}, nil
	case <-ctx.Done():
		return runtime.Outcome{}, ctx.Err()
	}
}

// A request reaches the attempt executing the run, under the run's own tenant
// only, and only if it was made while that attempt was listening: one made
// before the run had a worker does nothing, because the turn it would have
// compacted is not the one that runs.
func TestARequestToCompactReachesOnlyTheTurnRunningNowUnderItsTenant(t *testing.T) {
	w := newWorld(t)
	exec := newListener()
	rt := w.runtime(t, exec)

	runID := w.accept(t, rt, tag("docs-bot", "evt-1", "explain"))
	compact := func(tenant, by string) {
		t.Helper()
		if err := rt.Compact(context.Background(), runtime.CompactRequest{Tenant: tenant, RunID: runID, By: by}); err != nil {
			t.Fatalf("Compact(%s): %v", tenant, err)
		}
	}
	compact("acme", "too early")

	w.work(t, rt, nil)
	select {
	case <-exec.waiting:
	case <-time.After(20 * time.Second):
		t.Fatal("the turn never started")
	}
	compact("globex", "intruder")
	compact("acme", "U1")

	select {
	case got := <-exec.heard:
		if got.By != "U1" {
			t.Fatalf("the turn heard a request by %q, want only the one made while it ran, under its tenant", got.By)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the turn never heard the request")
	}
	if out := w.result(t, rt, runID); out.Text != "compacted" {
		t.Errorf("Output.Text = %q", out.Text)
	}
}

// Compact answers the same for every run id, as Stop does, and refuses only
// what it cannot carry.
func TestCompactRefusesWhatItCannotCarry(t *testing.T) {
	h := newHarness(t, newExecutor("answer")) // its bus is publish-only
	if err := h.rt.Compact(context.Background(), runtime.CompactRequest{Tenant: "acme", RunID: "r1"}); !errors.Is(err, runtime.ErrCompactUnavailable) {
		t.Fatalf("Compact on a publish-only bus = %v, want ErrCompactUnavailable", err)
	}

	w := newWorld(t)
	rt := w.runtime(t, newPatient())
	for _, id := range []string{"", "has:colon"} {
		if err := rt.Compact(context.Background(), runtime.CompactRequest{Tenant: "acme", RunID: id}); !errors.Is(err, runtime.ErrInvalid) {
			t.Errorf("Compact(%q) = %v, want ErrInvalid", id, err)
		}
	}
	if err := rt.Compact(context.Background(), runtime.CompactRequest{Tenant: "acme", RunID: "r0123", By: "U1"}); err != nil {
		t.Fatalf("Compact for an unknown run = %v, want nil", err)
	}
	events := w.bus.rec.all()
	if len(events) != 1 {
		t.Fatalf("published %d events, want the one request", len(events))
	}
	want, _ := runtime.CompactTopic("acme", "r0123")
	if e := events[0]; e.Topic.String() != want.String() || e.Kind != envelope.KindCompact || e.Tenant != "acme" {
		t.Errorf("request published as topic %s kind %s tenant %q, want %s %s acme", e.Topic, e.Kind, e.Tenant, want, envelope.KindCompact)
	}
	if stop, _ := runtime.StopTopic("acme", "r0123"); want.String() == stop.String() {
		t.Error("requests to compact share a stream with requests to stop")
	}
}

// loopModel calls the lookup tool for a number of turns and then answers, and
// writes a summary when it is asked for one.
type loopModel struct {
	turns int

	mu    sync.Mutex
	calls int
}

func (m *loopModel) Stream(_ context.Context, req saigetypes.Request) (<-chan saigetypes.Delta, error) {
	var out []saigetypes.Delta
	first, _ := json.Marshal(req.Messages[:1])
	m.mu.Lock()
	switch {
	case strings.Contains(string(first), saigetypes.SummaryInstruction[:30]):
		out = agenttest.TextResponse("the earlier lookups, summarized")
	case m.calls < m.turns:
		m.calls++
		out = agenttest.ToolCallResponse(fmt.Sprintf("call-%d", m.calls), "lookup", map[string]any{"n": m.calls})
	default:
		out = agenttest.TextResponse("done")
	}
	m.mu.Unlock()
	ch := make(chan saigetypes.Delta, len(out))
	for _, d := range out {
		ch <- d
	}
	close(ch)
	return ch, nil
}

// heldLookup is a lookup that holds its third call until released, so a test
// can ask for a compaction while the turn is in its tool loop.
type heldLookup struct {
	running chan struct{}
	release chan struct{}

	mu    sync.Mutex
	calls int
}

func (h *heldLookup) Definition() saigetypes.ToolDef {
	return saigetypes.ToolDef{Name: "lookup", Description: "Look something up", Parameters: saigetypes.ParameterSchema{Type: "object"}}
}

func (h *heldLookup) Execute(ctx context.Context, _ map[string]any) (string, error) {
	h.mu.Lock()
	h.calls++
	n := h.calls
	h.mu.Unlock()
	switch {
	case n == 3:
		close(h.running)
		select {
		case <-h.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	case n > 3:
		// Later calls leave the request time to cross the bus.
		time.Sleep(10 * time.Millisecond)
	}
	return "row of results. row of results.", nil
}

// End to end: a host asks a running run to compact, the turn summarizes its
// older history before its next model call, and the run's stream says so,
// with who asked. The run then finishes as it would have.
func TestARequestedCompactionIsMadeByTheRunningTurnAndPublished(t *testing.T) {
	w := newWorld(t)
	lookup := &heldLookup{running: make(chan struct{}), release: make(chan struct{})}
	model := &loopModel{turns: 30}
	w.specs.mu.Lock()
	w.specs.revs["docs-bot"][0].Spec.Tools = []string{"lookup"}
	w.specs.mu.Unlock()
	exec := sandboxOf(t, w.specs, map[string]saigetypes.Provider{"docs-bot": model},
		runtime.WithCatalog(func(runtime.Request, func(payload.Action) error) (agentrt.Tools, error) {
			return agentrt.StaticTools(lookup), nil
		}),
		runtime.WithAgentOptions(
			agentrt.WithMaxIter(40),
			agentrt.WithCompaction(agentrt.Compaction{OnRequest: true, KeepTurns: 1}),
		))
	rt := w.runtime(t, exec)
	w.work(t, rt, nil)

	runID := w.accept(t, rt, tag("docs-bot", "evt-1", "look it up"))
	select {
	case <-lookup.running:
	case <-time.After(20 * time.Second):
		t.Fatal("the turn never reached its third lookup")
	}
	if err := rt.Compact(context.Background(), runtime.CompactRequest{Tenant: "acme", RunID: runID, By: "U1"}); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	close(lookup.release)

	if out := w.result(t, rt, runID); out.Text != "done" {
		t.Fatalf("Output.Text = %q, want the run to finish its loop", out.Text)
	}
	var compactions []payload.Compaction
	for _, e := range w.bus.of(runID) {
		if e.Kind != envelope.KindCompacted {
			continue
		}
		body, err := payload.Decode[payload.Compaction](e)
		if err != nil {
			t.Fatalf("decode %s: %v", e.Kind, err)
		}
		compactions = append(compactions, body)
	}
	if len(compactions) != 1 {
		t.Fatalf("the run published %d %s events, want 1: %v", len(compactions), envelope.KindCompacted, kindsOf(w.bus.of(runID)))
	}
	if c := compactions[0]; c.Trigger != agentrt.TriggerRequested || c.By != "U1" || c.Summarized == 0 {
		t.Errorf("compaction = %+v, want a requested one by U1 that summarized something", c)
	}
}
