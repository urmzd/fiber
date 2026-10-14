package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/mandatum/internal/server"
	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/agentrt/payload"
	"github.com/urmzd/mandatum/pkg/agentspec"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// Through the composition root: a run's usage is delivered on its terminal
// event and read back from the ledger by GetRun, and the two agree. A route
// that selected every kind also receives the usage.call events, which is how a
// webhook consumer gets them.
func TestGetRunReadsTheUsageTheRunRecorded(t *testing.T) {
	c, err := newCore(coreConfig{Tenant: "acme", Workspace: t.TempDir()})
	if err != nil {
		t.Fatalf("newCore: %v", err)
	}
	t.Cleanup(func() { _ = c.close() })
	box := &inbox{}
	if err := c.sinks.Register(box); err != nil {
		t.Fatalf("register sink: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var g group
	c.startDelivery(ctx, &g)
	c.startExecution(ctx, &g)
	t.Cleanup(func() {
		cancel()
		_ = g.wait(10 * time.Second)
	})

	spec := agentspec.AgentSpec{Name: "docs-bot", Model: "m", Provider: agentspec.ProviderOffline, SystemPrompt: "Answer."}
	if _, err := c.store.Create(ctx, "acme", spec, "test"); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	target := address.MustParse("inbox://acme/out")
	acc, err := c.Invoke(ctx, envelope.Tag{
		ID: "Ev-1", Tenant: "acme", Agent: "docs-bot", Origin: "test",
		Text: "explain", Deliver: []envelope.Route{{Target: target}}, At: time.Now(),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for !box.completed(target.String()) {
		if time.Now().After(deadline) {
			t.Fatal("the run never completed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	var delivered *payload.RunUsage
	calls := 0
	for _, e := range box.at(target.String()) {
		switch e.Kind {
		case envelope.KindUsageCall:
			calls++
		case envelope.KindCompleted:
			body, err := payload.Decode[payload.Lifecycle](e)
			if err != nil {
				t.Fatal(err)
			}
			delivered = body.Usage
		}
	}
	if calls == 0 || delivered == nil || delivered.Total.ModelCalls != calls {
		t.Fatalf("delivered %d usage.call events and a completed event with usage %+v", calls, delivered)
	}
	run, err := c.Run(ctx, "acme", acc.RunID)
	if err != nil {
		t.Fatalf("read run: %v", err)
	}
	if run.Status != server.StatusCompleted || run.Usage == nil || run.Usage.Total.ModelCalls != delivered.Total.ModelCalls {
		t.Fatalf("GetRun = status %q usage %+v, want the usage the run delivered %+v", run.Status, run.Usage, delivered)
	}
}

// The terminal summary is for a person: exact money from integer micro-units
// with the date of the rates, a cost said to be partial when some calls were
// unpriced, and no invented figure when none was priced.
func TestTheUsageSummaryShowsExactMoneyAndSaysWhatIsUnknown(t *testing.T) {
	tests := []struct {
		name  string
		usage payload.Usage
		want  string
	}{
		{"priced", payload.Usage{ModelCalls: 2, PromptTokens: 12_345, CachedPromptTokens: 9_000, CompletionTokens: 678,
			Cost: &payload.Cost{Micros: 40_500, Currency: "USD", AsOf: "2026-07-24"}},
			"2 calls, 12,345 in (9,000 cached, 0 written), 678 out, USD 0.040500 as of 2026-07-24"},
		{"partly priced", payload.Usage{ModelCalls: 3, PromptTokens: 10, CompletionTokens: 2, UnpricedCalls: 1,
			Cost: &payload.Cost{Micros: 1_250_000, Currency: "USD"}},
			"3 calls, 10 in, 2 out, at least USD 1.250000 (1 calls unpriced)"},
		{"unpriced", payload.Usage{ModelCalls: 1, PromptTokens: 700, CompletionTokens: 20, UnpricedCalls: 1},
			"1 call, 700 in, 20 out, cost unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := usageLine(tc.usage); got != tc.want {
				t.Errorf("usageLine:\n got %q\nwant %q", got, tc.want)
			}
		})
	}

	var out bytes.Buffer
	u := &ui{err: &out}
	logUsage(u, &payload.RunUsage{
		Total:   payload.Usage{ModelCalls: 1, PromptTokens: 6_500, CompletionTokens: 10, UnpricedCalls: 1},
		Context: &payload.ContextUsage{Window: 200_000, Tokens: 6_500},
	})
	if !strings.Contains(out.String(), "context 6,500/200,000") {
		t.Errorf("summary = %q, want how full the context was", out.String())
	}
}
