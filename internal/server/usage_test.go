package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/agentrt/payload"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// GetRun carries what a run spent, split into its own calls and what it
// delegated, with the cost as integer micro-units and the date of the rates,
// and how full its context was. A run with no record of usage has none on the
// wire, rather than a zero that would read as "spent nothing".
func TestGetRunCarriesTheRunsUsage(t *testing.T) {
	h := newHarness(t, nil)
	cost := &payload.Cost{Micros: 40_500, Currency: "USD", AsOf: "2026-07-24"}
	h.runs.put(Run{ID: "run_spent", Tenant: tenantAcme, Agent: "docs-bot", Status: StatusCompleted, EndedAt: fixedTime,
		Usage: &payload.RunUsage{
			Own:      payload.Usage{ModelCalls: 2, Requests: 2, PromptTokens: 12_000, CachedPromptTokens: 9_000, CacheWriteTokens: 500, CompletionTokens: 300, Cost: cost},
			Children: payload.Usage{ModelCalls: 1, Requests: 1, PromptTokens: 700, CompletionTokens: 20, UnpricedCalls: 1},
			Total: payload.Usage{ModelCalls: 3, Requests: 3, PromptTokens: 12_700, CachedPromptTokens: 9_000, CacheWriteTokens: 500, CompletionTokens: 320, Cost: cost, UnpricedCalls: 1,
				OtherCosts: []payload.Cost{{Micros: 900, Currency: "EUR", AsOf: "2026-07-01"}}},
			Context: &payload.ContextUsage{Model: "claude-test", Window: 200_000, Tokens: 6_500},
		}})
	h.runs.put(Run{ID: "run_old", Tenant: tenantAcme, Agent: "docs-bot", Status: StatusCompleted, EndedAt: fixedTime})

	read := func(id string) *mandatumv1.Run {
		t.Helper()
		got, err := h.invokeClient(tokenAcme).GetRun(context.Background(), connect.NewRequest(&mandatumv1.GetRunRequest{RunId: id}))
		if err != nil {
			t.Fatalf("get run %s: %v", id, err)
		}
		return got.Msg
	}
	u := read("run_spent").GetUsage()
	total := u.GetTotal()
	if total.GetModelCalls() != 3 || total.GetPromptTokens() != 12_700 || total.GetCachedPromptTokens() != 9_000 ||
		total.GetCacheWriteTokens() != 500 || total.GetCompletionTokens() != 320 || total.GetRequests() != 3 || total.GetUnpricedCalls() != 1 {
		t.Errorf("total = %v", total)
	}
	if c := total.GetCost(); c.GetMicros() != 40_500 || c.GetCurrency() != "USD" || c.GetAsOf() != "2026-07-24" {
		t.Errorf("total cost = %v, want 40500 micro-USD as of 2026-07-24", c)
	}
	if o := total.GetOtherCosts(); len(o) != 1 || o[0].GetCurrency() != "EUR" || o[0].GetMicros() != 900 {
		t.Errorf("other_costs = %v, want the EUR sum carried, not dropped", o)
	}
	if u.GetOwn().GetModelCalls() != 2 || u.GetChildren().GetModelCalls() != 1 || u.GetChildren().GetCost() != nil {
		t.Errorf("own %v children %v", u.GetOwn(), u.GetChildren())
	}
	if ctx := u.GetContext(); ctx.GetWindow() != 200_000 || ctx.GetTokens() != 6_500 || ctx.GetModel() != "claude-test" {
		t.Errorf("context = %v", ctx)
	}
	if old := read("run_old"); old.GetUsage() != nil {
		t.Errorf("a run with no record of usage reports %v", old.GetUsage())
	}
}

// The Run resource only grew. Every field it had keeps its number, its name
// and its type, and every run status keeps its value, so a client generated
// before usage existed reads a run exactly as it did.
func TestTheRunResourceOnlyGrew(t *testing.T) {
	fields := (&mandatumv1.Run{}).ProtoReflect().Descriptor().Fields()
	before := []struct {
		number protoreflect.FieldNumber
		name   protoreflect.Name
		kind   protoreflect.Kind
	}{
		{1, "run_id", protoreflect.StringKind},
		{2, "tenant", protoreflect.StringKind},
		{3, "agent", protoreflect.StringKind},
		{4, "rev", protoreflect.Int32Kind},
		{5, "origin", protoreflect.StringKind},
		{6, "topic", protoreflect.StringKind},
		{7, "status", protoreflect.EnumKind},
		{8, "tag", protoreflect.MessageKind},
		{9, "last_seq", protoreflect.Uint64Kind},
		{10, "error", protoreflect.StringKind},
		{11, "created_at", protoreflect.MessageKind},
		{12, "started_at", protoreflect.MessageKind},
		{13, "ended_at", protoreflect.MessageKind},
		{14, "stop_reason", protoreflect.StringKind},
		{15, "stopped_by", protoreflect.StringKind},
	}
	for _, f := range before {
		got := fields.ByNumber(f.number)
		if got == nil || got.Name() != f.name || got.Kind() != f.kind {
			t.Errorf("Run field %d is %v, want %s (%s)", f.number, got, f.name, f.kind)
		}
	}
	if u := fields.ByName("usage"); u == nil || u.Number() != 16 || u.Message().FullName() != "mandatum.v1.RunUsage" {
		t.Errorf("Run.usage = %v, want field 16 of type RunUsage", u)
	}
	if f := (&mandatumv1.Usage{}).ProtoReflect().Descriptor().Fields().ByName("other_costs"); f == nil || f.Number() != 9 || !f.IsList() {
		t.Errorf("Usage.other_costs = %v, want repeated field 9", f)
	}
	if fields.Len() != len(before)+1 {
		t.Errorf("Run has %d fields, want the %d it had plus usage", fields.Len(), len(before))
	}
	for status, n := range map[mandatumv1.RunStatus]int32{
		mandatumv1.RunStatus_RUN_STATUS_UNSPECIFIED: 0,
		mandatumv1.RunStatus_RUN_STATUS_ACCEPTED:    1,
		mandatumv1.RunStatus_RUN_STATUS_RUNNING:     2,
		mandatumv1.RunStatus_RUN_STATUS_PARKED:      3,
		mandatumv1.RunStatus_RUN_STATUS_COMPLETED:   4,
		mandatumv1.RunStatus_RUN_STATUS_FAILED:      5,
		mandatumv1.RunStatus_RUN_STATUS_STOPPED:     6,
	} {
		if int32(status) != n {
			t.Errorf("%s = %d, want %d", status, int32(status), n)
		}
	}
}

// Usage is what a host holds a tenant's spend to, and only the attempt that
// spent the tokens knows them, so a client cannot publish usage events onto a
// run's stream by hand.
func TestAClientCannotPublishUsage(t *testing.T) {
	h := newHarness(t, nil)
	for _, kind := range []envelope.Kind{envelope.KindUsageCall, envelope.KindUsageChild} {
		t.Run(string(kind), func(t *testing.T) {
			_, err := h.busClient(tokenAcme).Publish(context.Background(), connect.NewRequest(&mandatumv1.PublishRequest{
				Event: &mandatumv1.Event{Topic: "agent:docs-bot:run_1", RunId: "run_1", Kind: string(kind), Payload: []byte(`{"accounting_id":"x","prompt_tokens":1}`)},
			}))
			requireCode(t, err, connect.CodeInvalidArgument)
		})
	}
}
