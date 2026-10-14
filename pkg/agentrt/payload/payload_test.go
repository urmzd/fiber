package payload_test

import (
	"encoding/json"
	"testing"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/agentrt/payload"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// A payload is a wire contract: a reader that only has the JSON must be able to
// reconstruct the body, so every kind has to survive the round trip a bus makes
// it take.
func TestEveryKindSurvivesEncodeDecode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind envelope.Kind
		body any
	}{
		{"text", envelope.KindText, payload.Text{Text: "hello"}},
		{"thinking", envelope.KindThinking, payload.Thinking{Text: "considering"}},
		{"tool call", envelope.KindToolCall, payload.ToolCall{ID: "call_1", Name: "grep", Phase: payload.PhaseExecuting}},
		{"tool done", envelope.KindToolDone, payload.ToolDone{ID: "call_1", Name: "grep", Result: "3 hits"}},
		{"tool failed", envelope.KindToolDone, payload.ToolDone{ID: "call_1", Name: "grep", Error: "no such file"}},
		{"citation", envelope.KindCitation, payload.Citation{
			Label: "[1]", Text: "the quoted passage", Index: 0,
			Source: payload.Provenance{DocumentUUID: "doc-1", SourceURI: "https://example.test/a", SectionIndex: 2},
		}},
		{"lifecycle", envelope.KindCompleted, payload.Lifecycle{Attempt: 2, Rev: 6, Text: "done"}},
		{"failure", envelope.KindFailed, payload.Lifecycle{Attempt: 1, Rev: 6, Error: "boom", Terminal: true}},
		{"stopped", envelope.KindStopped, payload.Stopped{Attempt: 1, Rev: 6, Reason: "Stopped by a user", By: "slack:U1", Text: "So far"}},
		{"stop request", envelope.KindStop, payload.StopRequest{RunID: "r0123", Reason: "Stopped by a user", By: "slack:U1"}},
		{"usage call", envelope.KindUsageCall, payload.UsageCall{
			AccountingID: "r0123/a1/1", Agent: "docs-bot", Model: "claude-test", Requests: 1,
			PromptTokens: 1200, CachedPromptTokens: 900, CacheWriteTokens: 100, CompletionTokens: 40,
			Cost: &payload.Cost{Micros: 1234, Currency: "USD", AsOf: "2026-07-24"}, ContextWindow: 200_000,
		}},
		{"usage child", envelope.KindUsageChild, payload.UsageChild{
			AccountingID: "run:r9", RunID: "r9", Agent: "helper", Depth: 1,
			Usage: payload.Usage{ModelCalls: 2, Requests: 2, PromptTokens: 10, CompletionTokens: 3, UnpricedCalls: 2},
		}},
		{"completed with usage", envelope.KindCompleted, payload.Lifecycle{Attempt: 1, Rev: 6, Text: "done", Usage: &payload.RunUsage{
			Own:     payload.Usage{ModelCalls: 1, Requests: 1, PromptTokens: 10, CompletionTokens: 2, Cost: &payload.Cost{Micros: 5, Currency: "USD"}},
			Total:   payload.Usage{ModelCalls: 1, Requests: 1, PromptTokens: 10, CompletionTokens: 2, Cost: &payload.Cost{Micros: 5, Currency: "USD"}},
			Context: &payload.ContextUsage{Model: "claude-test", Window: 200_000, Tokens: 10},
		}}},
		{"action", envelope.KindActionTaken, payload.Action{
			Name: "jira_transition", Target: "jira://acme/PROJ-5", Summary: "moved to done",
			Address: "jira://acme/PROJ-5", Data: json.RawMessage(`{"status":"done"}`),
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := payload.Encode(tc.body)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			e := envelope.Event{Kind: tc.kind, Payload: raw}

			got, known, err := payload.Of(e)
			if err != nil {
				t.Fatalf("Of: %v", err)
			}
			if !known {
				t.Fatalf("Of reported kind %s as unknown", tc.kind)
			}
			// Compare through JSON: a body may hold a json.RawMessage, which
			// is a slice and so is not comparable with ==.
			gotJSON, _ := json.Marshal(got)
			wantJSON, _ := json.Marshal(tc.body)
			if string(gotJSON) != string(wantJSON) {
				t.Fatalf("round trip changed the body:\n got %s\nwant %s", gotJSON, wantJSON)
			}
		})
	}
}

// An old client must survive a newer kind rather than dropping it, because kind
// is an open string and connectors ship independently of this package.
func TestUnknownKindDegradesToRawPayload(t *testing.T) {
	t.Parallel()

	e := envelope.Event{Kind: envelope.Kind("delta.hologram"), Payload: []byte(`{"shape":"cube"}`)}
	body, known, err := payload.Of(e)
	if err != nil {
		t.Fatalf("Of: %v", err)
	}
	if known {
		t.Fatal("an unrecognised kind must not be reported as known")
	}
	raw, ok := body.(json.RawMessage)
	if !ok {
		t.Fatalf("body is %T, want json.RawMessage", body)
	}
	if string(raw) != `{"shape":"cube"}` {
		t.Fatalf("raw payload is %s, want it passed through untouched", raw)
	}
}

// A kind may carry no body at all; a reader must not have to distinguish that
// from an empty one.
func TestEmptyPayloadDecodesToZeroValue(t *testing.T) {
	t.Parallel()

	got, err := payload.Decode[payload.Lifecycle](envelope.Event{Kind: envelope.KindStarted})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != (payload.Lifecycle{}) {
		t.Fatalf("got %+v, want the zero value", got)
	}
}

// Addresses travel as URIs so that Go field names never reach the wire; they
// must come back as addresses.
func TestActionAddressesRoundTripThroughTheirURIs(t *testing.T) {
	t.Parallel()

	target := address.MustParse("github://urmzd/mandatum/issues/42")
	result := address.MustParse("github://urmzd/mandatum/issues/42/comments/7")
	a := payload.Action{Name: "github_comment", Target: target.String(), Address: result.String()}

	raw, err := payload.Encode(a)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	back, err := payload.Decode[payload.Action](envelope.Event{Kind: envelope.KindActionTaken, Payload: raw})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	gotTarget, err := back.TargetAddress()
	if err != nil {
		t.Fatalf("TargetAddress: %v", err)
	}
	if gotTarget.String() != target.String() {
		t.Fatalf("target is %s, want %s", gotTarget, target)
	}
	gotResult, err := back.ResultAddress()
	if err != nil {
		t.Fatalf("ResultAddress: %v", err)
	}
	if gotResult.String() != result.String() {
		t.Fatalf("address is %s, want %s", gotResult, result)
	}
}

// An action with no addresses must not turn the empty string into an error.
func TestAbsentAddressesParseAsUnset(t *testing.T) {
	t.Parallel()

	a := payload.Action{Name: "cron_noop"}
	for _, tc := range []struct {
		name string
		get  func() (address.Address, error)
	}{
		{"target", a.TargetAddress},
		{"address", a.ResultAddress},
	} {
		got, err := tc.get()
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !got.IsZero() {
			t.Fatalf("%s is %s, want the zero address", tc.name, got)
		}
	}
}

// The field names are the contract. This test fails on a rename, which is the
// point: a renamed field silently breaks every deployed client.
func TestJSONFieldNamesAreStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body any
		want string
	}{
		{payload.Text{Text: "a"}, `{"text":"a"}`},
		{payload.Thinking{Text: "a"}, `{"text":"a"}`},
		{payload.ToolCall{ID: "1", Name: "n", Phase: payload.PhaseRequested}, `{"id":"1","name":"n","phase":"requested"}`},
		{payload.ToolDone{ID: "1"}, `{"id":"1"}`},
		{payload.Lifecycle{Attempt: 1}, `{"attempt":1}`},
		{payload.Citation{Label: "[1]", Text: "t"}, `{"label":"[1]","text":"t","source":{},"index":0}`},
		{payload.Action{Name: "n"}, `{"name":"n"}`},
		{
			payload.Usage{ModelCalls: 2, PromptTokens: 10, CachedPromptTokens: 6, CacheWriteTokens: 3, CompletionTokens: 4},
			`{"model_calls":2,"prompt_tokens":10,"cached_prompt_tokens":6,"cache_write_tokens":3,"completion_tokens":4}`,
		},
		{
			payload.Compaction{Trigger: "input_pressure", Strategy: "summary", TokensBefore: 9, TokensAfter: 4, Summarized: 6, Kept: 5},
			`{"trigger":"input_pressure","strategy":"summary","tokens_before":9,"tokens_after":4,"summarized":6,"kept":5}`,
		},
	}
	for _, tc := range tests {
		raw, err := payload.Encode(tc.body)
		if err != nil {
			t.Fatalf("Encode %T: %v", tc.body, err)
		}
		if string(raw) != tc.want {
			t.Errorf("%T encodes as %s, want %s", tc.body, raw, tc.want)
		}
	}
}

// Cache health is read as one number, and it must not divide by zero for a
// provider that reports no usage at all.
func TestCacheReadRatioIsTheCachedShareOfInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		u    payload.Usage
		want float64
	}{
		{"no usage reported", payload.Usage{}, 0},
		{"cold cache", payload.Usage{PromptTokens: 100, CacheWriteTokens: 100}, 0},
		{"warm cache", payload.Usage{PromptTokens: 200, CachedPromptTokens: 150}, 0.75},
	}
	for _, tc := range tests {
		if got := tc.u.CacheReadRatio(); got != tc.want {
			t.Errorf("%s: CacheReadRatio = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func call(id string, depth int, in, out int, micros int64, currency, asOf string) payload.UsageCall {
	c := payload.UsageCall{AccountingID: id, Depth: depth, Requests: 1, PromptTokens: in, CompletionTokens: out}
	if currency != "" {
		c.Cost = &payload.Cost{Micros: micros, Currency: currency, AsOf: asOf}
	}
	return c
}

// Delivery is at-least-once and a call is relayed up every level of a
// delegation, so the same report can arrive more than once. A tally counts an
// AccountingID once, keeps a run's own calls apart from what it delegated, and
// adds money as integers.
func TestATallyCountsEachAccountingIDOnce(t *testing.T) {
	t.Parallel()

	var tally payload.Tally
	reports := []payload.UsageCall{
		call("r/a1/1", 0, 1000, 50, 4500, "USD", "2026-07-24"),
		call("r/a1/1", 0, 1000, 50, 4500, "USD", "2026-07-24"), // redelivered
		call("r/a1/helper.sub-1/1", 1, 300, 20, 1200, "USD", "2026-06-01"),
		call("r/a1/2", 0, 1100, 60, 5100, "USD", "2026-07-24"),
	}
	fresh := 0
	for _, c := range reports {
		if tally.Call(c) {
			fresh++
		}
	}
	child := payload.UsageChild{AccountingID: payload.ChildRunAccountingID("r9"), RunID: "r9", Depth: 1,
		Usage: payload.Usage{ModelCalls: 2, Requests: 2, PromptTokens: 50, CompletionTokens: 5, Cost: &payload.Cost{Micros: 300, Currency: "USD", AsOf: "2026-07-24"}}}
	if !tally.Child(child) || tally.Child(child) {
		t.Fatal("a child run must be counted the first time it is reported and never again")
	}
	if fresh != 3 {
		t.Fatalf("%d calls counted as new, want 3", fresh)
	}

	got := tally.Usage()
	if got.Own.ModelCalls != 2 || got.Own.PromptTokens != 2100 || got.Own.Cost.Micros != 9600 {
		t.Errorf("Own = %+v cost %+v, want the run's two calls", got.Own, got.Own.Cost)
	}
	if got.Children.ModelCalls != 3 || got.Children.Cost.Micros != 1500 || got.Children.Cost.AsOf != "2026-06-01" {
		t.Errorf("Children = %+v cost %+v, want the sub-task's call and the child run, dated by the oldest card", got.Children, got.Children.Cost)
	}
	if got.Total.ModelCalls != 5 || got.Total.Cost.Micros != 11100 || got.Total.Cost.AsOf != "2026-06-01" {
		t.Errorf("Total = %+v cost %+v, want own and children added", got.Total, got.Total.Cost)
	}
	if got.Context == nil || got.Context.Tokens != 1100 {
		t.Errorf("Context = %+v, want the run's own latest call", got.Context)
	}
}

// Money adds only within one currency, and nothing is converted or guessed: a
// call in another currency, or with no price at all, is counted in
// UnpricedCalls and its tokens still count. A total priced from a card with no
// date has no date.
func TestCostsAddOnlyWithinOneCurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		calls        []payload.UsageCall
		wantMicros   int64
		wantNoCost   bool
		wantAsOf     string
		wantUnpriced int
	}{
		{"one currency", []payload.UsageCall{call("a", 0, 1, 1, 10, "USD", "2026-07-24"), call("b", 0, 1, 1, 20, "USD", "2026-07-24")}, 30, false, "2026-07-24", 0},
		{"a second currency", []payload.UsageCall{call("a", 0, 1, 1, 10, "USD", "2026-07-24"), call("b", 0, 1, 1, 20, "EUR", "2026-07-24")}, 10, false, "2026-07-24", 1},
		{"an unpriced call", []payload.UsageCall{call("a", 0, 1, 1, 10, "USD", "2026-07-24"), call("b", 0, 1, 1, 0, "", "")}, 10, false, "2026-07-24", 1},
		{"nothing priced", []payload.UsageCall{call("a", 0, 1, 1, 0, "", "")}, 0, true, "", 1},
		{"an undated card", []payload.UsageCall{call("a", 0, 1, 1, 10, "USD", "2026-07-24"), call("b", 0, 1, 1, 20, "USD", "")}, 30, false, "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var u payload.Usage
			for _, c := range tc.calls {
				u = u.AddCall(c)
			}
			if u.PromptTokens != len(tc.calls) {
				t.Errorf("PromptTokens = %d, want every call's tokens counted", u.PromptTokens)
			}
			if u.UnpricedCalls != tc.wantUnpriced {
				t.Errorf("UnpricedCalls = %d, want %d", u.UnpricedCalls, tc.wantUnpriced)
			}
			if tc.wantNoCost {
				if u.Cost != nil {
					t.Errorf("Cost = %+v, want none", *u.Cost)
				}
				return
			}
			if u.Cost == nil || u.Cost.Micros != tc.wantMicros || u.Cost.AsOf != tc.wantAsOf {
				t.Errorf("Cost = %+v, want %d micros as of %q", u.Cost, tc.wantMicros, tc.wantAsOf)
			}
		})
	}
}
