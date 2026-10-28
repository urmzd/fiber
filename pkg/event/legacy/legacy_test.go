package legacy_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/event/legacy"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/topic"
)

var at = time.Date(2026, 10, 28, 14, 3, 11, 0, time.UTC)

// oldKinds reads every envelope.Kind constant from its source, so a kind
// added there without a mapping here fails the test.
func oldKinds(t *testing.T) map[string]string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "..", "envelope", "envelope.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, d := range f.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok || g.Tok != token.CONST {
			continue
		}
		for _, s := range g.Specs {
			v := s.(*ast.ValueSpec)
			if id, ok := v.Type.(*ast.Ident); !ok || id.Name != "Kind" {
				continue
			}
			for i, n := range v.Names {
				lit := v.Values[i].(*ast.BasicLit)
				val, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatal(err)
				}
				out[n.Name] = val
			}
		}
	}
	if len(out) < 20 {
		t.Fatalf("found only %d Kind constants: %v", len(out), out)
	}
	return out
}

func TestEveryOldKindMaps(t *testing.T) {
	kinds := oldKinds(t)
	var dropped []string
	for name, kind := range kinds {
		typ, err := legacy.TypeOf(kind)
		switch {
		case errors.Is(err, legacy.ErrDropped):
			dropped = append(dropped, kind)
		case err != nil:
			t.Errorf("%s (%q) maps to nothing: %v", name, kind, err)
		default:
			if _, ok := event.Lookup(typ); !ok {
				t.Errorf("%s maps to %q, which is not registered", name, typ)
			}
		}
	}
	slices.Sort(dropped)
	if want := []string{"control.created", "control.deleted", "control.revised"}; !slices.Equal(dropped, want) {
		t.Errorf("dropped = %v, want %v", dropped, want)
	}
	if got := legacy.DroppedKinds(); !slices.Equal(got, dropped) {
		t.Errorf("DroppedKinds() = %v, want %v", got, dropped)
	}
	// No mapping names a kind that does not exist.
	values := slices.Collect(func(yield func(string) bool) {
		for _, v := range kinds {
			if !yield(v) {
				return
			}
		}
	})
	for _, k := range legacy.KeptKinds() {
		if !slices.Contains(values, k) {
			t.Errorf("KeptKinds has %q, which is no envelope.Kind", k)
		}
	}
	if len(legacy.KeptKinds())+len(dropped) != len(kinds) {
		t.Errorf("%d kept and %d dropped of %d kinds", len(legacy.KeptKinds()), len(dropped), len(kinds))
	}
	if _, err := legacy.TypeOf("delta.future"); !errors.Is(err, legacy.ErrUnknown) {
		t.Errorf("TypeOf(unknown) = %v, want ErrUnknown", err)
	}
}

// Each old selector selects, through its prefixes, exactly the types its old
// kinds map to, apart from the dropped ones.
func TestKindSelectorSelectsWhatItDid(t *testing.T) {
	kinds := oldKinds(t)
	selectors := map[string]bool{}
	for _, k := range kinds {
		selectors[k] = true
		selectors[envelope.Kind(k).Family()] = true
	}
	for sel := range selectors {
		prefixes, err := legacy.KindSelector(sel)
		if err != nil {
			if errors.Is(err, legacy.ErrDropped) {
				continue
			}
			t.Errorf("KindSelector(%q): %v", sel, err)
			continue
		}
		for _, k := range kinds {
			typ, err := legacy.TypeOf(k)
			if err != nil {
				continue
			}
			was := envelope.Kind(k).Matches(sel)
			now := slices.ContainsFunc(prefixes, func(p string) bool { return event.Selects(p, typ) })
			if was != now {
				t.Errorf("selector %q on %q: was %v, now %v (prefixes %v)", sel, k, was, now, prefixes)
			}
		}
	}
	for _, bad := range []string{"", "lifecycle.done", "deltas", "control.created"} {
		if _, err := legacy.KindSelector(bad); err == nil {
			t.Errorf("KindSelector(%q) is not an error", bad)
		}
	}
	// An empty route selected every kind that is not opt-in.
	all, err := legacy.KindSelectors(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range kinds {
		typ, err := legacy.TypeOf(k)
		if err != nil || event.Family(typ) != "run" {
			continue
		}
		was := envelope.Route{}.Wants(envelope.Kind(k))
		now := slices.ContainsFunc(all, func(p string) bool { return event.Selects(p, typ) })
		if was != now {
			t.Errorf("empty route on %q: was %v, now %v", k, was, now)
		}
	}
}

// wireEvent is the v0.25 bus encoding, as pkg/bus wrote it.
type wireEvent struct {
	envelope.Event
	Topic string `json:"topic"`
	Seq   uint64 `json:"seq,omitempty"`
}

func oldEvent(t *testing.T, kind envelope.Kind, body any) envelope.Event {
	t.Helper()
	p, err := payload.Encode(body)
	if err != nil {
		t.Fatal(err)
	}
	top, err := topic.Run("helper", "r1")
	if err != nil {
		t.Fatal(err)
	}
	return envelope.Event{
		Seq: 7, Topic: top, RunID: "r1", Tenant: "acme", Agent: "helper", Rev: 3,
		Origin: "slack", Kind: kind, Payload: p, At: at,
	}
}

type oldCase struct {
	kind envelope.Kind
	in   any
	want event.Body
}

func oldCases() []oldCase {
	q := payload.Question{ID: "q1", Kind: "approval", Prompt: "May I?", Options: []string{"yes", "no"}}
	usage := &payload.RunUsage{Own: payload.Usage{PromptTokens: 9, Cost: &payload.Cost{Micros: 12, Currency: "USD"}}}
	wantUsage := &event.RunUsage{Own: event.Usage{PromptTokens: 9, Cost: &event.Cost{Micros: 12, Currency: "USD"}}}
	// A lifecycle payload with every field set: each type keeps its own.
	full := payload.Lifecycle{
		Attempt: 2, Rev: 3, Error: "boom", Terminal: true, Reason: "waiting", Text: "done", Usage: usage,
		Question: &q, Answer: &payload.Answer{QuestionID: "q1", Decision: "approve"},
	}
	wq := event.Question{ID: "q1", Kind: "approval", Prompt: "May I?", Options: []string{"yes", "no"}}
	return []oldCase{
		{envelope.KindAccepted, full, event.Accepted{Attempt: 2, Rev: 3}},
		{envelope.KindStarted, full, event.Started{Attempt: 2, Rev: 3}},
		{envelope.KindParked, full, event.Parked{Attempt: 2, Rev: 3, Reason: "waiting", Question: &wq}},
		{envelope.KindResumed, full, event.Resumed{Attempt: 2, Rev: 3, Answer: &event.Answer{QuestionID: "q1", Decision: "approve"}}},
		{envelope.KindCompleted, full, event.Completed{Attempt: 2, Rev: 3, Text: "done", Usage: wantUsage}},
		{envelope.KindFailed, full, event.Failed{Attempt: 2, Rev: 3, Error: "boom", Terminal: true}},
		{envelope.KindStopped, payload.Stopped{Attempt: 1, Reason: "enough", By: "ana", Text: "so far"}, event.Stopped{Attempt: 1, Reason: "enough", By: "ana", Text: "so far"}},
		{envelope.KindChildParked, payload.ChildQuestion{RunID: "r2", Agent: "deployer", Question: q}, event.ChildParked{RunID: "r2", Agent: "deployer", Question: wq}},
		{envelope.KindText, payload.Text{Text: "Hello"}, event.Text{Text: "Hello"}},
		{envelope.KindThinking, payload.Thinking{Text: "Hmm"}, event.Thinking{Text: "Hmm"}},
		{envelope.KindToolCall, payload.ToolCall{ID: "c1", Name: "search", Phase: "executing", Arguments: json.RawMessage(`{"q":"x"}`)},
			event.ToolCall{ID: "c1", Name: "search", Phase: "executing", Arguments: json.RawMessage(`{"q":"x"}`)}},
		{envelope.KindToolDone, payload.ToolDone{ID: "c1", Name: "search", Result: "3 hits"}, event.ToolDone{ID: "c1", Name: "search", Result: "3 hits"}},
		{envelope.KindCitation, payload.Citation{Label: "[1]", Text: "q", Source: payload.Provenance{SourceURI: "https://x"}},
			event.Citation{Label: "[1]", Text: "q", Source: event.Provenance{SourceURI: "https://x"}}},
		{envelope.KindActionTaken, payload.Action{Name: "jira_comment", Target: "jira://acme/P-1", Summary: "ok"},
			event.Action{Name: "jira_comment", Target: "jira://acme/P-1", Summary: "ok"}},
		{envelope.KindUsageCall, payload.UsageCall{AccountingID: "r1/a1/1", Model: "m", PromptTokens: 9},
			event.UsageCall{AccountingID: "r1/a1/1", Model: "m", PromptTokens: 9}},
		{envelope.KindUsageChild, payload.UsageChild{AccountingID: "run:r2", RunID: "r2", Usage: payload.Usage{CompletionTokens: 4}},
			event.UsageChild{AccountingID: "run:r2", RunID: "r2", Usage: event.Usage{CompletionTokens: 4}}},
		{envelope.KindCompacted, map[string]any{"trigger": "input_pressure", "summarized": 3, "kept": 1},
			event.Compaction{Trigger: "input_pressure", Summarized: 3, Kept: 1}},
		{envelope.KindStop, payload.StopRequest{RunID: "r1", Reason: "stop", By: "ana", At: at}, event.StopRequest{RunID: "r1", Reason: "stop", By: "ana", At: at}},
		{envelope.KindCompact, payload.CompactRequest{RunID: "r1", By: "ana", At: at}, event.CompactRequest{RunID: "r1", By: "ana", At: at}},
	}
}

func checkDecoded(t *testing.T, c oldCase, got event.Envelope) {
	t.Helper()
	want, _ := legacy.TypeOf(string(c.kind))
	if got.Type != want || got.Run != "r1" || got.Seq != 7 || got.Tenant != "acme" || got.Agent != "helper" ||
		got.Version != "3" || !got.Time.Equal(at) || got.ID != "r1:7" || got.Channel != legacy.RunChannel("acme", "helper", "r1") {
		t.Errorf("%s: envelope = %+v", c.kind, got)
	}
	if !reflect.DeepEqual(got.Data, c.want) {
		t.Errorf("%s: data = %#v, want %#v", c.kind, got.Data, c.want)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("%s: Validate: %v", c.kind, err)
	}
}

func TestDecodeBusRow(t *testing.T) {
	for _, c := range oldCases() {
		e := oldEvent(t, c.kind, c.in)
		body, err := json.Marshal(wireEvent{Event: e, Topic: e.Topic.String()})
		if err != nil {
			t.Fatal(err)
		}
		got, err := legacy.DecodeBusRow(7, body)
		if err != nil {
			t.Fatalf("%s: %v", c.kind, err)
		}
		checkDecoded(t, c, got)
	}
	for kind, want := range map[envelope.Kind]error{
		envelope.KindAgentCreated: legacy.ErrDropped,
		"delta.future":            legacy.ErrUnknown,
	} {
		e := oldEvent(t, kind, map[string]any{})
		body, _ := json.Marshal(wireEvent{Event: e, Topic: e.Topic.String()})
		if _, err := legacy.DecodeBusRow(7, body); !errors.Is(err, want) {
			t.Errorf("%s: err = %v, want %v", kind, err, want)
		}
	}
	e := oldEvent(t, envelope.KindText, map[string]any{"text": 5})
	body, _ := json.Marshal(wireEvent{Event: e, Topic: e.Topic.String()})
	if _, err := legacy.DecodeBusRow(7, body); err == nil {
		t.Error("a broken known payload decoded")
	}
}

func TestDecodeEntry(t *testing.T) {
	for _, c := range oldCases() {
		raw, err := json.Marshal(runtime.EntryOf(oldEvent(t, c.kind, c.in)))
		if err != nil {
			t.Fatal(err)
		}
		got, err := legacy.DecodeEntry(raw)
		if err != nil {
			t.Fatalf("%s: %v", c.kind, err)
		}
		checkDecoded(t, c, got)
	}
}

func decodeTag(t *testing.T, tag envelope.Tag) legacy.Trigger {
	t.Helper()
	raw, err := json.Marshal(tag)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := legacy.DecodeTag(raw)
	if err != nil {
		t.Fatalf("DecodeTag: %v", err)
	}
	e, ok := event.Lookup(tr.Event.Type)
	if !ok {
		t.Fatalf("%s is not registered", tr.Event.Type)
	}
	got, err := e.Render(tr.Event)
	if err != nil {
		t.Fatal(err)
	}
	if want := agentrt.RequestText(tag.Text, tag.Meta); got != want {
		t.Errorf("%s renders\n%q, want\n%q", tr.Event.Type, got, want)
	}
	return tr
}

func TestDecodeTag(t *testing.T) {
	slackSource := address.Address{Connector: "slack", Workspace: "T01", Path: []string{"C02"}}.WithParam("thread", "1699.001")
	for _, tc := range []struct {
		name string
		tag  envelope.Tag
		want string
	}{
		{"slack mention", envelope.Tag{ID: "Ev1", Origin: "slack", Agent: "helper", Source: slackSource, Text: "hi",
			Meta: map[string]string{"slack_team": "T01", "slack_channel": "C02", "slack_user": "U1", "slack_event": "app_mention", "slack_ts": "1699.002", "slack_thread_ts": "1699.001"}},
			event.TypeMessagePosted},
		{"slash command", envelope.Tag{ID: "command:1", Origin: "slack", Text: "/ask x",
			Meta: map[string]string{"slack_team": "T01", "slack_channel": "C02", "slack_user": "U1", "slack_event": "slash_command", "slack_command": "/ask"}},
			event.TypeSlackCommandInvoked},
		{"console", envelope.Tag{ID: "c1", Origin: "console", Agent: "helper", Text: "hi", Meta: map[string]string{"humagen.project": "p"}},
			event.TypeMessagePosted},
		{"github review comment", envelope.Tag{ID: "d1", Origin: "github", Text: "look",
			Meta: map[string]string{"github_event": "pull_request_review_comment", "github_action": "created", "github_owner": "o", "github_repo": "r",
				"github_number": "4", "github_kind": "pull", "github_title": "T", "github_sender": "s", "github_comment_id": "9", "github_review_root_id": "8",
				"github_labels": "a,b", "github_draft": "true", "github_installation_id": "77"}},
			event.TypeGitHubReviewCommentCreated},
		{"github with a malformed number", envelope.Tag{ID: "d2", Origin: "github", Text: "look",
			Meta: map[string]string{"github_event": "issues", "github_action": "opened", "github_owner": "o", "github_repo": "r",
				"github_number": "four", "github_kind": "issue", "github_title": "T", "github_sender": "s"}},
			event.TypeMessagePosted},
		{"github event without a type", envelope.Tag{ID: "d3", Origin: "github", Text: "x", Meta: map[string]string{"github_event": "push", "github_action": ""}},
			event.TypeMessagePosted},
		{"jira assigned", envelope.Tag{ID: "j1", Origin: "jira", Text: "ticket",
			Meta: map[string]string{"jira_event": "jira:issue_updated", "jira_site": "acme", "jira_key": "P-1", "jira_assigned": "true", "jira_labels": "x"}},
			event.TypeJiraIssueAssigned},
		{"jira comment", envelope.Tag{ID: "j2", Origin: "jira", Text: "please",
			Meta: map[string]string{"jira_event": "comment_created", "jira_site": "acme", "jira_key": "P-1", "jira_comment_id": "100"}},
			event.TypeJiraCommentCreated},
		{"jira created", envelope.Tag{ID: "j3", Origin: "jira", Text: "new",
			Meta: map[string]string{"jira_event": "jira:issue_created", "jira_site": "acme", "jira_key": "P-1"}},
			event.TypeJiraIssueCreated},
		{"cron", envelope.Tag{ID: "nightly@1", Origin: "cron", Tenant: "acme", Agent: "helper", Text: "report",
			Source: address.Address{Connector: "cron", Workspace: "acme", Path: []string{"nightly"}},
			Meta:   map[string]string{"cron_schedule": "nightly", "cron_expression": "0 3 * * *", "cron_occurrence": "2026-10-28T03:00:00Z", "team": "ops"}},
			event.TypeCronTick},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.tag.At = at
			tr := decodeTag(t, tc.tag)
			if tr.Event.Type != tc.want {
				t.Errorf("type = %s, want %s", tr.Event.Type, tc.want)
			}
			if tr.Event.ID != tc.tag.ID || tr.Agent != tc.tag.Agent || !tr.Event.Time.Equal(at) || tr.Event.Tenant != tc.tag.Tenant {
				t.Errorf("trigger = %+v", tr)
			}
			if !tc.tag.Source.IsZero() && tr.Event.Channel != tc.tag.Source.String() {
				t.Errorf("channel = %q, want %q", tr.Event.Channel, tc.tag.Source.String())
			}
			if tr.Event.Channel == "" || tr.Event.Source == "" {
				t.Errorf("no channel or source: %+v", tr.Event)
			}
		})
	}
}

func TestDecodeTagMovesPrivateMetaAndRoutes(t *testing.T) {
	target := address.Address{Connector: "webhook", Workspace: "acme", Path: []string{"hook"}}
	tag := envelope.Tag{
		ID: "c1", Origin: "console", Agent: "helper", Text: "hi", At: at,
		Meta:    map[string]string{"_host.sources": "playbook", "channel": "ops"},
		Deliver: []envelope.Route{{Target: target, Kinds: []string{"lifecycle.completed", "usage"}}, {Target: target}},
	}
	tr := decodeTag(t, tag)
	if tr.Event.Private["_host.sources"] != "playbook" || len(tr.Event.Private) != 1 {
		t.Errorf("Private = %v", tr.Event.Private)
	}
	b := tr.Event.Data.(event.MessagePosted)
	if !reflect.DeepEqual(b.Context, map[string]string{"channel": "ops"}) || !slices.Equal(b.Mentions, []string{"helper"}) {
		t.Errorf("body = %+v", b)
	}
	want := []legacy.Route{
		{Target: target.String(), Types: []string{event.TypeCompleted, "run.usage"}},
		{Target: target.String(), Types: []string{"run"}},
	}
	if !reflect.DeepEqual(tr.Deliver, want) {
		t.Errorf("Deliver = %+v, want %+v", tr.Deliver, want)
	}
	raw, _ := json.Marshal(tr.Event)
	if bytes.Contains(raw, []byte("playbook")) || bytes.Contains(raw, []byte("_host")) {
		t.Errorf("private meta serialized: %s", raw)
	}

	tag.Deliver = []envelope.Route{{Target: target, Kinds: []string{"lifecycle.finished"}}}
	raw, _ = json.Marshal(tag)
	if _, err := legacy.DecodeTag(raw); !errors.Is(err, legacy.ErrUnknown) {
		t.Errorf("an unknown selector: err = %v, want ErrUnknown", err)
	}
}
