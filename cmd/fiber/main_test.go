package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/envelope"
)

// docsBot is a spec as a deployment would write it: a real provider, a
// fallback on another, and grants this local composition only partly has.
const docsBot = `{
  "name": "docs-bot",
  "description": "answers questions about how we ship",
  "provider": "anthropic",
  "model": "house-model",
  "system_prompt": "You answer questions about the deploy pipeline.",
  "fallbacks": [{"provider": "openai", "model": "other-model"}]
}`

// noKeys makes sure no vendor key is in reach, so a test that passes did so
// without one.
func noKeys(t *testing.T) {
	t.Helper()
	for _, k := range []string{agentrt.EnvAnthropicKey, agentrt.EnvOpenAIKey, agentrt.EnvOpenAIBaseURL} {
		t.Setenv(k, "")
	}
}

func writeSpec(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// fiberCLI runs the command line as main does and returns its exit status
// and output.
func fiberCLI(t *testing.T, stdin string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var out, errs bytes.Buffer
	code = cli(ctx, args, strings.NewReader(stdin), &out, &errs)
	return code, out.String(), errs.String()
}

// The point of the command: a spec written for a real model runs end to end
// on the offline provider with no key at all, its fallback to another vendor
// included, and the run's events are printed in the order they happened,
// ending in its completion.
func TestASpecRunsOfflineEndToEndWithNoKey(t *testing.T) {
	noKeys(t)
	code, stdout, stderr := fiberCLI(t, "", "run", "-offline", writeSpec(t, docsBot), "how does a release get out the door?")
	if code != exitCompleted {
		t.Fatalf("exit %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitCompleted, stdout, stderr)
	}
	at := -1
	for _, want := range []string{
		"[lifecycle.accepted] docs-bot revision 1, run ",
		"[lifecycle.started] attempt 1",
		"docs-bot is running with the offline provider, so no model was called.",
		"[usage.call] house-model:",
		"[lifecycle.completed] attempt 1",
	} {
		i := strings.Index(stdout, want)
		if i < 0 || i < at {
			t.Fatalf("stdout does not have %q after what came before it:\n%s", want, stdout)
		}
		at = i
	}
	if strings.Count(stdout, "running with the offline provider") != 1 {
		t.Errorf("the answer was printed more than once:\n%s", stdout)
	}
}

// With -json every line of stdout is one event, as the bus carried it, with
// its payload as JSON: a program can read the run without parsing text.
func TestJSONPrintsOneEventPerLine(t *testing.T) {
	noKeys(t)
	code, stdout, stderr := fiberCLI(t, "", "run", "-json", "-offline", writeSpec(t, docsBot), "how?")
	if code != exitCompleted {
		t.Fatalf("exit %d\nstderr:\n%s", code, stderr)
	}
	var kinds []envelope.Kind
	var said strings.Builder
	var last uint64
	var completed payload.Lifecycle
	runs := map[string]bool{}
	sc := bufio.NewScanner(strings.NewReader(stdout))
	for sc.Scan() {
		var e jsonEvent
		dec := json.NewDecoder(strings.NewReader(sc.Text()))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&e); err != nil {
			t.Fatalf("line %q is not one event: %v", sc.Text(), err)
		}
		if e.Seq <= last {
			t.Errorf("seq %d after %d", e.Seq, last)
		}
		last = e.Seq
		kinds = append(kinds, e.Kind)
		runs[e.RunID] = true
		if e.Agent != "docs-bot" || e.Rev != 1 || e.Topic != "agent:docs-bot:"+e.RunID {
			t.Errorf("event %+v is not docs-bot revision 1 on its run's topic", e)
		}
		switch e.Kind {
		case envelope.KindText:
			var p payload.Text
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatal(err)
			}
			said.WriteString(p.Text)
		case envelope.KindCompleted:
			if err := json.Unmarshal(e.Payload, &completed); err != nil {
				t.Fatal(err)
			}
		}
	}
	want := []envelope.Kind{envelope.KindAccepted, envelope.KindStarted, envelope.KindText, envelope.KindUsageCall, envelope.KindCompleted}
	if !slices.Equal(kinds, want) {
		t.Errorf("kinds %v, want %v", kinds, want)
	}
	if len(runs) != 1 {
		t.Errorf("events of %d runs, want one", len(runs))
	}
	if completed.Text == "" || completed.Text != said.String() {
		t.Errorf("completed with %q, the streamed answer was %q", completed.Text, said.String())
	}
}

// -provider and -model replace the spec's, and the run is on what they
// name: here the offline provider, under a model name of the test's.
func TestProviderAndModelFlagsReplaceTheSpecs(t *testing.T) {
	noKeys(t)
	code, stdout, stderr := fiberCLI(t, "", "run", "-json", "-provider", "offline", "-model", "chosen-here", writeSpec(t, `{
  "name": "docs-bot", "provider": "anthropic", "model": "house-model", "system_prompt": "Answer."
}`), "how?")
	if code != exitCompleted {
		t.Fatalf("exit %d\nstderr:\n%s", code, stderr)
	}
	if !strings.Contains(stdout, `"kind":"usage.call"`) || !strings.Contains(stdout, `"model":"chosen-here"`) || strings.Contains(stdout, "house-model") {
		t.Errorf("the run was not on the flags' model:\n%s", stdout)
	}
}

// A spec can come on standard input, and a MESSAGE may start with a dash.
func TestASpecOnStandardInputRuns(t *testing.T) {
	noKeys(t)
	code, stdout, stderr := fiberCLI(t, docsBot, "run", "-offline", "-", "-5 degrees: is that why the deploy failed?")
	if code != exitCompleted || !strings.Contains(stdout, "[lifecycle.completed]") {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
}

// -tools lists what a run would be granted here, needing no key and no
// MESSAGE, and runs nothing: the built-in ask_person, the author tools
// every App has, a delegation tool per spawn grant, which calls wait for
// approval, and the grants this composition cannot honour, with why.
func TestToolsListsWhatTheAgentWouldBeGrantedAndRunsNothing(t *testing.T) {
	noKeys(t)
	path := writeSpec(t, `{
  "name": "docs-bot", "provider": "anthropic", "model": "house-model", "system_prompt": "Answer.",
  "tools": ["ask_person", "author_get", "jira_transition", "mcp:issues"],
  "approvals": ["author_get"],
  "access": {"spawn": ["helper"]}
}`)

	code, stdout, stderr := fiberCLI(t, "", "run", "-tools", path)
	if code != exitCompleted {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	for _, want := range []string{
		"docs-bot revision 1 would be granted 3 tools:",
		"ask_person", "tag_helper",
		"(each call waits for approval)",
		"2 grants are unavailable here",
		`"jira_transition" is not contributed by any registered connector`,
		`provider "issues"`,
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the listing does not say %q:\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout, "[lifecycle.") || stderr != "" {
		t.Errorf("listing ran something or logged:\nstdout:\n%s\nstderr:\n%s", stdout, stderr)
	}

	code, stdout, stderr = fiberCLI(t, "", "run", "-tools", "-json", path, "a message is allowed")
	if code != exitCompleted {
		t.Fatalf("exit %d\nstderr:\n%s", code, stderr)
	}
	var l listing
	if err := json.Unmarshal([]byte(stdout), &l); err != nil {
		t.Fatalf("-tools -json printed %q: %v", stdout, err)
	}
	var names, approved, missing []string
	for _, tl := range l.Tools {
		names = append(names, tl.Name)
		if tl.Approval {
			approved = append(approved, tl.Name)
		}
	}
	for _, u := range l.Unavailable {
		missing = append(missing, u.Name)
	}
	if !slices.Equal(names, []string{"ask_person", "author_get", "tag_helper"}) || !slices.Equal(approved, []string{"author_get"}) ||
		!slices.Equal(missing, []string{"jira_transition", "mcp:issues"}) || l.Agent != "docs-bot" || l.Rev != 1 {
		t.Errorf("listing %+v", l)
	}
}

// A run that fails exits 1 once the ledger says it failed for good, after
// every attempt's failure is printed; here the spec's provider has no key.
func TestARunThatFailsExitsOne(t *testing.T) {
	noKeys(t)
	code, stdout, stderr := fiberCLI(t, "", "run", "-provider", "anthropic", writeSpec(t, docsBot), "how?")
	if code != exitFailed {
		t.Fatalf("exit %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitFailed, stdout, stderr)
	}
	if !strings.Contains(stdout, "[lifecycle.failed] attempt 1:") || !strings.Contains(stdout, agentrt.EnvAnthropicKey) {
		t.Errorf("the failure was not printed:\n%s", stdout)
	}
	if strings.Contains(stdout, "[lifecycle.completed]") || !strings.Contains(stderr, "failed") {
		t.Errorf("stdout:\n%s\nstderr:\n%s", stdout, stderr)
	}
}

// A run that outlives -timeout exits 1 and says so.
func TestARunOutOfTimeExitsOne(t *testing.T) {
	noKeys(t)
	code, _, stderr := fiberCLI(t, "", "run", "-offline", "-timeout", "1ns", writeSpec(t, docsBot), "how?")
	if code != exitFailed || !strings.Contains(stderr, "timed out after 1ns") {
		t.Errorf("exit %d, stderr %q; want %d and a timeout", code, stderr, exitFailed)
	}
}

// A command line or a spec that is wrong exits 2 before anything runs, and
// says what is wrong. A misspelt field is wrong, not ignored.
func TestBadInputExitsTwoAndSaysWhy(t *testing.T) {
	noKeys(t)
	good := writeSpec(t, docsBot)
	for _, tc := range []struct {
		name  string
		stdin string
		args  []string
		says  string
	}{
		{"no command", "", nil, "Usage: fiber run"},
		{"an unknown command", "", []string{"serve"}, `unknown command "serve"`},
		{"no spec", "", []string{"run"}, "no SPEC"},
		{"no message", "", []string{"run", good}, "no MESSAGE"},
		{"an empty message", "", []string{"run", good, "  "}, "MESSAGE is empty"},
		{"a flag after the spec", "", []string{"run", good, "hi", "-json"}, "flags go before it"},
		{"a flag with a value after the spec", "", []string{"run", good, "--timeout=1s"}, "flags go before it"},
		{"too many arguments", "", []string{"run", good, "hi", "there"}, "too many arguments"},
		{"an unknown flag", "", []string{"run", "-verbose", good, "hi"}, "-verbose"},
		{"offline and a provider", "", []string{"run", "-offline", "-provider", "openai", good, "hi"}, "give one"},
		{"a negative timeout", "", []string{"run", "-timeout", "-1s", good, "hi"}, "negative"},
		{"a missing file", "", []string{"run", filepath.Join(t.TempDir(), "nope.json"), "hi"}, "no such file"},
		{"empty input", "", []string{"run", "-", "hi"}, "empty"},
		{"not JSON", "name: docs-bot", []string{"run", "-", "hi"}, "not an agent spec"},
		{"a misspelt field", `{"name":"x","provider":"offline","model":"m","system-prompt":"hi"}`, []string{"run", "-", "hi"}, `unknown field "system-prompt"`},
		{"two specs", `{"name":"x"} {"name":"y"}`, []string{"run", "-", "hi"}, "more than one JSON value"},
		{"no system prompt", `{"name":"x","provider":"offline","model":"m"}`, []string{"run", "-", "hi"}, "empty system prompt"},
		{"an unknown provider", `{"name":"x","provider":"house","model":"m","system_prompt":"hi"}`, []string{"run", "-", "hi"}, `names provider "house"`},
		{"an approval for a tool it is not granted", `{"name":"x","provider":"offline","model":"m","system_prompt":"hi","approvals":["deploy"]}`, []string{"run", "-tools", "-"}, "deploy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := fiberCLI(t, tc.stdin, tc.args...)
			if code != exitBadInput {
				t.Errorf("exit %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitBadInput, stdout, stderr)
			}
			if !strings.Contains(stderr, tc.says) || stdout != "" {
				t.Errorf("stderr %q does not say %q, or stdout is not empty: %q", stderr, tc.says, stdout)
			}
		})
	}
}

func TestHelpIsExitZero(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"-h"}, {"run", "-h"}} {
		if code, _, _ := fiberCLI(t, "", args...); code != exitCompleted {
			t.Errorf("%v: exit %d, want %d", args, code, exitCompleted)
		}
	}
}

// The text form puts every event on a line of its own under its kind, keeps
// the answer's fragments together, and shows a terminal event's answer when
// none of it streamed, as a replayed attempt's does not.
func TestTextShowsEachEventUnderItsKind(t *testing.T) {
	ev := func(k envelope.Kind, body any) envelope.Event {
		b, err := payload.Encode(body)
		if err != nil {
			t.Fatal(err)
		}
		return envelope.Event{Kind: k, Agent: "docs-bot", Rev: 2, RunID: "r1", Payload: b}
	}
	for _, tc := range []struct {
		name   string
		events []envelope.Event
		want   string
	}{
		{"fragments stay on one line until another kind", []envelope.Event{
			ev(envelope.KindText, payload.Text{Text: "Merging "}),
			ev(envelope.KindText, payload.Text{Text: "releases."}),
			ev(envelope.KindToolCall, payload.ToolCall{Name: "lookup", Phase: payload.PhaseRequested}),
			ev(envelope.KindToolCall, payload.ToolCall{Name: "lookup", Phase: payload.PhaseExecuting, Arguments: json.RawMessage(`{ "q": "x" }`)}),
			ev(envelope.KindToolDone, payload.ToolDone{Name: "lookup", Result: "two\nrows"}),
			ev(envelope.KindToolDone, payload.ToolDone{Name: "lookup", Error: "refused"}),
			ev(envelope.KindThinking, payload.Thinking{Text: "hmm"}),
			ev(envelope.KindText, payload.Text{Text: "Done.\n"}),
			ev(envelope.KindCompleted, payload.Lifecycle{Attempt: 1, Text: "Merging releases.Done."}),
		}, "Merging releases.\n" +
			"[delta.tool.call] lookup (requested)\n" +
			"[delta.tool.call] lookup {\"q\":\"x\"}\n" +
			"[delta.tool.done] lookup: two rows\n" +
			"[delta.tool.done] lookup failed: refused\n" +
			"[delta.thinking] hmm\n" +
			"Done.\n" +
			"[lifecycle.completed] attempt 1\n"},
		{"an answer that did not stream is shown with its ending", []envelope.Event{
			ev(envelope.KindAccepted, payload.Lifecycle{Rev: 2}),
			ev(envelope.KindResumed, payload.Lifecycle{Attempt: 2}),
			ev(envelope.KindCompleted, payload.Lifecycle{Attempt: 2, Text: "From the ledger."}),
		}, "[lifecycle.accepted] docs-bot revision 2, run r1\n" +
			"[lifecycle.resumed] attempt 2\n" +
			"[lifecycle.completed] attempt 2\n" +
			"From the ledger.\n"},
		{"failures, questions and citations", []envelope.Event{
			ev(envelope.KindFailed, payload.Lifecycle{Attempt: 1, Error: "boom"}),
			ev(envelope.KindFailed, payload.Lifecycle{Attempt: 2, Error: "boom", Terminal: true}),
			ev(envelope.KindParked, payload.Lifecycle{Question: &payload.Question{Kind: payload.QuestionChoice, Prompt: "Which?", Options: []string{"a", "b"}}}),
			ev(envelope.KindCitation, payload.Citation{Label: "[1]", Source: payload.Provenance{SourceURI: "https://docs.example/x"}}),
			{Kind: "custom.kind", Payload: []byte(`{"a": 1}`)},
		}, "[lifecycle.failed] attempt 1: boom; the run may be attempted again\n" +
			"[lifecycle.failed] attempt 2: boom; the run will not be attempted again\n" +
			"[lifecycle.parked] choice: Which? [a, b]\n" +
			"[delta.citation] [1] https://docs.example/x\n" +
			"[custom.kind] {\"a\":1}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			r := &textRenderer{w: &b}
			for _, e := range tc.events {
				r.event(e)
			}
			r.close()
			if b.String() != tc.want {
				t.Errorf("got\n%s\nwant\n%s", b.String(), tc.want)
			}
		})
	}
}
