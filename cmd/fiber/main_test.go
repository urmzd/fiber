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

	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/turn/native"
)

// docsBot is a definition as a deployment would write it: a real provider
// and a fallback on another.
const docsBot = `---
apiVersion: saige/v1
name: docs-bot
version: 1.0.0
description: answers questions about how we ship
model:
  use: anthropic/house-model
  fallback:
    - openai/other-model
---
You answer questions about the deploy pipeline.
`

// agentFile is a minimal definition with frontmatter lines of the test's.
func agentFile(front, prompt string) string {
	return "---\napiVersion: saige/v1\nname: docs-bot\nversion: 1.0.0\n" + front + "---\n" + prompt + "\n"
}

// noKeys makes sure no vendor key is in reach, so a test that passes did so
// without one.
func noKeys(t *testing.T) {
	t.Helper()
	for _, k := range []string{native.EnvAnthropicKey, native.EnvOpenAIKey, native.EnvOpenAIBaseURL} {
		t.Setenv(k, "")
	}
}

func writeSpec(t *testing.T, body string) string {
	t.Helper()
	return writeFile(t, "docs-bot.agent.md", body)
}

func writeFile(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
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
		"[run.accepted.v1] docs-bot version 1.0.0, run ",
		"[run.started.v1] attempt 1",
		"docs-bot is running with the offline provider, so no model was called.",
		"[run.usage.v1] house-model:",
		"[run.completed.v1] attempt 1",
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
	var types []string
	var said strings.Builder
	var last uint64
	var completed event.Completed
	runs := map[string]bool{}
	sc := bufio.NewScanner(strings.NewReader(stdout))
	for sc.Scan() {
		var e event.Envelope
		if err := json.Unmarshal([]byte(sc.Text()), &e); err != nil {
			t.Fatalf("line %q is not one event: %v", sc.Text(), err)
		}
		if e.Seq <= last {
			t.Errorf("seq %d after %d", e.Seq, last)
		}
		last = e.Seq
		types = append(types, e.Type)
		runs[e.Run] = true
		if e.Agent != "docs-bot" || e.Version != "1.0.0" || e.Run == "" {
			t.Errorf("event %+v is not docs-bot version 1.0.0 of a run", e)
		}
		switch b := e.Data.(type) {
		case event.Text:
			said.WriteString(b.Text)
		case event.Completed:
			completed = b
		}
	}
	want := []string{event.TypeAccepted, event.TypeStarted, event.TypeText, event.TypeUsageCall, event.TypeCompleted}
	if !slices.Equal(types, want) {
		t.Errorf("types %v, want %v", types, want)
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
	code, stdout, stderr := fiberCLI(t, "", "run", "-json", "-provider", "offline", "-model", "chosen-here", writeSpec(t, agentFile("model:\n  use: anthropic/house-model\n", "Answer.")), "how?")
	if code != exitCompleted {
		t.Fatalf("exit %d\nstderr:\n%s", code, stderr)
	}
	if !strings.Contains(stdout, `"type":"run.usage.v1"`) || !strings.Contains(stdout, `"model":"chosen-here"`) || strings.Contains(stdout, "house-model") {
		t.Errorf("the run was not on the flags' model:\n%s", stdout)
	}
}

// A spec can come on standard input, and a MESSAGE may start with a dash.
func TestASpecOnStandardInputRuns(t *testing.T) {
	noKeys(t)
	code, stdout, stderr := fiberCLI(t, docsBot, "run", "-offline", "-", "-5 degrees: is that why the deploy failed?")
	if code != exitCompleted || !strings.Contains(stdout, "[run.completed.v1]") {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
}

// -tools lists what a run would be granted here, needing no key and no
// MESSAGE, and runs nothing: the built-in ask_person, the author tools
// every App has, which calls wait for approval, and the grants this
// composition cannot honour, with why.
func TestToolsListsWhatTheAgentWouldBeGrantedAndRunsNothing(t *testing.T) {
	noKeys(t)
	path := writeSpec(t, agentFile(`model:
  use: anthropic/house-model
tools:
  registry: [ask_person, author_get, jira_transition]
  mcp:
    - server: issues
approval:
  ask: [author_get]
`, "Answer."))

	code, stdout, stderr := fiberCLI(t, "", "run", "-tools", path)
	if code != exitCompleted {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	for _, want := range []string{
		"docs-bot 1.0.0 would be granted 2 tools:",
		"ask_person", "author_get",
		"(each call waits for approval)",
		"2 grants are unavailable here",
		`"jira_transition" is not contributed by any registered connector`,
		`provider "issues"`,
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the listing does not say %q:\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout, "[run.") || stderr != "" {
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
	if !slices.Equal(names, []string{"ask_person", "author_get"}) || !slices.Equal(approved, []string{"author_get"}) ||
		!slices.Equal(missing, []string{"jira_transition", "mcp:issues"}) || l.Agent != "docs-bot" || l.Version != "1.0.0" {
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
	if !strings.Contains(stdout, "[run.failed.v1] attempt 1:") || !strings.Contains(stdout, native.EnvAnthropicKey) {
		t.Errorf("the failure was not printed:\n%s", stdout)
	}
	if strings.Contains(stdout, "[run.completed.v1]") || !strings.Contains(stderr, "failed") {
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

// A policy file is read with the definition, strictly.
func TestAPolicyFileIsRead(t *testing.T) {
	noKeys(t)
	policy := writeFile(t, "policy.json", `{"tagged_only": true, "fallback_on": ["timeout"]}`)
	code, stdout, stderr := fiberCLI(t, "", "run", "-offline", "-policy", policy, writeSpec(t, docsBot), "how?")
	if code != exitCompleted || !strings.Contains(stdout, "[run.completed.v1]") {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
}

// A command line or a definition that is wrong exits 2 before anything runs, and
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
		{"no definition", "", []string{"run"}, "no DEFINITION"},
		{"no message", "", []string{"run", good}, "no MESSAGE"},
		{"an empty message", "", []string{"run", good, "  "}, "MESSAGE is empty"},
		{"a flag after the spec", "", []string{"run", good, "hi", "-json"}, "flags go before it"},
		{"a flag with a value after the spec", "", []string{"run", good, "--timeout=1s"}, "flags go before it"},
		{"too many arguments", "", []string{"run", good, "hi", "there"}, "too many arguments"},
		{"an unknown flag", "", []string{"run", "-verbose", good, "hi"}, "-verbose"},
		{"offline and a provider", "", []string{"run", "-offline", "-provider", "openai", good, "hi"}, "give one"},
		{"a negative timeout", "", []string{"run", "-timeout", "-1s", good, "hi"}, "negative"},
		{"a missing file", "", []string{"run", filepath.Join(t.TempDir(), "nope.agent.md"), "hi"}, "no such file"},
		{"empty input", "", []string{"run", "-", "hi"}, "empty"},
		{"no frontmatter", "name: docs-bot", []string{"run", "-", "hi"}, "frontmatter"},
		{"no prompt", agentFile("model:\n  use: offline/m\n", ""), []string{"run", "-", "hi"}, "prompt is empty"},
		{"no model", agentFile("", "hi"), []string{"run", "-", "hi"}, "a model is required"},
		{"an unknown provider", agentFile("model:\n  use: house/m\n", "hi"), []string{"run", "-", "hi"}, `names provider "house"`},
		{"an approval for a tool it is not granted", agentFile("model:\n  use: offline/m\napproval:\n  ask: [deploy]\n", "hi"), []string{"run", "-tools", "-"}, "deploy"},
		{"a subagent it cannot resolve", agentFile("model:\n  use: offline/m\nsubagents:\n  - ref: helper\n", "hi"), []string{"run", "-tools", "-"}, "helper"},
		{"a misspelt policy field", "", []string{"run", "-policy", writeFile(t, "p.json", `{"tagged-only": true}`), good, "hi"}, `unknown field "tagged-only"`},
		{"two policies", "", []string{"run", "-policy", writeFile(t, "p.json", `{} {}`), good, "hi"}, "more than one JSON value"},
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
	ev := func(body event.Body) event.Envelope {
		return event.Envelope{Type: body.Type(), Agent: "docs-bot", Version: "2.0.0", Run: "r1", Data: body}
	}
	for _, tc := range []struct {
		name   string
		events []event.Envelope
		want   string
	}{
		{"fragments stay on one line until another kind", []event.Envelope{
			ev(event.Text{Text: "Merging "}),
			ev(event.Text{Text: "releases."}),
			ev(event.ToolCall{Name: "lookup", Phase: event.PhaseRequested}),
			ev(event.ToolCall{Name: "lookup", Phase: event.PhaseExecuting, Arguments: json.RawMessage(`{ "q": "x" }`)}),
			ev(event.ToolDone{Name: "lookup", Result: "two\nrows"}),
			ev(event.ToolDone{Name: "lookup", Error: "refused"}),
			ev(event.Thinking{Text: "hmm"}),
			ev(event.Text{Text: "Done.\n"}),
			ev(event.Completed{Attempt: 1, Text: "Merging releases.Done."}),
		}, "Merging releases.\n" +
			"[run.delta.tool.call.v1] lookup (requested)\n" +
			"[run.delta.tool.call.v1] lookup {\"q\":\"x\"}\n" +
			"[run.delta.tool.done.v1] lookup: two rows\n" +
			"[run.delta.tool.done.v1] lookup failed: refused\n" +
			"[run.delta.thinking.v1] hmm\n" +
			"Done.\n" +
			"[run.completed.v1] attempt 1\n"},
		{"an answer that did not stream is shown with its ending", []event.Envelope{
			ev(event.Accepted{}),
			ev(event.Resumed{Attempt: 2}),
			ev(event.Completed{Attempt: 2, Text: "From the ledger."}),
		}, "[run.accepted.v1] docs-bot version 2.0.0, run r1\n" +
			"[run.resumed.v1] attempt 2\n" +
			"[run.completed.v1] attempt 2\n" +
			"From the ledger.\n"},
		{"failures, questions and citations", []event.Envelope{
			ev(event.Failed{Attempt: 1, Error: "boom"}),
			ev(event.Failed{Attempt: 2, Error: "boom", Terminal: true}),
			ev(event.Parked{Question: &event.Question{Kind: event.QuestionChoice, Prompt: "Which?", Options: []string{"a", "b"}}}),
			ev(event.Citation{Label: "[1]", Source: event.Provenance{SourceURI: "https://docs.example/x"}}),
			{Type: "custom.thing.v1", Data: event.Unknown{TypeName: "custom.thing.v1", Raw: json.RawMessage(`{"a": 1}`)}},
		}, "[run.failed.v1] attempt 1: boom; the run may be attempted again\n" +
			"[run.failed.v1] attempt 2: boom; the run will not be attempted again\n" +
			"[run.parked.v1] choice: Which? [a, b]\n" +
			"[run.delta.citation.v1] [1] https://docs.example/x\n" +
			"[custom.thing.v1] {\"a\":1}\n"},
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
