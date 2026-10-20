package agentrt_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/envelope"
)

func decodeCall(t *testing.T, c agentrt.Chunk) payload.ToolCall {
	t.Helper()
	if c.Kind != envelope.KindToolCall {
		t.Fatalf("kind is %s, want %s", c.Kind, envelope.KindToolCall)
	}
	var body payload.ToolCall
	if err := json.Unmarshal(c.Payload, &body); err != nil {
		t.Fatalf("decode tool call: %v", err)
	}
	return body
}

func decodeDone(t *testing.T, c agentrt.Chunk) payload.ToolDone {
	t.Helper()
	if c.Kind != envelope.KindToolDone {
		t.Fatalf("kind is %s, want %s", c.Kind, envelope.KindToolDone)
	}
	var body payload.ToolDone
	if err := json.Unmarshal(c.Payload, &body); err != nil {
		t.Fatalf("decode tool done: %v", err)
	}
	return body
}

// A subscriber that sees a tool run must be able to see what it was run with,
// whether the model sent the arguments whole or streamed them in fragments.
func TestToolEventsCarryTheCallsArguments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		model []saigetypes.Delta
	}{
		{
			name: "arguments sent whole",
			model: []saigetypes.Delta{
				saigetypes.PartStart{Index: 0, Kind: saigetypes.KindToolCall, ID: "c1", Name: "grep"},
				saigetypes.PartEnd{Index: 0, Part: saigetypes.ToolCallPart{ID: "c1", Name: "grep", Arguments: map[string]any{"path": ".", "pattern": "TODO"}}},
			},
		},
		{
			name: "arguments streamed in fragments",
			model: []saigetypes.Delta{
				saigetypes.PartStart{Index: 0, Kind: saigetypes.KindToolCall, ID: "c1", Name: "grep"},
				saigetypes.PartDelta{Index: 0, Args: `{"pattern": "TODO",`},
				saigetypes.PartDelta{Index: 0, Args: ` "path": "."}`},
				saigetypes.PartEnd{Index: 0},
			},
		},
	}
	const want = `{"path":".","pattern":"TODO"}`
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tr := agentrt.NewTranslator()
			for _, d := range tc.model {
				if c, ok := tr.Delta(d); ok {
					if body := decodeCall(t, c); body.Arguments != nil {
						t.Errorf("the requested call carries %s; its arguments do not exist yet", body.Arguments)
					}
				}
			}
			c, ok := tr.Delta(saigetypes.ToolExecStartDelta{ToolCallID: "c1", Name: "grep"})
			if !ok {
				t.Fatal("executing produced no event")
			}
			if got := string(decodeCall(t, c).Arguments); got != want {
				t.Errorf("executing call carries %s, want %s", got, want)
			}
			c, ok = tr.Delta(saigetypes.ToolExecEndDelta{ToolCallID: "c1", Name: "grep", Result: "3 matches"})
			if !ok {
				t.Fatal("done produced no event")
			}
			if got := string(decodeDone(t, c).Arguments); got != want {
				t.Errorf("done carries %s, want %s", got, want)
			}
		})
	}
}

// What a tool ran with is what its done event reports, even when that is not
// what the model asked for: an approval can edit arguments.
func TestToolDoneCarriesTheArgumentsTheToolRanWith(t *testing.T) {
	t.Parallel()

	tr := agentrt.NewTranslator()
	tr.Delta(saigetypes.PartStart{Index: 0, Kind: saigetypes.KindToolCall, ID: "c1", Name: "deploy"})
	tr.Delta(saigetypes.PartEnd{Index: 0, Part: saigetypes.ToolCallPart{ID: "c1", Name: "deploy", Arguments: map[string]any{"env": "prod"}}})
	tr.Delta(saigetypes.ToolExecStartDelta{ToolCallID: "c1", Name: "deploy"})
	tr.Ran("c1", "deploy", map[string]any{"env": "staging"})

	c, _ := tr.Delta(saigetypes.ToolExecEndDelta{ToolCallID: "c1", Name: "deploy", Result: "ok"})
	if got := string(decodeDone(t, c).Arguments); got != `{"env":"staging"}` {
		t.Errorf("done carries %s, want the edited arguments", got)
	}
}

// Arguments over the cap are left out whole, never cut into something that is
// not JSON, and the event says how large they were.
func TestOversizedToolArgumentsAreLeftOutAndSized(t *testing.T) {
	t.Parallel()

	big := strings.Repeat("x", payload.MaxToolArguments)
	tr := agentrt.NewTranslator()
	tr.Delta(saigetypes.PartStart{Index: 0, Kind: saigetypes.KindToolCall, ID: "c1", Name: "write"})
	tr.Delta(saigetypes.PartEnd{Index: 0, Part: saigetypes.ToolCallPart{ID: "c1", Name: "write", Arguments: map[string]any{"body": big}}})

	wantBytes := len(`{"body":""}`) + len(big)
	c, _ := tr.Delta(saigetypes.ToolExecStartDelta{ToolCallID: "c1", Name: "write"})
	call := decodeCall(t, c)
	if call.Arguments != nil || call.ArgumentsBytes != wantBytes {
		t.Errorf("executing call carries %d bytes of arguments and reports %d, want none and %d", len(call.Arguments), call.ArgumentsBytes, wantBytes)
	}
	c, _ = tr.Delta(saigetypes.ToolExecEndDelta{ToolCallID: "c1", Name: "write"})
	done := decodeDone(t, c)
	if done.Arguments != nil || done.ArgumentsBytes != wantBytes {
		t.Errorf("done carries %d bytes of arguments and reports %d, want none and %d", len(done.Arguments), done.ArgumentsBytes, wantBytes)
	}
}

// A deployment's scrubber decides what reaches the bus, and only that: the
// tool still runs with the arguments the model sent.
func TestToolArgumentsAreScrubbedBeforeTheyArePublished(t *testing.T) {
	t.Parallel()

	tool := &agenttest.MockTool{
		Def:    saigetypes.ToolDef{Name: "login", Parameters: saigetypes.ParameterSchema{Type: "object"}},
		Result: "logged in",
	}
	rev := offlineRevision(1, "")
	rev.Spec.Tools = []string{"login"}
	r, err := agentrt.New(rev,
		agentrt.WithTools(agentrt.StaticTools(tool)),
		agentrt.WithToolArguments(func(name string, args json.RawMessage) json.RawMessage {
			if name != "login" {
				return args
			}
			var m map[string]any
			if err := json.Unmarshal(args, &m); err != nil {
				return nil
			}
			m["password"] = "[redacted]"
			out, _ := json.Marshal(m)
			return out
		}),
		agentrt.WithScript(
			agenttest.ToolCallResponse("c1", "login", map[string]any{"user": "ada", "password": "hunter2"}),
			agenttest.TextResponse("done"),
		))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got, _ := collect(t, r, agentrt.Turn{Text: "log in"})

	const want = `{"password":"[redacted]","user":"ada"}`
	var seen int
	for _, c := range got {
		var args json.RawMessage
		switch c.Kind {
		case envelope.KindToolCall:
			body := decodeCall(t, c)
			if body.Phase != payload.PhaseExecuting {
				continue
			}
			args = body.Arguments
		case envelope.KindToolDone:
			args = decodeDone(t, c).Arguments
		default:
			continue
		}
		seen++
		if string(args) != want {
			t.Errorf("%s carries %s, want %s", c.Kind, args, want)
		}
		if strings.Contains(string(c.Payload), "hunter2") {
			t.Errorf("%s published the secret: %s", c.Kind, c.Payload)
		}
	}
	if seen != 2 {
		t.Errorf("saw %d tool events with arguments, want an executing call and a done", seen)
	}
	if len(tool.Calls) != 1 || tool.Calls[0]["password"] != "hunter2" {
		t.Errorf("the tool ran with %v, want the arguments the model sent", tool.Calls)
	}
}

// A Translator built as a literal, as callers did before it held any state,
// still translates.
func TestAZeroTranslatorStillTranslates(t *testing.T) {
	t.Parallel()

	var tr agentrt.Translator
	tr.Delta(saigetypes.PartStart{Index: 0, Kind: saigetypes.KindToolCall, ID: "c1", Name: "grep"})
	tr.Delta(saigetypes.PartEnd{Index: 0, Part: saigetypes.ToolCallPart{ID: "c1", Name: "grep", Arguments: map[string]any{"path": "."}}})
	c, ok := tr.Delta(saigetypes.ToolExecEndDelta{ToolCallID: "c1", Name: "grep"})
	if !ok || string(decodeDone(t, c).Arguments) != `{"path":"."}` {
		t.Fatalf("zero translator produced %s, %v", c.Payload, ok)
	}
}

// A tool can start running, with arguments an approval edited, before the
// delta saying it began executing is translated. The executing event still
// reports what the model asked for, and the done event what the tool ran with.
func TestTheExecutingEventKeepsTheModelsArgumentsWhenTheToolRanFirst(t *testing.T) {
	t.Parallel()

	tr := agentrt.NewTranslator()
	tr.Delta(saigetypes.PartStart{Index: 0, Kind: saigetypes.KindToolCall, ID: "c1", Name: "deploy"})
	tr.Delta(saigetypes.PartEnd{Index: 0, Part: saigetypes.ToolCallPart{ID: "c1", Name: "deploy", Arguments: map[string]any{"env": "prod"}}})
	tr.Ran("c1", "deploy", map[string]any{"env": "staging"})

	c, _ := tr.Delta(saigetypes.ToolExecStartDelta{ToolCallID: "c1", Name: "deploy"})
	if got := string(decodeCall(t, c).Arguments); got != `{"env":"prod"}` {
		t.Errorf("executing call carries %s, want what the model asked for", got)
	}
	c, _ = tr.Delta(saigetypes.ToolExecEndDelta{ToolCallID: "c1", Name: "deploy"})
	if got := string(decodeDone(t, c).Arguments); got != `{"env":"staging"}` {
		t.Errorf("done carries %s, want what the tool ran with", got)
	}
}

// Call ids are unique only within one agent's stream. A sub-agent's call that
// reuses its parent's id gets its own arguments, and finishing it leaves the
// parent's in place.
func TestANestedCallsArgumentsAreKeptApartFromItsParents(t *testing.T) {
	t.Parallel()

	nested := func(d saigetypes.Delta) saigetypes.Delta {
		return saigetypes.ToolExecDelta{ToolCallID: "c1", Inner: d}
	}
	tr := agentrt.NewTranslator()
	tr.Delta(saigetypes.PartStart{Index: 0, Kind: saigetypes.KindToolCall, ID: "c1", Name: "delegate"})
	tr.Delta(saigetypes.PartEnd{Index: 0, Part: saigetypes.ToolCallPart{ID: "c1", Name: "delegate", Arguments: map[string]any{"who": "parent"}}})
	tr.Delta(saigetypes.ToolExecStartDelta{ToolCallID: "c1", Name: "delegate"})

	tr.Delta(nested(saigetypes.PartStart{Index: 0, Kind: saigetypes.KindToolCall, ID: "c1", Name: "grep"}))
	tr.Delta(nested(saigetypes.PartEnd{Index: 0, Part: saigetypes.ToolCallPart{ID: "c1", Name: "grep", Arguments: map[string]any{"who": "child"}}}))
	c, _ := tr.Delta(nested(saigetypes.ToolExecStartDelta{ToolCallID: "c1", Name: "grep"}))
	if got := string(decodeCall(t, c).Arguments); got != `{"who":"child"}` {
		t.Errorf("the child's executing call carries %s, want its own", got)
	}
	tr.Ran("c1", "grep", map[string]any{"who": "child ran"}, "c1")
	c, _ = tr.Delta(nested(saigetypes.ToolExecEndDelta{ToolCallID: "c1", Name: "grep"}))
	if got := string(decodeDone(t, c).Arguments); got != `{"who":"child ran"}` {
		t.Errorf("the child's done carries %s, want what it ran with", got)
	}

	c, _ = tr.Delta(saigetypes.ToolExecEndDelta{ToolCallID: "c1", Name: "delegate"})
	if got := string(decodeDone(t, c).Arguments); got != `{"who":"parent"}` {
		t.Errorf("the parent's done carries %s, want the parent's own", got)
	}
}
