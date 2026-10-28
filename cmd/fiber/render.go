package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/urmzd/fiber"
	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/envelope"
	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

// renderer prints a run's events as they arrive.
type renderer interface {
	event(e envelope.Event)
	// close ends the output, once no more events will be printed.
	close()
}

// textRenderer prints events for a person. The answer and the reasoning
// stream inline as their fragments arrive; every other event is one line,
// "[kind] what happened", under the kind's own name, which is what a route's
// kind filter selects.
type textRenderer struct {
	w io.Writer
	// inline is the delta kind whose fragments the current line holds, if
	// any, and mid is whether that line has not been ended.
	inline envelope.Kind
	mid    bool
	// said is whether any of the answer was streamed, so a run whose answer
	// arrives only on its terminal event (a replayed attempt streams none)
	// still shows it.
	said bool
}

// summaryLen bounds what one line says of a tool's arguments or result. The
// full text is in the -json output.
const summaryLen = 160

func (t *textRenderer) event(e envelope.Event) {
	switch e.Kind {
	case envelope.KindText, envelope.KindThinking:
		frag, err := payload.Decode[payload.Text](e)
		if err != nil {
			t.line(e.Kind, err.Error())
			return
		}
		if t.inline != e.Kind {
			t.endLine()
			t.inline = e.Kind
			if e.Kind == envelope.KindThinking {
				fmt.Fprintf(t.w, "[%s] ", e.Kind)
				t.mid = true
			}
		}
		if frag.Text == "" {
			return
		}
		fmt.Fprint(t.w, frag.Text)
		t.mid = !strings.HasSuffix(frag.Text, "\n")
		if e.Kind == envelope.KindText {
			t.said = true
		}
		return
	}
	t.endLine()
	body, known, err := payload.Of(e)
	if err != nil {
		t.line(e.Kind, err.Error())
		return
	}
	if !known {
		t.line(e.Kind, compact(e.Payload))
		return
	}
	switch b := body.(type) {
	case payload.Lifecycle:
		t.lifecycle(e, b)
	case payload.Stopped:
		t.line(e.Kind, join(fmt.Sprintf("attempt %d", b.Attempt), b.Reason))
		t.answer(b.Text)
	case payload.ToolCall:
		if b.Phase == payload.PhaseRequested {
			t.line(e.Kind, b.Name+" (requested)")
			return
		}
		t.line(e.Kind, join(b.Name, clip(compact(b.Arguments))))
	case payload.ToolDone:
		if b.Error != "" {
			t.line(e.Kind, b.Name+" failed: "+clip(b.Error))
			return
		}
		t.line(e.Kind, b.Name+": "+clip(b.Result))
	case payload.Citation:
		where := b.Source.SourceURI
		if where == "" {
			where = join(b.Source.DocumentTitle, b.Source.SectionHeading)
		}
		t.line(e.Kind, join(b.Label, where))
	case payload.Action:
		if b.Error != "" {
			t.line(e.Kind, join(b.Name, b.Target)+" failed: "+clip(b.Error))
			return
		}
		t.line(e.Kind, join(join(b.Name, b.Target), clip(b.Summary)))
	case payload.UsageCall:
		t.line(e.Kind, fmt.Sprintf("%s: %d tokens in (%d cached), %d out",
			orNone(b.Model), b.PromptTokens, b.CachedPromptTokens, b.CompletionTokens))
	default:
		t.line(e.Kind, clip(compact(e.Payload)))
	}
}

func (t *textRenderer) lifecycle(e envelope.Event, b payload.Lifecycle) {
	switch e.Kind {
	case envelope.KindAccepted:
		t.line(e.Kind, fmt.Sprintf("%s revision %d, run %s", e.Agent, e.Rev, e.RunID))
	case envelope.KindFailed:
		then := "the run may be attempted again"
		if b.Terminal {
			then = "the run will not be attempted again"
		}
		t.line(e.Kind, fmt.Sprintf("attempt %d: %s; %s", b.Attempt, b.Error, then))
	case envelope.KindParked:
		what := b.Reason
		if q := b.Question; q != nil {
			what = q.Kind + ": " + q.Prompt
			if q.Tool != nil {
				what = join(what, "("+q.Tool.Name+" "+clip(compact(q.Tool.Arguments))+")")
			}
			if len(q.Options) > 0 {
				what = join(what, "["+strings.Join(q.Options, ", ")+"]")
			}
		}
		t.line(e.Kind, what)
	case envelope.KindCompleted:
		if u := b.Usage; u != nil && !u.Total.IsZero() {
			t.line(e.Kind, fmt.Sprintf("attempt %d, %d model calls, %d tokens in, %d out",
				b.Attempt, u.Total.ModelCalls, u.Total.PromptTokens, u.Total.CompletionTokens))
		} else {
			t.line(e.Kind, fmt.Sprintf("attempt %d", b.Attempt))
		}
		t.answer(b.Text)
	default:
		t.line(e.Kind, fmt.Sprintf("attempt %d", b.Attempt))
	}
}

// answer prints a terminal event's answer when none of it was streamed.
func (t *textRenderer) answer(text string) {
	if t.said || text == "" {
		return
	}
	fmt.Fprintln(t.w, strings.TrimRight(text, "\n"))
	t.said = true
}

func (t *textRenderer) line(k envelope.Kind, what string) {
	if what == "" {
		fmt.Fprintf(t.w, "[%s]\n", k)
		return
	}
	fmt.Fprintf(t.w, "[%s] %s\n", k, what)
}

func (t *textRenderer) endLine() {
	if t.mid {
		fmt.Fprintln(t.w)
	}
	t.inline, t.mid = "", false
}

func (t *textRenderer) close() { t.endLine() }

// jsonRenderer prints one JSON object per event, per line. It is the event
// as the bus carries it, with its payload as the JSON it is rather than as
// encoded bytes.
type jsonRenderer struct{ enc *json.Encoder }

type jsonEvent struct {
	Seq     uint64          `json:"seq"`
	Kind    envelope.Kind   `json:"kind"`
	Topic   string          `json:"topic"`
	RunID   string          `json:"run_id"`
	Tenant  string          `json:"tenant"`
	Agent   string          `json:"agent"`
	Rev     int             `json:"rev"`
	Origin  string          `json:"origin"`
	At      time.Time       `json:"at"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func (j *jsonRenderer) event(e envelope.Event) {
	body := json.RawMessage(e.Payload)
	if len(body) > 0 && !json.Valid(body) {
		// Every built-in kind's body is JSON. One that is not is carried
		// as a string rather than dropped.
		body, _ = json.Marshal(string(e.Payload))
	}
	_ = j.enc.Encode(jsonEvent{
		Seq: e.Seq, Kind: e.Kind, Topic: e.Topic.String(), RunID: e.RunID, Tenant: e.Tenant,
		Agent: e.Agent, Rev: e.Rev, Origin: e.Origin, At: e.At, Payload: body,
	})
}

func (j *jsonRenderer) close() {}

// listedTool is one tool in a listing: what the model is shown, and whether
// its calls wait for a person's approval.
type listedTool struct {
	Name        string                     `json:"name"`
	Description string                     `json:"description,omitempty"`
	Parameters  saigetypes.ParameterSchema `json:"parameters"`
	Approval    bool                       `json:"approval,omitempty"`
}

type listing struct {
	Agent       string                    `json:"agent"`
	Rev         int                       `json:"rev"`
	Tools       []listedTool              `json:"tools"`
	Unavailable []agentrt.UnavailableTool `json:"unavailable,omitempty"`
}

// listTools prints what a run of tag would be granted, as this composition
// resolves it, and runs nothing.
func listTools(ctx context.Context, app *fiber.App, tag envelope.Tag, rev agentspec.Revision, asJSON bool, stdout, stderr io.Writer) int {
	granted, err := app.ToolsFor(ctx, tag)
	if err != nil {
		fmt.Fprintf(stderr, "fiber run: %v\n", err)
		return exitFailed
	}
	l := listing{Agent: rev.Spec.Name, Rev: rev.Rev, Tools: []listedTool{}, Unavailable: granted.Unavailable}
	for _, d := range granted.Tools {
		l.Tools = append(l.Tools, listedTool{
			Name: d.Name, Description: d.Description, Parameters: d.Parameters,
			Approval: slices.Contains(rev.Spec.Approvals, d.Name),
		})
	}
	if asJSON {
		if err := json.NewEncoder(stdout).Encode(l); err != nil {
			fmt.Fprintf(stderr, "fiber run: %v\n", err)
			return exitFailed
		}
		return exitCompleted
	}
	fmt.Fprintf(stdout, "%s revision %d would be granted %s:\n", l.Agent, l.Rev, count(len(l.Tools), "tool", "tools"))
	width := 0
	for _, t := range l.Tools {
		width = max(width, utf8.RuneCountInString(t.Name))
	}
	for _, u := range l.Unavailable {
		width = max(width, utf8.RuneCountInString(u.Name))
	}
	for _, t := range l.Tools {
		what := firstLine(t.Description)
		if t.Approval {
			what = join(what, "(each call waits for approval)")
		}
		fmt.Fprintf(stdout, "  %-*s  %s\n", width, t.Name, what)
	}
	if len(l.Unavailable) > 0 {
		fmt.Fprintf(stdout, "%s unavailable here, and the model is told so:\n", count(len(l.Unavailable), "grant is", "grants are"))
		for _, u := range l.Unavailable {
			fmt.Fprintf(stdout, "  %-*s  %s\n", width, u.Name, u.Reason)
			if u.Note != "" {
				fmt.Fprintf(stdout, "  %-*s  the model is told: %s\n", width, "", u.Note)
			}
		}
	}
	return exitCompleted
}

func count(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", n, many)
}

// compact renders a JSON value on one line, or nothing for none.
func compact(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		return string(raw)
	}
	return b.String()
}

// clip puts s on one line and bounds it to summaryLen runes.
func clip(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) <= summaryLen {
		return s
	}
	r := []rune(s)
	return string(r[:summaryLen]) + "..."
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return clip(line)
}

// join puts two parts of a line together, leaving out an empty one.
func join(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	}
	return a + " " + b
}

func orNone(s string) string {
	if s == "" {
		return "a model"
	}
	return s
}
