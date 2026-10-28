package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/urmzd/fiber"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/turn/native"
	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

// renderer prints a run's events as they arrive.
type renderer interface {
	event(e event.Envelope)
	// close ends the output, once no more events will be printed.
	close()
}

// textRenderer prints events for a person. The answer and the reasoning
// stream inline as their fragments arrive; every other event is one line,
// "[type] what happened", under the event's own type, which is what a
// subscription's type prefix selects.
type textRenderer struct {
	w io.Writer
	// inline is the delta type whose fragments the current line holds, if
	// any, and mid is whether that line has not been ended.
	inline string
	mid    bool
	// said is whether any of the answer was streamed, so a run whose answer
	// arrives only on its terminal event (a replayed attempt streams none)
	// still shows it.
	said bool
}

// summaryLen bounds what one line says of a tool's arguments or result. The
// full text is in the -json output.
const summaryLen = 160

func (t *textRenderer) event(e event.Envelope) {
	switch b := e.Data.(type) {
	case event.Text:
		t.inlineText(e.Type, b.Text, false)
		return
	case event.Thinking:
		t.inlineText(e.Type, b.Text, true)
		return
	}
	t.endLine()
	switch b := e.Data.(type) {
	case event.Accepted:
		t.line(e.Type, fmt.Sprintf("%s version %s, run %s", e.Agent, e.Version, e.Run))
	case event.Started:
		t.line(e.Type, fmt.Sprintf("attempt %d", b.Attempt))
	case event.Resumed:
		t.line(e.Type, fmt.Sprintf("attempt %d", b.Attempt))
	case event.Failed:
		then := "the run may be attempted again"
		if b.Terminal {
			then = "the run will not be attempted again"
		}
		t.line(e.Type, fmt.Sprintf("attempt %d: %s; %s", b.Attempt, b.Error, then))
	case event.Parked:
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
		t.line(e.Type, what)
	case event.Completed:
		if u := b.Usage; u != nil && !u.Total.IsZero() {
			t.line(e.Type, fmt.Sprintf("attempt %d, %d model calls, %d tokens in, %d out",
				b.Attempt, u.Total.ModelCalls, u.Total.PromptTokens, u.Total.CompletionTokens))
		} else {
			t.line(e.Type, fmt.Sprintf("attempt %d", b.Attempt))
		}
		t.answer(b.Text)
	case event.Stopped:
		t.line(e.Type, join(fmt.Sprintf("attempt %d", b.Attempt), b.Reason))
		t.answer(b.Text)
	case event.ToolCall:
		if b.Phase == event.PhaseRequested {
			t.line(e.Type, b.Name+" (requested)")
			return
		}
		t.line(e.Type, join(b.Name, clip(compact(b.Arguments))))
	case event.ToolDone:
		if b.Error != "" {
			t.line(e.Type, b.Name+" failed: "+clip(b.Error))
			return
		}
		t.line(e.Type, b.Name+": "+clip(b.Result))
	case event.Citation:
		where := b.Source.SourceURI
		if where == "" {
			where = join(b.Source.DocumentTitle, b.Source.SectionHeading)
		}
		t.line(e.Type, join(b.Label, where))
	case event.Action:
		if b.Error != "" {
			t.line(e.Type, join(b.Name, b.Target)+" failed: "+clip(b.Error))
			return
		}
		t.line(e.Type, join(join(b.Name, b.Target), clip(b.Summary)))
	case event.UsageCall:
		t.line(e.Type, fmt.Sprintf("%s: %d tokens in (%d cached), %d out",
			orNone(b.Model), b.PromptTokens, b.CachedPromptTokens, b.CompletionTokens))
	default:
		raw, _ := json.Marshal(e.Data)
		t.line(e.Type, clip(compact(raw)))
	}
}

// inlineText streams one fragment of the answer or the reasoning.
func (t *textRenderer) inlineText(typ, text string, thinking bool) {
	if t.inline != typ {
		t.endLine()
		t.inline = typ
		if thinking {
			fmt.Fprintf(t.w, "[%s] ", typ)
			t.mid = true
		}
	}
	if text == "" {
		return
	}
	fmt.Fprint(t.w, text)
	t.mid = !strings.HasSuffix(text, "\n")
	if !thinking {
		t.said = true
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

func (t *textRenderer) line(typ, what string) {
	if what == "" {
		fmt.Fprintf(t.w, "[%s]\n", typ)
		return
	}
	fmt.Fprintf(t.w, "[%s] %s\n", typ, what)
}

func (t *textRenderer) endLine() {
	if t.mid {
		fmt.Fprintln(t.w)
	}
	t.inline, t.mid = "", false
}

func (t *textRenderer) close() { t.endLine() }

// jsonRenderer prints one JSON object per event, per line: the envelope as
// the bus carries it, its data inline.
type jsonRenderer struct{ enc *json.Encoder }

func (j *jsonRenderer) event(e event.Envelope) { _ = j.enc.Encode(e) }

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
	Agent       string                   `json:"agent"`
	Version     string                   `json:"version"`
	Tools       []listedTool             `json:"tools"`
	Unavailable []native.UnavailableTool `json:"unavailable,omitempty"`
}

// listTools prints what a run answering trigger would be granted, as this
// composition resolves it, and runs nothing.
func listTools(ctx context.Context, app *fiber.App, trigger event.Envelope, rec definition.Record, asJSON bool, stdout, stderr io.Writer) int {
	granted, err := app.ToolsFor(ctx, trigger, rec.Name)
	if err != nil {
		fmt.Fprintf(stderr, "fiber run: %v\n", err)
		return exitFailed
	}
	agent, err := definition.Parse(rec.Content, rec.Policy, definition.Checks{Providers: app.Providers()})
	if err != nil {
		fmt.Fprintf(stderr, "fiber run: %v\n", err)
		return exitFailed
	}
	l := listing{Agent: rec.Name, Version: rec.Version, Tools: []listedTool{}, Unavailable: granted.Unavailable}
	for _, d := range granted.Tools {
		l.Tools = append(l.Tools, listedTool{
			Name: d.Name, Description: d.Description, Parameters: d.Parameters,
			Approval: slices.Contains(agent.Spec.Approvals, d.Name),
		})
	}
	if asJSON {
		if err := json.NewEncoder(stdout).Encode(l); err != nil {
			fmt.Fprintf(stderr, "fiber run: %v\n", err)
			return exitFailed
		}
		return exitCompleted
	}
	fmt.Fprintf(stdout, "%s %s would be granted %s:\n", l.Agent, l.Version, count(len(l.Tools), "tool", "tools"))
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
