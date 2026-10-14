package cli

import (
	"fmt"
	"strings"
	"time"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/agentrt/payload"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// eventJSON is the shape the streaming commands emit under --format json.
//
// It is a hand-written struct rather than protojson of the wire Event for one
// reason: payload is bytes on the wire, and protojson would base64 it. A
// consumer piping `mandatum listen` into jq wants the citation it can read, not
// a blob it has to decode in a second step. Kind-specific bodies are decoded
// here and inlined, and anything unrecognised falls back to the raw string so
// a new kind is still legible to an old client.
type eventJSON struct {
	Seq     uint64    `json:"seq"`
	Topic   string    `json:"topic"`
	RunID   string    `json:"run_id"`
	Agent   string    `json:"agent"`
	Rev     int32     `json:"rev"`
	Origin  string    `json:"origin"`
	Kind    string    `json:"kind"`
	At      time.Time `json:"at"`
	Payload any       `json:"payload,omitempty"`
}

func toEventJSON(e *mandatumv1.Event) eventJSON {
	out := eventJSON{
		Seq:    e.GetSeq(),
		Topic:  e.GetTopic(),
		RunID:  e.GetRunId(),
		Agent:  e.GetAgent(),
		Rev:    e.GetRev(),
		Origin: e.GetOrigin(),
		Kind:   e.GetKind(),
	}
	if ts := e.GetAt(); ts != nil {
		out.At = ts.AsTime()
	}
	if body, known, err := payload.Of(asEnvelope(e)); err == nil && known {
		out.Payload = body
	} else if raw := e.GetPayload(); len(raw) > 0 {
		out.Payload = string(raw)
	}
	return out
}

// asEnvelope rebuilds enough of an envelope.Event for payload.Of to dispatch
// on. Only Kind and Payload are read, but the whole value is filled so that a
// future decoder needing more does not silently see zeros.
func asEnvelope(e *mandatumv1.Event) envelope.Event {
	return envelope.Event{
		Seq:     e.GetSeq(),
		RunID:   e.GetRunId(),
		Tenant:  e.GetTenant(),
		Agent:   e.GetAgent(),
		Rev:     int(e.GetRev()),
		Origin:  e.GetOrigin(),
		Kind:    envelope.Kind(e.GetKind()),
		Payload: e.GetPayload(),
	}
}

// renderEventText writes one event for a human.
//
// answerOnly is what makes `mandatum tag` feel like a conversation rather than a
// log: text fragments are written bare and unterminated so they accumulate into
// the answer as it arrives, and everything else goes to stderr as a diagnostic.
// `mandatum listen` sets it false and gets one labelled line per event, because
// there a stream of many runs concatenated into one paragraph would be
// unreadable.
func renderEventText(u *ui, e *mandatumv1.Event, answerOnly bool) {
	kind := envelope.Kind(e.GetKind())
	body, known, err := payload.Of(asEnvelope(e))
	if err != nil || !known {
		if !answerOnly {
			u.printf("%s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), kind)
		}
		return
	}

	switch v := body.(type) {
	case payload.Text:
		if answerOnly {
			u.printf("%s", v.Text)
			u.flush()
			return
		}
		u.printf("%s %s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), u.cyan(string(kind)), oneLine(v.Text))

	case payload.Thinking:
		// Reasoning is a diagnostic even in listen: a reader asked for the
		// agent's output, and thinking is how it got there.
		u.logf("%s %s\n", u.dim("thinking"), u.dim(oneLine(v.Text)))

	case payload.ToolCall:
		u.logf("%s %s %s\n", u.blue("tool"), v.Name, u.dim(v.Phase))

	case payload.ToolDone:
		if v.Error != "" {
			u.logf("%s %s %s\n", u.red("tool"), v.Name, u.red(oneLine(v.Error)))
			return
		}
		u.logf("%s %s %s\n", u.green("tool"), v.Name, u.dim(oneLine(v.Result)))

	case payload.Citation:
		// Citations go to stdout even in answerOnly mode: they are part of the
		// answer, not commentary on it, and an answer whose grounding is only
		// visible on stderr cannot be piped anywhere useful.
		src := v.Source.SourceURI
		if src == "" {
			src = strings.TrimSpace(v.Source.DocumentTitle + " " + v.Source.SectionHeading)
		}
		if answerOnly {
			u.printf("\n%s %s %s\n", u.yellow(v.Label), oneLine(v.Text), u.dim(src))
			return
		}
		u.printf("%s %s %s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), u.yellow(string(kind)), oneLine(v.Text), u.dim(src))

	case payload.Lifecycle:
		line := string(kind)
		if v.Attempt > 0 {
			line += fmt.Sprintf(" attempt=%d", v.Attempt)
		}
		if v.Rev > 0 {
			line += fmt.Sprintf(" rev=%d", v.Rev)
		}
		paint := u.dim
		switch kind {
		case envelope.KindCompleted:
			paint = u.green
		case envelope.KindFailed:
			paint = u.red
		}
		if answerOnly {
			u.logf("%s\n", paint(line))
			logUsage(u, v.Usage)
			return
		}
		u.printf("%s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), paint(line))

	case payload.Stopped:
		line := string(kind)
		if v.Attempt > 0 {
			line += fmt.Sprintf(" attempt=%d", v.Attempt)
		}
		if v.Rev > 0 {
			line += fmt.Sprintf(" rev=%d", v.Rev)
		}
		if v.By != "" {
			line += " by=" + oneLine(v.By)
		}
		if v.Reason != "" {
			line += " reason=" + fmt.Sprintf("%q", oneLine(v.Reason))
		}
		if answerOnly {
			u.logf("%s\n", u.yellow(line))
			logUsage(u, v.Usage)
			return
		}
		u.printf("%s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), u.yellow(line))

	case payload.StopRequest:
		if answerOnly {
			return
		}
		u.printf("%s %s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), u.yellow(string(kind)), oneLine(v.RunID))

	case payload.Action:
		u.logf("%s %s\n", u.green("action"), oneLine(v.Summary))

	case payload.UsageCall:
		// One line per call in listen, where the stream is the point. In
		// tag the run's total follows the terminal event instead, so the
		// answer is not interleaved with accounting.
		if answerOnly {
			return
		}
		line := callLine(v)
		u.printf("%s %s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), u.dim(string(kind)), line)

	case payload.UsageChild:
		if answerOnly {
			return
		}
		u.printf("%s %s %s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), u.dim(string(kind)),
			oneLine(firstNonEmptyString(v.Agent, v.RunID)), usageLine(v.Usage))

	default:
		if !answerOnly {
			u.printf("%s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), kind)
		}
	}
}

// logUsage writes a run's total after its terminal event, on stderr: it is
// about the run, not part of the answer.
func logUsage(u *ui, usage *payload.RunUsage) {
	if usage == nil || usage.Total.ModelCalls == 0 {
		return
	}
	line := usageLine(usage.Total)
	if c := usage.Context; c != nil && c.Window > 0 {
		line += fmt.Sprintf(", context %s/%s", count(c.Tokens), count(c.Window))
	}
	u.logf("%s %s\n", u.dim("usage"), u.dim(line))
}

// usageLine renders summed usage for a person: calls, tokens, and the cost
// only where every call was priced, said to be partial otherwise.
func usageLine(t payload.Usage) string {
	calls := "calls"
	if t.ModelCalls == 1 {
		calls = "call"
	}
	line := fmt.Sprintf("%d %s, %s in", t.ModelCalls, calls, count(t.PromptTokens))
	if t.CachedPromptTokens > 0 || t.CacheWriteTokens > 0 {
		line += fmt.Sprintf(" (%s cached, %s written)", count(t.CachedPromptTokens), count(t.CacheWriteTokens))
	}
	line += fmt.Sprintf(", %s out", count(t.CompletionTokens))
	costs := ""
	if t.Cost != nil {
		costs = money(*t.Cost)
	}
	for _, c := range t.OtherCosts {
		if costs != "" {
			costs += " + "
		}
		costs += money(c)
	}
	switch {
	case costs != "" && t.UnpricedCalls == 0:
		line += ", " + costs
	case costs != "":
		line += fmt.Sprintf(", at least %s (%d calls unpriced)", costs, t.UnpricedCalls)
	default:
		line += ", cost unknown"
	}
	return line
}

// callLine renders one model call.
func callLine(c payload.UsageCall) string {
	line := c.Model
	if c.Depth > 0 {
		line = fmt.Sprintf("%s via %s", line, c.Agent)
	}
	if c.ResponseCacheHit {
		return line + " response cache, nothing spent"
	}
	line += fmt.Sprintf(" %s in, %s out", count(c.PromptTokens), count(c.CompletionTokens))
	if c.ContextWindow > 0 {
		line += fmt.Sprintf(", context %s/%s", count(c.PromptTokens), count(c.ContextWindow))
	}
	if c.Cost != nil {
		line += ", " + money(*c.Cost)
	} else {
		line += ", cost unknown"
	}
	return strings.TrimSpace(line)
}

// money renders integer micro-units exactly: six decimal places, no rounding.
func money(c payload.Cost) string {
	sign := ""
	m := c.Micros
	if m < 0 {
		sign, m = "-", -m
	}
	s := fmt.Sprintf("%s %s%d.%06d", c.Currency, sign, m/1_000_000, m%1_000_000)
	if c.AsOf != "" {
		s += " as of " + c.AsOf
	}
	return s
}

// count renders a token count with thousands separators.
func count(n int) string {
	s := fmt.Sprintf("%d", n)
	if n < 0 {
		return s
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func firstNonEmptyString(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// oneLine flattens a fragment for a single-line render. Deltas arrive with
// whatever whitespace the model produced, and a newline inside a table cell
// breaks the alignment of every row after it.
func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", "")
	s = strings.TrimSpace(s)
	const max = 120
	if len(s) > max {
		return s[:max-1] + "…"
	}
	return s
}

// terminal reports whether an event ends its run. `tag` stops on it; `listen`
// ignores it, because a topic outlives the runs on it.
func terminal(e *mandatumv1.Event) bool {
	switch envelope.Kind(e.GetKind()) {
	case envelope.KindCompleted, envelope.KindFailed, envelope.KindStopped:
		return true
	default:
		return false
	}
}
