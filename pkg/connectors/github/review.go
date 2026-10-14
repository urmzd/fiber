package github

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/urmzd/mandatum/pkg/connectors/internal/render"
	"github.com/urmzd/mandatum/pkg/connectors/internal/sink"
)

// ReviewFormat identifies a structured final answer, distinct from ordinary text.
const ReviewFormat = "mandatum.github.review.v1"

// ReviewReport is the agent's evidence-backed final answer. Counts are derived
// from findings, never supplied by the model. Evidence is cited, not verified
// by the formatter; the agent must obtain it from tools before making claims.
type ReviewReport struct {
	Format   string          `json:"format"`
	Summary  string          `json:"summary"`
	Comments []string        `json:"comments,omitempty"`
	Findings []ReviewFinding `json:"findings"`
	Related  []ReviewLink    `json:"related,omitempty"`
}

// ReviewFinding couples a severity and suggestion to inspectable evidence.
type ReviewFinding struct {
	Severity   string           `json:"severity"`
	Title      string           `json:"title"`
	Suggestion string           `json:"suggestion"`
	Evidence   []ReviewEvidence `json:"evidence"`
	Prompt     string           `json:"prompt"`
}

// ReviewEvidence contains an observed fact and a source pointer. Source can be
// an HTTPS permalink or a concrete local location, such as a test command.
type ReviewEvidence struct {
	Observation string `json:"observation"`
	Source      string `json:"source"`
}

// ReviewLink is a related concept or reference shown after the findings.
type ReviewLink struct {
	Title string `json:"title"`
	URL   string `json:"url"`
}

// FormatReview renders a complete report, rejecting unsupported or unsupported-by-
// evidence findings instead of presenting them as a clean review.
func FormatReview(report ReviewReport) (string, error) {
	if report.Format != ReviewFormat || strings.TrimSpace(report.Summary) == "" {
		return "", fmt.Errorf("github: review needs format %q and a summary", ReviewFormat)
	}
	counts := map[string]int{"error": 0, "warning": 0, "info": 0}
	for i, f := range report.Findings {
		if _, ok := counts[f.Severity]; !ok {
			return "", fmt.Errorf("github: finding %d has invalid severity %q", i+1, f.Severity)
		}
		if strings.TrimSpace(f.Title) == "" || strings.TrimSpace(f.Suggestion) == "" || strings.TrimSpace(f.Prompt) == "" || len(f.Evidence) == 0 {
			return "", fmt.Errorf("github: finding %d needs a title, suggestion, evidence and prompt", i+1)
		}
		for _, e := range f.Evidence {
			if strings.TrimSpace(e.Observation) == "" || strings.TrimSpace(e.Source) == "" {
				return "", fmt.Errorf("github: finding %d has incomplete evidence", i+1)
			}
		}
		counts[f.Severity]++
	}
	var b strings.Builder
	errorLabel, warningLabel := "errors", "warnings"
	if counts["error"] == 1 {
		errorLabel = "error"
	}
	if counts["warning"] == 1 {
		warningLabel = "warning"
	}
	fmt.Fprintf(&b, "🔴 **%d %s** · 🟡 **%d %s** · 🔵 **%d info**\n\n## Summary\n\n%s\n", counts["error"], errorLabel, counts["warning"], warningLabel, counts["info"], reviewText(report.Summary))
	if len(report.Comments) > 0 {
		b.WriteString("\n## Comments\n\n")
		for _, comment := range report.Comments {
			fmt.Fprintf(&b, "- %s\n", reviewText(comment))
		}
	}
	if len(report.Findings) > 0 {
		b.WriteString("\n## Findings\n\n| Level | Finding | Suggestion | Evidence | Follow-up prompt |\n| :---: | :--- | :--- | :--- | :--- |\n")
		icons := map[string]string{"error": "🔴 Error", "warning": "🟡 Warning", "info": "🔵 Info"}
		for _, f := range report.Findings {
			var evidence []string
			for _, e := range f.Evidence {
				evidence = append(evidence, reviewText(e.Observation)+" ("+reviewPointer(e.Source)+")")
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", icons[f.Severity], reviewText(f.Title), reviewText(f.Suggestion), strings.Join(evidence, "<br><br>"), reviewText(f.Prompt))
		}
	} else {
		b.WriteString("\nNo findings reported. See the summary and comments for review scope and limitations.\n")
	}
	if len(report.Related) > 0 {
		b.WriteString("\n## Related concepts\n\n")
		for _, link := range report.Related {
			fmt.Fprintf(&b, "- %s: %s\n", reviewText(link.Title), reviewPointer(link.URL))
		}
	}
	b.WriteString("\n---\n\nReact with 👍 or 👎 on this comment. Reply and tag the agent to continue the review.\n")
	result := b.String()
	if len(result) > MaxBodyChars-2048 {
		return "", fmt.Errorf("github: structured review is too long; shorten findings without removing their evidence")
	}
	return result, nil
}

// reviewText keeps model text inside its own table cell or paragraph. Markup is
// owned by the formatter, so pipes, HTML, links and mentions cannot alter layout.
func reviewText(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "|", "&#124;", "\\", "&#92;", "`", "&#96;", "*", "&#42;", "_", "&#95;", "[", "&#91;", "]", "&#93;", "@", "&#64;").Replace(s)
}

func reviewPointer(source string) string {
	u, err := url.Parse(source)
	if err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Host != "" && u.User == nil {
		dest := strings.NewReplacer("(", "%28", ")", "%29", "<", "%3C", ">", "%3E", "|", "%7C", " ", "%20", "\n", "%0A", "\r", "%0D").Replace(u.String())
		return "[source](" + dest + ")"
	}
	return reviewText(source)
}

func reviewBody(v render.View) string {
	// Structured agent output stays out of the comment until it is a complete
	// report. Progress still comes from lifecycle and tool events.
	text := strings.TrimSpace(v.Text)
	var report ReviewReport
	structured := json.Unmarshal([]byte(text), &report) == nil && report.Format == ReviewFormat
	pending := strings.HasPrefix(text, "{") && strings.Contains(text, ReviewFormat)
	if !structured && !pending && (!strings.HasPrefix(text, "{") || v.Terminal()) {
		return sink.Clamp(sink.Layout(markdown{}, v), MaxBodyChars)
	}
	v.Text = ""
	v.Thinking = ""
	status := sink.Layout(markdown{}, render.View{Agent: v.Agent, Status: v.Status})
	if !v.Terminal() {
		v.Text = "Preparing an evidence-backed review…"
		return sink.Clamp(sink.Layout(markdown{}, v), MaxBodyChars)
	}
	if !structured {
		return status + "\n\n⚠️ **Review incomplete**: the agent did not return a complete structured report. No finding counts are available."
	}
	body, err := FormatReview(report)
	if err != nil {
		return status + "\n\n⚠️ **Review needs correction**: " + reviewText(err.Error())
	}
	if v.Error != "" {
		body += "\n\n⚠️ **Run failed**: " + reviewText(v.Error)
	}
	return sink.Clamp(status+"\n\n"+body, MaxBodyChars)
}
