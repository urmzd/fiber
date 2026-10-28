package author

import (
	"encoding/json"
	"strings"

	"github.com/urmzd/fiber/pkg/definition"
)

// diffContext is how many unchanged lines are kept around a change.
const diffContext = 2

// Diff renders what changes between two drafts, as lines prefixed "- " for
// removed, "+ " for added and two spaces for context. It returns the empty
// string when the two are the same file and the same policy.
//
// Both sides are rendered as the definition file, line by line, then the
// policy one fact per line, so the diff is as fine-grained as the edit a
// person made.
func Diff(from, to Draft) string {
	a, b := renderDraft(from), renderDraft(to)
	ops := lineDiff(a, b)
	changed := false
	for _, op := range ops {
		if op.kind != ' ' {
			changed = true
			break
		}
	}
	if !changed {
		return ""
	}
	// Keep each change and diffContext lines either side of it.
	keep := make([]bool, len(ops))
	for i, op := range ops {
		if op.kind == ' ' {
			continue
		}
		for j := max(0, i-diffContext); j <= min(len(ops)-1, i+diffContext); j++ {
			keep[j] = true
		}
	}
	var out strings.Builder
	skipped := false
	for i, op := range ops {
		if !keep[i] {
			skipped = true
			continue
		}
		if skipped && out.Len() > 0 {
			out.WriteString("  ...\n")
		}
		skipped = false
		out.WriteByte(op.kind)
		out.WriteByte(' ')
		out.WriteString(op.line)
		out.WriteByte('\n')
	}
	return strings.TrimRight(out.String(), "\n")
}

// renderDraft lays a draft out one line per fact. An absent draft (the
// "from" side of a create) renders as no lines.
func renderDraft(d Draft) []string {
	if strings.TrimSpace(d.Definition) == "" {
		return nil
	}
	var lines []string
	for _, l := range strings.Split(strings.TrimRight(d.Definition, "\n"), "\n") {
		lines = append(lines, strings.TrimRight(l, " \t\r"))
	}
	raw, err := json.MarshalIndent(d.Policy.Normalize(), "", "  ")
	if err == nil && string(raw) != "{}" {
		lines = append(lines, "# policy")
		lines = append(lines, strings.Split(string(raw), "\n")...)
	}
	return lines
}

// Draft is a definition as the actions take and show it: the .agent.md file
// and its policy.
type Draft struct {
	Definition string            `json:"definition"`
	Policy     definition.Policy `json:"policy,omitzero"`
}

// draftOf is a stored record as a draft.
func draftOf(r definition.Record) Draft { return Draft{Definition: r.Content, Policy: r.Policy} }

type diffOp struct {
	kind byte // ' ', '-' or '+'
	line string
}

// lineDiff is a longest-common-subsequence diff. Definitions are a few hundred
// lines at most, so the quadratic table is cheaper than a dependency.
func lineDiff(a, b []string) []diffOp {
	lcs := make([][]int, len(a)+1)
	for i := range lcs {
		lcs[i] = make([]int, len(b)+1)
	}
	for i := len(a) - 1; i >= 0; i-- {
		for j := len(b) - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	ops := make([]diffOp, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			ops = append(ops, diffOp{' ', a[i]})
			i++
			j++
		case lcs[i+1][j] >= lcs[i][j+1]:
			ops = append(ops, diffOp{'-', a[i]})
			i++
		default:
			ops = append(ops, diffOp{'+', b[j]})
			j++
		}
	}
	for ; i < len(a); i++ {
		ops = append(ops, diffOp{'-', a[i]})
	}
	for ; j < len(b); j++ {
		ops = append(ops, diffOp{'+', b[j]})
	}
	return ops
}
