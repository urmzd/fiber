package author

import (
	"sort"
	"strings"

	"github.com/urmzd/fiber/pkg/agentspec"
)

// diffContext is how many unchanged lines are kept around a change.
const diffContext = 2

// Diff renders what changes between two definitions, as lines prefixed "- "
// for removed, "+ " for added and two spaces for context. It returns the empty
// string when the two canonicalize to the same definition, which is exactly
// when the registry would treat a revise as a no-op.
//
// Both sides are rendered one fact per line (one tool, one spawn grant, one
// prompt line), so the diff is as fine-grained as the edit a person made.
func Diff(from, to agentspec.AgentSpec) string {
	a, b := renderSpec(from.Normalize()), renderSpec(to.Normalize())
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

// renderSpec lays a canonical definition out one fact per line. An absent
// definition (the "from" side of a create) renders as no lines.
func renderSpec(s agentspec.AgentSpec) []string {
	if s.Name == "" && s.SystemPrompt == "" {
		return nil
	}
	lines := []string{
		"name: " + s.Name,
		"description: " + s.Description,
		"model: " + s.Model,
		"provider: " + s.Provider,
	}
	for _, fb := range s.Fallbacks {
		lines = append(lines, "fallback: "+fb.Provider+" "+fb.Model)
	}
	for _, c := range s.FallbackOn {
		lines = append(lines, "fallback_on: "+c)
	}
	for _, t := range s.Tools {
		lines = append(lines, "tool: "+t)
	}
	for _, t := range s.Approvals {
		lines = append(lines, "approval: "+t)
	}
	for _, t := range s.Approvers {
		lines = append(lines, "approver: "+t)
	}
	if s.SelfApproval {
		lines = append(lines, "self_approval: true")
	}
	for _, t := range s.Answerers {
		lines = append(lines, "answerer: "+t)
	}
	if s.TaggedOnly {
		lines = append(lines, "tagged_only: true")
	}
	for _, t := range s.Access.Spawn {
		lines = append(lines, "spawn: "+t)
	}
	for _, a := range s.Access.WorkspaceAreas {
		lines = append(lines, "workspace_area: "+a)
	}
	for _, src := range s.Sources {
		line := "source: " + src.Name + " " + src.URI
		keys := make([]string, 0, len(src.Options))
		for k := range src.Options {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			line += " " + k + "=" + src.Options[k]
		}
		lines = append(lines, line)
	}
	lines = append(lines, "system_prompt:")
	for _, l := range strings.Split(s.SystemPrompt, "\n") {
		lines = append(lines, "  "+strings.TrimRight(l, " \t\r"))
	}
	return lines
}

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
