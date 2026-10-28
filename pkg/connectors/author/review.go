package author

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/turn"
)

// Severity ranks a finding.
type Severity string

// Severities, most serious first.
const (
	// SeverityError marks a definition that will misbehave or fail to build.
	SeverityError Severity = "error"
	// SeverityWarning marks a definition that works and is probably wrong.
	SeverityWarning Severity = "warning"
	// SeverityInfo marks something worth a second look.
	SeverityInfo Severity = "info"
)

// Finding codes. They are stable identifiers a caller may filter on.
const (
	CodeNoGoal         = "no-goal"
	CodeNoOutputFormat = "no-output-format"
	CodeUngrantedTool  = "ungranted-tool"
	CodeUnknownTool    = "unknown-tool"
	CodeUnusedTool     = "unused-tool"
	CodeUngrantedSpawn = "ungranted-spawn"
	CodeUnusedSpawn    = "unused-spawn"
	CodeSpawnMissing   = "spawn-missing"
	CodeSpawnCycle     = "spawn-cycle"
	CodeVolatile       = "volatile-content"
	CodePromptTooLong  = "prompt-too-long"
)

// MaxPromptLen is the system prompt length, in characters, past which Review
// reports CodePromptTooLong. It is a smell threshold, not a limit anything
// enforces.
const MaxPromptLen = 16000

// delegatePrefix is the prefix of the tool the runtime derives from each spawn
// grant. It mirrors pkg/runtime.DelegateToolName, restated here so a linter
// does not import the runtime.
const delegatePrefix = "tag_"

// Finding is one thing Review noticed about a draft.
type Finding struct {
	// Code identifies the rule.
	Code string `json:"code"`
	// Severity ranks it.
	Severity Severity `json:"severity"`
	// Subject is the tool, agent or text the finding is about, when there is
	// one.
	Subject string `json:"subject,omitempty"`
	// Message says what is wrong and what to do about it.
	Message string `json:"message"`
}

// World is what Review knows about the deployment the draft will live in.
// Both fields are optional, and each one switches on the rules that need it.
type World struct {
	// Agents are the tenant's current definitions. They let Review check that
	// spawn targets exist and that delegation does not loop. A definition with
	// the draft's own name is replaced by the draft.
	Agents []turn.Spec
	// Tools are the tool names the deployment can resolve. With them, a tool
	// reference is checked exactly. Without them, Review falls back to a
	// narrower heuristic described on Review.
	Tools []string
}

// Review lints a draft definition and returns what it found, most serious
// first.
//
// It is a pure function of its arguments: no model, no clock, no I/O. The
// same draft in the same world yields the same findings in the same order,
// which is what lets it back both an agent tool and a CI lint.
//
// The rules are heuristics over text and are documented as such:
//
//   - A goal is recognized by second-person role or purpose phrasing ("you
//     are", "your job", "goal:"). An output format by words such as "format",
//     "respond with", "JSON" or "concise".
//   - A tool reference is an identifier in the prompt, such as jira_comment.
//     With World.Tools it is any identifier that names a tool in the catalog.
//     Without, it is a backticked identifier sharing a connector prefix with a
//     tool the spec does grant, which catches the common slip (granting
//     jira_comment, instructing the agent to use jira_transition) without
//     flagging every snake_case word. A tool group "mcp:docs" (or
//     "mcp:docs:write") counts as
//     granting every identifier with the docs_ prefix, since an MCP tool's
//     name is qualified by its provider ("docs__search").
//   - Volatile content is a date, a clock time, or phrasing such as "today
//     is". The system prompt is the cached prefix of every request, so
//     anything in it that changes between runs defeats prompt caching.
//
// Review does not validate: it reads a compiled spec (definition.Parse), and
// a draft that does not compile is reported by validation instead.
func Review(spec turn.Spec, world World) []Finding {
	prompt := strings.ToLower(spec.SystemPrompt)
	words := wordSet(prompt)

	var out []Finding
	add := func(code string, sev Severity, subject, format string, args ...any) {
		out = append(out, Finding{Code: code, Severity: sev, Subject: subject, Message: fmt.Sprintf(format, args...)})
	}

	if !goalPattern.MatchString(prompt) {
		add(CodeNoGoal, SeverityWarning, "", "the prompt never states what the agent is for; open with its role and goal")
	}
	if !formatPattern.MatchString(prompt) {
		add(CodeNoOutputFormat, SeverityWarning, "", "the prompt never says what an answer should look like; state the output format")
	}

	granted := toSet(lower(spec.Tools))
	spawn := toSet(spec.Access.Spawn)
	catalog := toSet(lower(world.Tools))
	agents := make(map[string]turn.Spec, len(world.Agents)+1)
	for _, a := range world.Agents {
		agents[a.Name] = a
	}
	agents[spec.Name] = spec

	grantedPrefixes := map[string]bool{}
	groups := map[string]bool{}
	for t := range granted {
		if g, ok, err := definition.ParseMCPGrant(t); ok {
			if err == nil {
				// "mcp:docs:write" is the docs group too.
				groups[g.Provider] = true
				grantedPrefixes[g.Provider] = true
			}
			continue
		}
		if p, _, ok := strings.Cut(t, "_"); ok {
			grantedPrefixes[p] = true
		}
	}

	// Instructions that reach for something the spec does not grant.
	for _, w := range sortedKeys(words) {
		if !strings.ContainsAny(w, "_.") || granted[w] {
			continue
		}
		// A connector's actions are named after it, so a tool sharing a
		// granted group's prefix is taken as granted through the group.
		if groups[prefixOf(w)] {
			continue
		}
		if target, ok := strings.CutPrefix(w, delegatePrefix); ok {
			_, known := agents[target]
			if !spawn[target] && (known || backticked(prompt, w)) {
				add(CodeUngrantedSpawn, SeverityError, w,
					"the prompt tells the agent to delegate with %s, but %q is not in access.spawn, so that tool will not exist", w, target)
			}
			continue
		}
		switch {
		case catalog[w]:
			add(CodeUngrantedTool, SeverityError, w,
				"the prompt refers to the tool %s, which the spec does not grant; grant it or remove the instruction", w)
		case len(catalog) == 0 && backticked(prompt, w) && grantedPrefixes[prefixOf(w)]:
			add(CodeUngrantedTool, SeverityWarning, w,
				"the prompt refers to %s, which looks like a tool and is not granted; grant it or remove the instruction", w)
		}
	}

	for _, t := range spec.Tools {
		key := strings.ToLower(t)
		if len(catalog) > 0 && !catalog[key] {
			add(CodeUnknownTool, SeverityError, t,
				"the spec grants %s, which this deployment cannot resolve; an agent naming a missing tool fails to build", t)
		}
		if !words[key] && !strings.HasPrefix(key, definition.MCPGroupPrefix) {
			add(CodeUnusedTool, SeverityInfo, t,
				"the spec grants %s but the prompt never mentions it; say when to use it or drop the grant", t)
		}
	}

	for _, target := range spec.Access.Spawn {
		if _, ok := agents[target]; !ok {
			add(CodeSpawnMissing, SeverityError, target,
				"access.spawn names %q, which is not an agent in this tenant; create it first or remove the grant", target)
		}
		if !words[target] && !words[delegatePrefix+target] {
			add(CodeUnusedSpawn, SeverityInfo, target,
				"the agent may delegate to %q but the prompt never mentions it; say when to delegate or drop the grant", target)
		}
	}
	if cycle := spawnCycle(spec.Name, agents); cycle != nil {
		add(CodeSpawnCycle, SeverityError, strings.Join(cycle, " -> "),
			"delegation loops back to this agent (%s); two agents that can tag each other can do so forever", strings.Join(cycle, " -> "))
	}

	seen := map[string]bool{}
	for _, re := range volatilePatterns {
		for _, m := range re.FindAllString(prompt, -1) {
			if seen[m] {
				continue
			}
			seen[m] = true
			add(CodeVolatile, SeverityWarning, m,
				"%q changes between runs; the system prompt is the cached prefix of every request, so pass volatile facts in the request instead", m)
		}
	}

	if n := utf8.RuneCountInString(spec.SystemPrompt); n > MaxPromptLen {
		add(CodePromptTooLong, SeverityWarning, "",
			"the prompt is %d characters, over the %d guideline; move reference material into a source the agent retrieves from", n, MaxPromptLen)
	}

	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if ra, rb := severityRank(a.Severity), severityRank(b.Severity); ra != rb {
			return ra < rb
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		return a.Subject < b.Subject
	})
	return out
}

var (
	goalPattern = regexp.MustCompile(`\byou are\b|\byou're\b|\byour (job|goal|task|role|purpose|mission)\b|\b(goal|objective|purpose|role|mission):` +
		`|\byou (\w+ )?(answer|help|route|review|write|triage|handle|decide|create|manage|summari[sz]e|classify|draft)\b`)
	formatPattern = regexp.MustCompile(`\bformat|\b(respond|reply|answer) (with|in)\b|\boutput\b|\bjson\b|\bmarkdown\b|\bbullet|\bsentences?\b|\bparagraphs?\b` +
		`|\bconcise|\bbrief|\btable\b|\bone line\b|\bplain text\b`)

	months = `(?:january|february|march|april|may|june|july|august|september|october|november|december)`
	// volatilePatterns are checked in order, so findings are stable.
	volatilePatterns = []*regexp.Regexp{
		regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}\b`),
		regexp.MustCompile(`\b\d{1,2}/\d{1,2}/\d{2,4}\b`),
		regexp.MustCompile(`\b` + months + ` \d{1,2}(?:st|nd|rd|th)?(?:,? \d{4})?\b`),
		regexp.MustCompile(`\b` + months + ` \d{4}\b`),
		regexp.MustCompile(`\b\d{1,2}:\d{2}(?::\d{2})?\b`),
		regexp.MustCompile(`\btoday is\b|\bthe date is\b|\bcurrent (?:date|time)\b|\bas of today\b|\bright now it is\b`),
	}
)

func severityRank(s Severity) int {
	switch s {
	case SeverityError:
		return 0
	case SeverityWarning:
		return 1
	}
	return 2
}

// wordSet splits lower-cased text into identifier-shaped words. The characters
// "_", "." and "-" stay inside a word, because tool names carry the first two
// and agent names the third, and are trimmed from its ends, because a sentence
// ends in a full stop.
func wordSet(text string) map[string]bool {
	out := map[string]bool{}
	for _, w := range strings.FieldsFunc(text, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' && r != '.' && r != '-'
	}) {
		if w = strings.Trim(w, "_.-"); w != "" {
			out[w] = true
		}
	}
	return out
}

func backticked(prompt, word string) bool { return strings.Contains(prompt, "`"+word+"`") }

func prefixOf(word string) string {
	p, _, _ := strings.Cut(word, "_")
	return p
}

func lower(values []string) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = strings.ToLower(v)
	}
	return out
}

func toSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, v := range values {
		out[v] = true
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// spawnCycle returns the shortest delegation path from start back to itself,
// or nil. Neighbours are visited in sorted order, so the path reported for a
// graph with several cycles is always the same one.
func spawnCycle(start string, agents map[string]turn.Spec) []string {
	type step struct {
		name string
		path []string
	}
	visited := map[string]bool{}
	queue := []step{{start, []string{start}}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		next := append([]string(nil), agents[cur.name].Access.Spawn...)
		sort.Strings(next)
		for _, n := range next {
			if n == start {
				return append(cur.path, start)
			}
			if _, ok := agents[n]; !ok || visited[n] {
				continue
			}
			visited[n] = true
			queue = append(queue, step{n, append(append([]string(nil), cur.path...), n)})
		}
	}
	return nil
}

// renderFindings is how findings are shown to a model or a person.
func renderFindings(findings []Finding) string {
	if len(findings) == 0 {
		return "review: no findings"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "review: %d finding(s)", len(findings))
	for _, f := range findings {
		fmt.Fprintf(&b, "\n- %s %s: %s", f.Severity, f.Code, f.Message)
	}
	return b.String()
}
