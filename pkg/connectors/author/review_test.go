package author_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/urmzd/fiber/pkg/connectors/author"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/turn"
)

// compiled is an old-shape spec as the turn.Spec a definition compiles to.
func compiled(t testing.TB, s definition.AgentSpec) turn.Spec {
	t.Helper()
	rec, err := definition.FromAgentSpec(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	a, err := definition.Parse(rec.Content, rec.Policy, definition.Checks{})
	if err != nil {
		t.Fatal(err)
	}
	return a.Spec
}

// tspec is spec as a turn.Spec, without validation, so a draft may be as
// wrong as a test needs.
func tspec(name, prompt string) turn.Spec {
	return turn.Spec{Name: name, Description: "test agent", Model: "offline", Provider: definition.ProviderOffline, SystemPrompt: prompt}
}

// codes flattens findings to "code:subject", in the order Review returned
// them.
func codes(findings []author.Finding) []string {
	out := []string{}
	for _, f := range findings {
		out = append(out, strings.TrimSuffix(f.Code+":"+f.Subject, ":"))
	}
	return out
}

func TestReviewFindsWhatIsWrongWithADraftAndNothingElse(t *testing.T) {
	t.Parallel()
	peer := func(name string, spawn ...string) turn.Spec {
		s := tspec(name, goodPrompt)
		s.Access.Spawn = spawn
		return s
	}
	cases := []struct {
		name  string
		edit  func(*turn.Spec)
		world author.World
		want  []string
	}{
		{
			name: "a prompt with a goal and a format is clean",
			edit: func(*turn.Spec) {},
		},
		{
			name: "no stated goal",
			edit: func(s *turn.Spec) { s.SystemPrompt = "Docs live in the handbook. Reply with plain text." },
			want: []string{author.CodeNoGoal},
		},
		{
			name: "no output format",
			edit: func(s *turn.Spec) { s.SystemPrompt = "You are a docs agent." },
			want: []string{author.CodeNoOutputFormat},
		},
		{
			name: "a granted tool the prompt never mentions",
			edit: func(s *turn.Spec) { s.Tools = []string{"jira_comment"} },
			want: []string{author.CodeUnusedTool + ":jira_comment"},
		},
		{
			name: "a granted tool the prompt does mention",
			edit: func(s *turn.Spec) {
				s.Tools = []string{"jira_comment"}
				s.SystemPrompt += " Leave notes with jira_comment."
			},
		},
		{
			name: "an instruction to use a catalog tool that is not granted",
			edit: func(s *turn.Spec) { s.SystemPrompt += " Close tickets with jira_transition." },
			world: author.World{
				Tools: []string{"jira_comment", "jira_transition"},
			},
			want: []string{author.CodeUngrantedTool + ":jira_transition"},
		},
		{
			name: "an MCP tool group grants its provider's tools and needs no mention",
			edit: func(s *turn.Spec) {
				s.Tools = []string{"mcp:docs"}
				s.SystemPrompt += " Look things up with docs__search."
			},
			world: author.World{
				Tools: []string{"jira_comment", "docs__search", "mcp:docs"},
			},
		},
		{
			name: "an MCP tool group the deployment cannot resolve",
			edit: func(s *turn.Spec) { s.Tools = []string{"mcp:linear"} },
			world: author.World{
				Tools: []string{"jira_comment", "mcp:docs"},
			},
			want: []string{author.CodeUnknownTool + ":mcp:linear"},
		},
		{
			name: "with no catalog, a backticked sibling of a granted tool",
			edit: func(s *turn.Spec) {
				s.Tools = []string{"jira_comment"}
				s.SystemPrompt += " Use `jira_comment`, then `jira_transition`, and pass `issue_key`."
			},
			want: []string{author.CodeUngrantedTool + ":jira_transition"},
		},
		{
			name:  "a granted tool the deployment cannot resolve",
			edit:  func(s *turn.Spec) { s.Tools = []string{"jira_coment"}; s.SystemPrompt += " Use jira_coment." },
			world: author.World{Tools: []string{"jira_comment"}},
			want:  []string{author.CodeUnknownTool + ":jira_coment"},
		},
		{
			name: "a spawn target that does not exist",
			edit: func(s *turn.Spec) {
				s.Access.Spawn = []string{"ghost"}
				s.SystemPrompt += " Hand billing questions to ghost."
			},
			want: []string{author.CodeSpawnMissing + ":ghost"},
		},
		{
			name: "a spawn grant the prompt never mentions",
			edit: func(s *turn.Spec) { s.Access.Spawn = []string{"triage"} },
			world: author.World{
				Agents: []turn.Spec{peer("triage")},
			},
			want: []string{author.CodeUnusedSpawn + ":triage"},
		},
		{
			name: "an instruction to delegate to an agent that is not granted",
			edit: func(s *turn.Spec) { s.SystemPrompt += " Escalate with tag_triage." },
			world: author.World{
				Agents: []turn.Spec{peer("triage")},
			},
			want: []string{author.CodeUngrantedSpawn + ":tag_triage"},
		},
		{
			name: "a delegation loop through another agent",
			edit: func(s *turn.Spec) {
				s.Access.Spawn = []string{"triage"}
				s.SystemPrompt += " Escalate with tag_triage."
			},
			world: author.World{
				Agents: []turn.Spec{peer("triage", "support"), peer("support", "docs")},
			},
			want: []string{author.CodeSpawnCycle + ":docs -> triage -> support -> docs"},
		},
		{
			name: "an agent that may delegate to itself",
			edit: func(s *turn.Spec) {
				s.Access.Spawn = []string{"docs"}
				s.SystemPrompt += " Retry with tag_docs."
			},
			want: []string{author.CodeSpawnCycle + ":docs -> docs"},
		},
		{
			name: "a stored copy of the draft does not hide a loop the draft removes",
			edit: func(s *turn.Spec) {},
			world: author.World{
				Agents: []turn.Spec{peer("docs", "triage"), peer("triage", "docs")},
			},
		},
		{
			name: "dates and times in the cached prefix",
			edit: func(s *turn.Spec) {
				s.SystemPrompt += " Today is 2026-03-04. Office hours end at 17:30. The freeze starts March 9, 2026."
			},
			want: []string{
				author.CodeVolatile + ":17:30",
				author.CodeVolatile + ":2026-03-04",
				author.CodeVolatile + ":march 9, 2026",
				author.CodeVolatile + ":today is",
			},
		},
		{
			name: "a prompt past the length guideline",
			edit: func(s *turn.Spec) {
				s.SystemPrompt += strings.Repeat(" Be helpful.", author.MaxPromptLen/12)
			},
			want: []string{author.CodePromptTooLong},
		},
		{
			name: "errors sort ahead of warnings ahead of info",
			edit: func(s *turn.Spec) {
				s.SystemPrompt = "Docs live in the handbook."
				s.Tools = []string{"search"}
				s.Access.Spawn = []string{"ghost"}
			},
			want: []string{
				author.CodeSpawnMissing + ":ghost",
				author.CodeNoGoal,
				author.CodeNoOutputFormat,
				author.CodeUnusedSpawn + ":ghost",
				author.CodeUnusedTool + ":search",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			draft := tspec("docs", goodPrompt)
			tc.edit(&draft)
			got := author.Review(draft, tc.world)
			want := tc.want
			if want == nil {
				want = []string{}
			}
			if !reflect.DeepEqual(codes(got), want) {
				t.Fatalf("findings = %v\n    want %v\n%+v", codes(got), want, got)
			}
			if again := author.Review(draft, tc.world); !reflect.DeepEqual(got, again) {
				t.Fatalf("Review is not deterministic:\n%+v\n%+v", got, again)
			}
			for _, f := range got {
				if f.Message == "" || f.Severity == "" {
					t.Errorf("finding %+v is missing a message or a severity", f)
				}
			}
		})
	}
}

// The linter is only credible if what this package ships passes it.
func TestTheBuiltInPromptPassesItsOwnReview(t *testing.T) {
	t.Parallel()
	own := compiled(t, author.Spec("offline", definition.ProviderOffline))
	for _, world := range []author.World{{}, {Tools: author.ToolNames()}} {
		if got := author.Review(own, world); len(got) != 0 {
			t.Errorf("the built-in prompt has findings: %+v", got)
		}
	}
	for _, name := range author.ToolNames(author.EffectRead, author.EffectPropose, author.EffectWrite) {
		if !strings.Contains(own.SystemPrompt, name) {
			t.Errorf("the built-in prompt never mentions %s", name)
		}
	}
	for _, phrase := range []string{"Inspect before you change", "one change per approval", "diff and every finding"} {
		if !strings.Contains(own.SystemPrompt, phrase) {
			t.Errorf("the built-in prompt lost the instruction %q", phrase)
		}
	}
}
