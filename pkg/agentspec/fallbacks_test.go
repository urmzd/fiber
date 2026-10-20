package agentspec_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/urmzd/fiber/pkg/agentspec"
)

// Fallbacks came after revisions were already stored with their hashes. A spec
// with none, nil or empty, hashes exactly as it did before they existed, so no
// stored revision reads as changed.
func TestFallbacksLeaveTheHashOfASpecWithoutThemUnchanged(t *testing.T) {
	t.Parallel()
	const before = "sha256:3dbf7901c3a3336ee0afd3806eca2f03c1348dbaa8c0dc49dcc9614d5d91da95" // valid().Hash() in v0.10.0
	empty := valid()
	empty.Fallbacks = []agentspec.ModelRef{}
	for name, s := range map[string]agentspec.AgentSpec{"nil": valid(), "empty": empty} {
		if got := s.Hash(); got != before {
			t.Errorf("a spec with %s fallbacks hashes to %s, want %s as before fallbacks existed", name, got, before)
		}
	}
	if empty.Normalize().Fallbacks != nil {
		t.Error("Normalize kept an empty fallback list instead of collapsing it to nil")
	}
}

// Fallbacks are content, and their order is the order they are tried in, so
// adding one or reordering them is an edit; presentation is not.
func TestFallbacksArePartOfTheRevisionInOrder(t *testing.T) {
	t.Parallel()
	a := agentspec.ModelRef{Provider: agentspec.ProviderOpenAI, Model: "gpt-5"}
	b := agentspec.ModelRef{Provider: agentspec.ProviderOllama, Model: "llama4"}

	one := valid()
	one.Fallbacks = []agentspec.ModelRef{a}
	if one.Hash() == valid().Hash() {
		t.Fatal("adding a fallback did not change the hash")
	}
	ab, ba := valid(), valid()
	ab.Fallbacks = []agentspec.ModelRef{a, b}
	ba.Fallbacks = []agentspec.ModelRef{b, a}
	if ab.Hash() == ba.Hash() {
		t.Error("reordering fallbacks did not change the hash")
	}
	if got := ba.Normalize().Fallbacks; got[0] != b || got[1] != a {
		t.Errorf("Normalize reordered fallbacks to %v", got)
	}
	messy := valid()
	messy.Fallbacks = []agentspec.ModelRef{{Provider: " OpenAI ", Model: " gpt-5\n"}, b}
	if messy.Hash() != ab.Hash() {
		t.Error("case and whitespace in a fallback changed the hash")
	}
	if c := messy.Clone(); &c.Fallbacks[0] == &messy.Fallbacks[0] {
		t.Error("Clone shares the fallback list")
	}
}

// Each fallback is held to the primary's rules, the list is bounded, and a
// fallback that repeats a model already in the chain is refused.
func TestFallbacksAreValidatedLikeThePrimary(t *testing.T) {
	t.Parallel()
	ref := func(provider, model string) agentspec.ModelRef {
		return agentspec.ModelRef{Provider: provider, Model: model}
	}
	for _, tc := range []struct {
		name      string
		fallbacks []agentspec.ModelRef
		want      string // empty means valid
	}{
		{"none", nil, ""},
		{"one on another provider", []agentspec.ModelRef{ref("openai", "gpt-5")}, ""},
		{"another model on the same provider", []agentspec.ModelRef{ref("anthropic", "claude-sonnet-5")}, ""},
		{"the most allowed", []agentspec.ModelRef{ref("openai", "a"), ref("openai", "b"), ref("ollama", "c"), ref("offline", "d")}, ""},
		{"one too many", []agentspec.ModelRef{ref("openai", "a"), ref("openai", "b"), ref("ollama", "c"), ref("offline", "d"), ref("offline", "e")}, "limit is 4"},
		{"no provider", []agentspec.ModelRef{ref(" ", "gpt-5")}, "fallback 1 names no provider"},
		{"no model", []agentspec.ModelRef{ref("openai", "gpt-5"), ref("ollama", "")}, "fallback 2 (provider \"ollama\") names no model"},
		{"an unknown provider", []agentspec.ModelRef{ref("gpt-9", "x")}, "fallback 1 names provider \"gpt-9\""},
		{"the primary again", []agentspec.ModelRef{ref("Anthropic", "claude-haiku-5-5")}, "repeats the primary model"},
		{"a repeat of a fallback", []agentspec.ModelRef{ref("openai", "gpt-5"), ref("ollama", "x"), ref("OPENAI", " gpt-5")}, "fallback 3 (openai \"gpt-5\") repeats fallback 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := valid()
			s.Fallbacks = tc.fallbacks
			err := s.Validate()
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("Validate: %v", err)
			case tc.want != "" && !errors.Is(err, agentspec.ErrInvalid):
				t.Fatalf("Validate returned %v, want ErrInvalid", err)
			case tc.want != "" && !strings.Contains(err.Error(), tc.want):
				t.Fatalf("error %q does not say %q", err, tc.want)
			}
		})
	}
}

// A deployment's own provider table governs fallbacks exactly as it governs
// the primary.
func TestFallbacksUseTheDeploymentsProviderTable(t *testing.T) {
	t.Parallel()
	s := valid()
	s.Fallbacks = []agentspec.ModelRef{{Provider: "bedrock", Model: "m"}}
	if err := s.Validate(); !errors.Is(err, agentspec.ErrInvalid) {
		t.Fatalf("built-in table accepted provider bedrock: %v", err)
	}
	if err := s.ValidateWith(append(agentspec.KnownProviders(), "bedrock")); err != nil {
		t.Fatalf("a table naming bedrock refused it: %v", err)
	}
}
