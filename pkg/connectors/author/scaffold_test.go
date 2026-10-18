package author_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/connectors/author"
	"github.com/urmzd/fiber/pkg/registry"
)

func system() author.System {
	return author.System{
		Router: author.Role{Name: "Front-Desk", Role: "Receives every request"},
		Specialists: []author.Role{
			{Name: "support", Role: "Resolves account and billing problems.", Tools: []string{"jira_comment"}},
			{Name: "triage", Role: "Classifies and prioritizes incoming bug reports"},
			{Name: "docs", Role: "Answers how-to questions from the documentation."},
		},
		Model:    "offline",
		Provider: agentspec.ProviderOffline,
	}
}

// The delegation graph a scaffold grants is a star: the router reaches the
// specialists and nothing else, and no specialist reaches anything.
func TestAScaffoldGrantsTheRouterItsSpecialistsAndNothingElse(t *testing.T) {
	t.Parallel()
	specs, err := author.Scaffold(system())
	if err != nil {
		t.Fatalf("Scaffold: %v", err)
	}
	if len(specs) != 4 {
		t.Fatalf("%d definitions, want 3 specialists and a router", len(specs))
	}
	router := specs[len(specs)-1]
	if router.Name != "front-desk" {
		t.Fatalf("the router is not last, or not canonical: %q", router.Name)
	}
	if want := []string{"docs", "support", "triage"}; !reflect.DeepEqual(router.Access.Spawn, want) {
		t.Errorf("router may spawn %v, want exactly %v", router.Access.Spawn, want)
	}
	if len(router.Access.WorkspaceAreas) != 0 || len(router.Tools) != 0 {
		t.Errorf("router was granted more than delegation: %+v tools %v", router.Access, router.Tools)
	}
	for _, s := range specs[:3] {
		if !reflect.DeepEqual(s.Access, agentspec.Access{}) {
			t.Errorf("specialist %s was granted access %+v, want none", s.Name, s.Access)
		}
		if !strings.Contains(router.SystemPrompt, "tag_"+s.Name) {
			t.Errorf("the router's prompt never says how to reach %s", s.Name)
		}
	}
	if got := specs[0].Tools; !reflect.DeepEqual(got, []string{"jira_comment"}) {
		t.Errorf("support's tools = %v, want what was asked for", got)
	}
}

// A scaffold that returns can be stored, in the order returned, and reviews
// clean once it is.
func TestAScaffoldIsAcceptedByTheRegistryAndPassesReview(t *testing.T) {
	t.Parallel()
	specs, err := author.Scaffold(system())
	if err != nil {
		t.Fatalf("Scaffold: %v", err)
	}
	store := registry.NewMemory()
	for _, s := range specs {
		// Stored before review, so a router is reviewed against specialists
		// that exist: the order Scaffold returns is the order that works.
		rev, err := store.Create(context.Background(), "acme", s, "test")
		if err != nil {
			t.Fatalf("the registry refused %s: %v", s.Name, err)
		}
		if rev.Hash != s.Hash() {
			t.Errorf("%s was not canonical: the registry stored different content", s.Name)
		}
		revs, _, err := store.List(context.Background(), "acme", registry.Page{})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		world := author.World{}
		for _, r := range revs {
			world.Agents = append(world.Agents, r.Spec)
		}
		if got := author.Review(s, world); len(got) != 0 {
			t.Errorf("%s has findings: %+v", s.Name, got)
		}
	}
}

func TestAScaffoldThatCannotBeStoredIsRefused(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		edit func(*author.System)
	}{
		{"no specialists", func(s *author.System) { s.Specialists = nil }},
		{"two specialists with one name", func(s *author.System) { s.Specialists[1].Name = "Support" }},
		{"a specialist named like the router", func(s *author.System) { s.Specialists[0].Name = "front-desk" }},
		{"a specialist with no role", func(s *author.System) { s.Specialists[0].Role = " " }},
		{"a router with no name", func(s *author.System) { s.Router.Name = "" }},
		{"a name that is not addressable", func(s *author.System) { s.Specialists[0].Name = "sup:port" }},
		{"a provider the deployment does not know", func(s *author.System) { s.Provider = "nonesuch" }},
		{"no model", func(s *author.System) { s.Model = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sys := system()
			tc.edit(&sys)
			if specs, err := author.Scaffold(sys); !errors.Is(err, author.ErrInvalid) || specs != nil {
				t.Fatalf("Scaffold = %v, %v; want nothing and ErrInvalid", specs, err)
			}
		})
	}
	sys := system()
	sys.Provider = "in-house"
	if _, err := author.ScaffoldWith(sys, []string{"in-house"}); err != nil {
		t.Errorf("ScaffoldWith the deployment's own provider table: %v", err)
	}
}
