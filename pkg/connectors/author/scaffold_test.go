package author_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/urmzd/fiber/pkg/connectors/author"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/turn"
)

// parsed compiles every scaffolded draft.
func parsed(t *testing.T, drafts []author.Draft) []definition.Agent {
	t.Helper()
	out := make([]definition.Agent, 0, len(drafts))
	for _, d := range drafts {
		a, err := definition.Parse(d.Definition, d.Policy, definition.Checks{})
		if err != nil {
			t.Fatalf("a scaffolded draft does not parse: %v\n%s", err, d.Definition)
		}
		out = append(out, a)
	}
	return out
}

func system() author.System {
	return author.System{
		Router: author.Role{Name: "Front-Desk", Role: "Receives every request"},
		Specialists: []author.Role{
			{Name: "support", Role: "Resolves account and billing problems.", Tools: []string{"jira_comment"}},
			{Name: "triage", Role: "Classifies and prioritizes incoming bug reports"},
			{Name: "docs", Role: "Answers how-to questions from the documentation."},
		},
		Model:    "offline",
		Provider: definition.ProviderOffline,
	}
}

// The delegation graph a scaffold grants is a star: the router reaches the
// specialists and nothing else, and no specialist reaches anything.
func TestAScaffoldGrantsTheRouterItsSpecialistsAndNothingElse(t *testing.T) {
	t.Parallel()
	drafts, err := author.Scaffold(system())
	if err != nil {
		t.Fatalf("Scaffold: %v", err)
	}
	if len(drafts) != 4 {
		t.Fatalf("%d drafts, want 3 specialists and a router", len(drafts))
	}
	agents := parsed(t, drafts)
	router := agents[len(agents)-1].Spec
	if router.Name != "front-desk" {
		t.Fatalf("the router is not last, or not canonical: %q", router.Name)
	}
	if want := []string{"docs", "support", "triage"}; !reflect.DeepEqual(router.Access.Spawn, want) {
		t.Errorf("router may delegate to %v, want exactly %v", router.Access.Spawn, want)
	}
	if len(router.Access.WorkspaceAreas) != 0 || len(router.Tools) != 0 {
		t.Errorf("router was granted more than delegation: %+v tools %v", router.Access, router.Tools)
	}
	for i, a := range agents[:3] {
		if !reflect.DeepEqual(a.Spec.Access, turn.Policy{}) || !reflect.DeepEqual(a.Policy, definition.Policy{}) {
			t.Errorf("specialist %s was granted access %+v policy %+v, want none", a.Spec.Name, a.Spec.Access, a.Policy)
		}
		if !strings.Contains(router.SystemPrompt, "tag_"+a.Spec.Name) {
			t.Errorf("the router's prompt never says how to reach %s", a.Spec.Name)
		}
		if a.Definition.Version != "1.0.0" || !strings.Contains(drafts[i].Definition, "apiVersion") {
			t.Errorf("specialist %s is not a 1.0.0 definition file", a.Spec.Name)
		}
	}
	if got := agents[0].Spec.Tools; !reflect.DeepEqual(got, []string{"jira_comment"}) {
		t.Errorf("support's tools = %v, want what was asked for", got)
	}
}

// A scaffold that returns can be stored, in the order returned, resolves, and
// reviews clean once it is.
func TestAScaffoldIsAcceptedByTheSourceAndPassesReview(t *testing.T) {
	t.Parallel()
	drafts, err := author.Scaffold(system())
	if err != nil {
		t.Fatalf("Scaffold: %v", err)
	}
	ctx := context.Background()
	store := definition.NewMemory()
	for i, d := range drafts {
		// Stored before review, so a router is reviewed against specialists
		// that exist: the order Scaffold returns is the order that works.
		rec, err := store.Put(ctx, definition.Record{Tenant: "acme", Content: d.Definition, Policy: d.Policy, CreatedBy: "test"})
		if err != nil {
			t.Fatalf("the source refused draft %d: %v", i, err)
		}
		if _, err := definition.NewResolver(store, definition.Checks{}).Resolve(ctx, "acme", rec.Name); err != nil {
			t.Fatalf("%s does not resolve: %v", rec.Name, err)
		}
		recs, _, err := store.List(ctx, "acme", definition.Page{})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		world := author.World{}
		var self turn.Spec
		for _, r := range recs {
			a, err := definition.Parse(r.Content, r.Policy, definition.Checks{})
			if err != nil {
				t.Fatal(err)
			}
			world.Agents = append(world.Agents, a.Spec)
			if r.Name == rec.Name {
				self = a.Spec
			}
		}
		if got := author.Review(self, world); len(got) != 0 {
			t.Errorf("%s has findings: %+v", rec.Name, got)
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
			if drafts, err := author.Scaffold(sys); !errors.Is(err, author.ErrInvalid) || drafts != nil {
				t.Fatalf("Scaffold = %v, %v; want nothing and ErrInvalid", drafts, err)
			}
		})
	}
	sys := system()
	sys.Provider = "in-house"
	if _, err := author.ScaffoldWith(sys, definition.Checks{Providers: []string{"in-house"}}); err != nil {
		t.Errorf("ScaffoldWith the deployment's own provider table: %v", err)
	}
}
