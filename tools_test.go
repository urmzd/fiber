package fiber

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/toolprovider"
	"github.com/urmzd/fiber/pkg/turn"
	"github.com/urmzd/fiber/pkg/turn/native"
)

// A listing of an agent's tools is what a run of the tag would be granted,
// for the person the tag names: ana, who has a credential of her own for a
// person-scoped provider, is shown its tools, and the server is reached with
// her credential alone; carol, who has none, is shown the grant as
// unavailable with the note her model would be told, and nothing reaches the
// server for her or for a tag that names nobody. No tool runs and no model
// is built: the agent's provider has no key here.
func TestAListingShowsEachPersonWhatTheirRunWouldBeGranted(t *testing.T) {
	const (
		tenant   = "acme"
		anaToken = "ana-own-token-do-not-print"
	)
	t.Setenv(native.EnvAnthropicKey, "")

	var mu sync.Mutex
	var seen []string
	var ran atomic.Int32
	srv := mcp.NewServer(&mcp.Implementation{Name: "issues", Version: "1"}, nil)
	srv.AddTool(&mcp.Tool{Name: "search", Description: "Search issues", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			ran.Add(1)
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "3 open issues"}}}, nil
		})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil)
	mcpTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization"))
		mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(mcpTS.Close)

	search, err := toolprovider.NewTool("search", "Search issues", json.RawMessage(`{"type":"object"}`))
	if err != nil {
		t.Fatal(err)
	}
	search.Class = toolprovider.ClassRead
	people := mcptools.PersonResolverFunc(func(_ context.Context, _ string, _ toolprovider.Provider, a event.Actor) (mcptools.PersonCredential, error) {
		if a.User == "u-ana" {
			return mcptools.PersonCredential{Secret: mcptools.NewSecret(anaToken)}, nil
		}
		return mcptools.PersonCredential{}, mcptools.ErrNoPersonCredential
	})
	app := newApp(t, WithTenant(tenant), WithPersonCredentials(people),
		WithToolProvider(toolprovider.Provider{Name: "issues", Transport: toolprovider.TransportHTTP,
			Endpoint: mcpTS.URL, CredentialScope: toolprovider.ScopePerson}, search))
	ctx := context.Background()
	if _, err := createAgent(app.Definitions(), ctx, tenant, definition.AgentSpec{
		Name: "triage", Provider: definition.ProviderAnthropic, Model: "m", SystemPrompt: "Triage.",
		Tools: []string{"mcp:issues", native.AskPersonToolName, "jira_transition"},
	}, "test"); err != nil {
		t.Fatal(err)
	}
	listFor := func(a event.Actor) native.Granted {
		t.Helper()
		e := posted("list", tenant, "triage", "inbox://"+tenant+"/out", "")
		e.Data = event.MessagePosted{Request: event.Request{Actor: a, Mentions: []string{"triage"}}}
		g, err := app.ToolsFor(ctx, e, "triage")
		if err != nil {
			t.Fatalf("ToolsFor %+v: %v", a, err)
		}
		return g
	}
	names := func(g native.Granted) (tools, unavailable []string) {
		for _, d := range g.Tools {
			tools = append(tools, d.Name)
		}
		for _, u := range g.Unavailable {
			unavailable = append(unavailable, u.Name)
		}
		return tools, unavailable
	}

	anas := listFor(event.Actor{ID: "U-ana", User: "u-ana"})
	if b, _ := json.Marshal(anas); strings.Contains(string(b), anaToken) {
		t.Errorf("ana's credential is in her listing: %s", b)
	}
	tools, missing := names(anas)
	if !slices.Equal(tools, []string{native.AskPersonToolName, "issues__search"}) || !slices.Equal(missing, []string{"jira_transition"}) {
		t.Errorf("ana is shown %v and %v unavailable, want ask_person and issues__search, and jira_transition", tools, missing)
	}
	mu.Lock()
	reached := slices.Clone(seen)
	seen = nil
	mu.Unlock()
	if len(reached) == 0 || slices.ContainsFunc(reached, func(h string) bool { return h != "Bearer "+anaToken }) {
		t.Errorf("the server was reached with %v, want ana's own credential and nothing else", reached)
	}

	for _, tc := range []struct {
		who  event.Actor
		note string
	}{
		{event.Actor{ID: "U-carol", User: "u-carol"}, "they have not connected one for issues"},
		{event.Actor{}, "this request was made for no one"},
	} {
		g := listFor(tc.who)
		tools, missing := names(g)
		if !slices.Equal(tools, []string{native.AskPersonToolName}) || !slices.Equal(missing, []string{"jira_transition", "mcp:issues"}) {
			t.Errorf("%+v is shown %v and %v unavailable, want ask_person, and jira_transition and the group", tc.who, tools, missing)
			continue
		}
		if note := g.Unavailable[1].Note; !strings.Contains(note, tc.note) {
			t.Errorf("%+v's listing gives the group the note %q, want the model's, which says %q", tc.who, note, tc.note)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 0 {
		t.Errorf("listing for a person with no credential reached the server with %v", seen)
	}
	if ran.Load() != 0 {
		t.Errorf("listing ran a tool %d times", ran.Load())
	}
}

// A turn runtime a program brings is asked for the listing only if it can
// give one; otherwise the App says it cannot rather than guessing.
func TestAListingNeedsATurnRuntimeThatCanGiveOne(t *testing.T) {
	app := newApp(t, WithTurnRuntime(turn.Func(func(context.Context, turn.Pinned, turn.Input, func(context.Context, event.Body) error) (turn.Result, error) {
		return turn.Result{}, nil
	})))
	ctx := context.Background()
	if _, err := createAgent(app.Definitions(), ctx, "acme", definition.AgentSpec{
		Name: "triage", Provider: definition.ProviderOffline, Model: "m", SystemPrompt: "Triage.",
	}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ToolsFor(ctx, posted("list", "acme", "triage", "inbox://acme/out", ""), "triage"); !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("ToolsFor = %v, want errors.ErrUnsupported", err)
	}
}
