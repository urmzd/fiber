package fiber

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// BYOK end to end. A person-scoped MCP provider declared with
// WithToolProvider (accepted in full, without dialling it, since it has no
// credential of its own) is reached by each person's run with that person's
// own credential, from WithPersonCredentials: ana's held as a secret, bob's as
// an env: reference. carol has none, so her run goes ahead without the tools,
// her model is told why, and the operator log names her and the provider. The
// credentials never reach the model, an event or a log.
//
// With $FIBER_POSTGRES_TEST_URL set it runs again on a Postgres pool.
func TestEachPersonsRunReachesAPersonScopedProviderWithTheirOwnCredential(t *testing.T) {
	t.Run("memory", func(t *testing.T) { personsReachTheirOwn(t, nil) })
	t.Run("postgres", func(t *testing.T) { personsReachTheirOwn(t, testPool(t)) })
}

func personsReachTheirOwn(t *testing.T, pool *pgxpool.Pool) {
	const (
		tenant    = "acme"
		anaToken  = "ana-own-token-do-not-print"
		bobToken  = "bob-own-token-do-not-print"
		bobRef    = "env:FIBER_TEST_BOB_TOKEN"
		shared    = "shared-token-never-a-fallback"
		sharedRef = "env:FIBER_TEST_SHARED_TOKEN"
	)
	t.Setenv("FIBER_TEST_BOB_TOKEN", bobToken)
	t.Setenv("FIBER_TEST_SHARED_TOKEN", shared)

	var mu sync.Mutex
	var calls []string
	srv := mcp.NewServer(&mcp.Implementation{Name: "issues", Version: "1"}, nil)
	srv.AddTool(&mcp.Tool{Name: "search", Description: "Search issues", InputSchema: map[string]any{"type": "object"}},
		func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			auth := req.Extra.Header.Get("Authorization")
			mu.Lock()
			calls = append(calls, auth)
			mu.Unlock()
			// A server that echoes the credential back: it must reach the
			// model redacted.
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "3 open issues for " + auth}}}, nil
		})
	mcpTS := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil))
	t.Cleanup(mcpTS.Close)

	// The model calls issues__search when it is offered and answers once it
	// has a tool result, or straight away when it has no tools.
	var bodies []string
	modelTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		switch body := string(b); {
		case strings.Contains(body, `"role":"tool"`):
			fmt.Fprint(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"three"},"finish_reason":"stop"}]}`+"\n\n")
		case strings.Contains(body, `"name":"issues__search"`):
			fmt.Fprint(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"issues__search","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`+"\n\n")
		default:
			fmt.Fprint(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"I cannot search for you."},"finish_reason":"stop"}]}`+"\n\n")
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(modelTS.Close)
	t.Setenv("OPENAI_BASE_URL", modelTS.URL)
	t.Setenv("OPENAI_API_KEY", "k")

	search, err := toolprovider.NewTool("search", "Search issues", json.RawMessage(`{"type":"object"}`))
	if err != nil {
		t.Fatal(err)
	}
	search.Class = toolprovider.ClassRead

	var asked []event.Actor
	people := mcptools.PersonResolverFunc(func(_ context.Context, tn string, p toolprovider.Provider, a event.Actor) (mcptools.PersonCredential, error) {
		mu.Lock()
		asked = append(asked, a)
		mu.Unlock()
		if tn != tenant || p.Name != "issues" {
			return mcptools.PersonCredential{}, fmt.Errorf("asked about %s/%s", tn, p.Name)
		}
		switch a.User {
		case "u-ana":
			return mcptools.PersonCredential{Secret: mcptools.NewSecret(anaToken)}, nil
		case "u-bob":
			return mcptools.PersonCredential{Ref: bobRef}, nil
		}
		return mcptools.PersonCredential{}, mcptools.ErrNoPersonCredential
	})

	var logs strings.Builder
	var logMu sync.Mutex
	logger := slog.New(slog.NewJSONHandler(lockedWriter{mu: &logMu, w: &logs}, nil))
	box := &inbox{}
	opts := []Option{WithTenant(tenant), WithConnector(box), WithLogger(logger), WithPersonCredentials(people),
		WithToolProvider(toolprovider.Provider{Name: "issues", Transport: toolprovider.TransportHTTP,
			Endpoint: mcpTS.URL, CredentialScope: toolprovider.ScopePerson}, search),
		// A shared provider beside it: its credential is in reach of the
		// process and must still never reach the person-scoped one.
		WithToolProvider(toolprovider.Provider{Name: "wiki", Transport: toolprovider.TransportHTTP,
			Endpoint: "http://127.0.0.1:1/mcp", CredentialRef: sharedRef}),
	}
	if pool != nil {
		opts = append(opts, WithPostgres(pool))
	}
	c := newApp(t, opts...)
	ctx := context.Background()
	if p, err := c.ToolProviders().GetProvider(ctx, tenant, "issues"); err != nil || p.Scope() != toolprovider.ScopePerson {
		t.Fatalf("the declared provider was recorded as %+v, %v", p, err)
	}
	if snap, err := c.ToolProviders().Snapshot(ctx, tenant, "issues", 0); err != nil || len(snap.Tools) != 1 || snap.Tools[0].Name != "search" {
		t.Fatalf("the declared tool was not accepted without dialling: %+v, %v", snap, err)
	}

	start(t, c.Run)
	spec := definition.AgentSpec{Name: "triage", Model: "m", Provider: definition.ProviderOpenAI,
		SystemPrompt: "Triage.", Tools: []string{"mcp:issues"}}
	if _, err := createAgent(c.Definitions(), ctx, tenant, spec, "test"); err != nil {
		t.Fatal(err)
	}
	people3 := []event.Actor{{ID: "U-ana", User: "u-ana"}, {ID: "U-bob", User: "u-bob"}, {ID: "U-carol", User: "u-carol"}}
	for _, a := range people3 {
		target, _ := address.Parse("inbox://" + tenant + "/" + a.User)
		e := posted("Ev-"+a.User, tenant, "triage", target.String(), "how many for "+a.User+"?")
		// The request's own context claims to be ana; it is not who the
		// run acts for.
		e.Data = event.MessagePosted{Request: event.Request{
			Text: "how many for " + a.User + "?", Actor: a, Mentions: []string{"triage"},
			Context: map[string]string{"actor": "U-ana", "user": "u-ana"},
		}}
		if _, err := ask(ctx, c, e); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(20 * time.Second)
	for _, a := range people3 {
		target := "inbox://" + tenant + "/" + a.User
		for !box.completed(target) {
			if time.Now().After(deadline) {
				t.Fatalf("%s's run did not complete; delivered %d events", a.User, len(box.at(target)))
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	slices.Sort(calls)
	if want := []string{"Bearer " + anaToken, "Bearer " + bobToken}; !slices.Equal(calls, want) {
		t.Errorf("the MCP server saw calls with %v, want one with each person's own credential", calls)
	}
	for _, a := range asked {
		if a.User != "u-ana" && a.User != "u-bob" && a.User != "u-carol" {
			t.Errorf("the resolver was asked about %+v", a)
		}
	}
	runBodies := func(user string) []string {
		var out []string
		for _, b := range bodies {
			if strings.Contains(b, "how many for "+user+"?") {
				out = append(out, b)
			}
		}
		return out
	}
	for _, user := range []string{"u-ana", "u-bob"} {
		got := runBodies(user)
		if len(got) != 2 || !strings.Contains(got[1], "3 open issues for Bearer "+mcptools.Redacted) {
			t.Errorf("%s's model was not given the redacted answer: %v", user, got)
		}
	}
	carol := runBodies("u-carol")
	if len(carol) != 1 || strings.Contains(carol[0], `"name":"issues__search"`) ||
		!strings.Contains(carol[0], "they have not connected one for issues") {
		t.Errorf("carol's model was offered the tools or not told why: %v", carol)
	}
	for _, b := range bodies {
		for _, secret := range []string{anaToken, bobToken, shared} {
			if strings.Contains(b, secret) {
				t.Errorf("a credential reached the model: %s", b)
			}
		}
	}
	for _, a := range people3 {
		for _, e := range box.at("inbox://" + tenant + "/" + a.User) {
			for _, secret := range []string{anaToken, bobToken, shared} {
				if strings.Contains(bodyJSON(e), secret) {
					t.Errorf("event %s of %s's run carries a credential", e.Type, a.User)
				}
			}
		}
	}
	logMu.Lock()
	defer logMu.Unlock()
	for _, want := range []string{"mcp:issues", `actor \"U-carol\" (user \"u-carol\") has not connected one`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the operator log does not say %q:\n%s", want, logs.String())
		}
	}
	for _, secret := range []string{anaToken, bobToken, shared} {
		if strings.Contains(logs.String(), secret) {
			t.Errorf("a credential reached the log")
		}
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
