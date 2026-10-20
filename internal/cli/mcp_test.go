package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// The composition root wires the tool provider store into the sandbox: an
// agent granted "mcp:issues" is offered the provider's accepted read tools as
// "issues__<tool>", its
// model's call reaches the MCP server with the credential the provider's
// reference resolves to, and the answer comes back to the model.
//
// With $FIBER_POSTGRES_TEST_URL set it runs again on Postgres, where the store
// is migrated at startup alongside the registry and the ledger.
func TestTheCoreExpandsAnMCPGrantAgainstItsToolProviderStore(t *testing.T) {
	t.Run("memory", func(t *testing.T) { coreExpandsAnMCPGrant(t, "", "acme") })
	if url := os.Getenv("FIBER_POSTGRES_TEST_URL"); url != "" {
		t.Run("postgres", func(t *testing.T) {
			coreExpandsAnMCPGrant(t, url, fmt.Sprintf("acme-%d", time.Now().UnixNano()))
		})
	}
}

func coreExpandsAnMCPGrant(t *testing.T, postgres, tenant string) {
	const secret = "mcp-credential-value"
	var mu sync.Mutex
	var mcpAuth []string
	srv := mcp.NewServer(&mcp.Implementation{Name: "issues", Version: "1"}, nil)
	srv.AddTool(&mcp.Tool{Name: "search", Description: "Search issues", InputSchema: map[string]any{"type": "object"}},
		func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			mu.Lock()
			mcpAuth = append(mcpAuth, req.Extra.Header.Get("Authorization"))
			mu.Unlock()
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "3 open issues"}}}, nil
		})
	mcpTS := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil))
	t.Cleanup(mcpTS.Close)

	// The model: an OpenAI-compatible endpoint that calls the expanded tool
	// once and then answers with what it was told.
	var bodies []string
	modelTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		n := len(bodies)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if n == 1 {
			fmt.Fprint(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"issues__search","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`+"\n\n")
		} else {
			fmt.Fprint(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"three"},"finish_reason":"stop"}]}`+"\n\n")
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(modelTS.Close)
	t.Setenv("OPENAI_BASE_URL", modelTS.URL)
	t.Setenv("OPENAI_API_KEY", "k")
	t.Setenv("ISSUES_TOKEN", secret)

	c, err := newCore(coreConfig{Tenant: tenant, Workspace: t.TempDir(), Postgres: postgres})
	if err != nil {
		t.Fatalf("newCore: %v", err)
	}
	t.Cleanup(func() { _ = c.close() })
	ctx, cancel := context.WithCancel(context.Background())

	// An admin registers the provider and accepts its tool.
	p, err := c.tools.PutProvider(ctx, toolprovider.Provider{Tenant: tenant, Name: "issues",
		Transport: toolprovider.TransportHTTP, Endpoint: mcpTS.URL, CredentialRef: "env:ISSUES_TOKEN"})
	if err != nil {
		t.Fatal(err)
	}
	s, err := mcptools.Connect(ctx, p, mcptools.WithResolver(mcptools.EnvResolver()))
	if err != nil {
		t.Fatal(err)
	}
	offered, err := s.Offered(ctx)
	_ = s.Close()
	if err != nil {
		t.Fatal(err)
	}
	offered[0].Class = toolprovider.ClassRead
	if _, err := c.tools.Accept(ctx, tenant, "issues", 0, offered, "admin"); err != nil {
		t.Fatal(err)
	}

	box := &inbox{}
	if err := c.sinks.Register(box); err != nil {
		t.Fatal(err)
	}
	var g group
	c.startDelivery(ctx, &g)
	c.startExecution(ctx, &g)
	t.Cleanup(func() {
		cancel()
		_ = g.wait(10 * time.Second)
	})
	spec := agentspec.AgentSpec{Name: "triage", Model: "m", Provider: agentspec.ProviderOpenAI,
		SystemPrompt: "Triage.", Tools: []string{"mcp:issues"}}
	if _, err := c.store.Create(ctx, tenant, spec, "test"); err != nil {
		t.Fatal(err)
	}
	target, _ := address.Parse("inbox://" + tenant + "/out")
	if _, err := c.Invoke(ctx, envelope.Tag{ID: "Ev-1", Tenant: tenant, Agent: "triage", Origin: "test",
		Text: "how many?", Deliver: []envelope.Route{{Target: target}}, At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for !box.completed(target.String()) {
		if time.Now().After(deadline) {
			t.Fatalf("the run did not complete; delivered %d events", len(box.at(target.String())))
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 || !strings.Contains(bodies[0], `"name":"issues__search"`) {
		t.Fatalf("the model was not offered issues__search: %v", bodies)
	}
	if !strings.Contains(bodies[1], "3 open issues") {
		t.Errorf("the model was not given the MCP server's answer: %s", bodies[1])
	}
	if len(mcpAuth) != 1 || mcpAuth[0] != "Bearer "+secret {
		t.Errorf("the MCP server saw %v, want one call with the resolved credential", mcpAuth)
	}
	for _, e := range box.at(target.String()) {
		if strings.Contains(string(e.Payload), secret) {
			t.Errorf("event %s carries the credential", e.Kind)
		}
	}
}
