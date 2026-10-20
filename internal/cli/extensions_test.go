package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// A binary built on fiber registers a model provider, a tool source and a
// connector, and each reaches the layer it belongs to: the registry stores an
// agent naming the provider, the turn runs on it, the tool resolves, and the
// connector receives the run's events.
func TestExtensionsReachEveryLayerOfTheCore(t *testing.T) {
	lookup := &agenttest.MockTool{Def: saigetypes.ToolDef{Name: "lookup_runbook", Parameters: saigetypes.ParameterSchema{Type: "object"}}, Result: "restart it"}
	house := func(spec agentrt.Spec) (saigetypes.Provider, error) {
		return &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{
			agenttest.ToolCallResponse("c1", "lookup_runbook", map[string]any{"service": "api"}),
			agenttest.TextResponse("house says: restart it"),
		}}, nil
	}
	box := &inbox{}
	c, err := newCore(coreConfig{Tenant: "acme", Workspace: t.TempDir(), Ext: Extensions{
		Providers: map[string]agentrt.ProviderFunc{"house": house},
		Catalogs: []runtime.Catalog{func(runtime.Request, func(payload.Action) error) (agentrt.Tools, error) {
			return agentrt.StaticTools(lookup), nil
		}},
		Connectors: []connector.Connector{box},
	}})
	if err != nil {
		t.Fatalf("newCore: %v", err)
	}
	t.Cleanup(func() { _ = c.close() })
	ctx, cancel := context.WithCancel(context.Background())
	var g group
	c.startDelivery(ctx, &g)
	c.startExecution(ctx, &g)
	t.Cleanup(func() {
		cancel()
		_ = g.wait(10 * time.Second)
	})

	spec := agentspec.AgentSpec{Name: "oncall", Model: "house-1", Provider: "house", SystemPrompt: "Help.", Tools: []string{"lookup_runbook"}}
	if _, err := c.store.Create(ctx, "acme", spec, "test"); err != nil {
		t.Fatalf("the registry refused an agent on the registered provider: %v", err)
	}
	target, _ := address.Parse("inbox://acme/out")
	if _, err := c.Invoke(ctx, envelope.Tag{ID: "Ev-1", Tenant: "acme", Agent: "oncall", Origin: "test",
		Text: "api is down", Deliver: []envelope.Route{{Target: target}}, At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for !box.completed(target.String()) {
		if time.Now().After(deadline) {
			t.Fatalf("the run did not complete; delivered %d events", len(box.at(target.String())))
		}
		time.Sleep(20 * time.Millisecond)
	}
	var text string
	var toolDone bool
	for _, e := range box.at(target.String()) {
		switch e.Kind {
		case envelope.KindText:
			text += string(e.Payload)
		case envelope.KindToolDone:
			toolDone = strings.Contains(string(e.Payload), "restart it")
		}
	}
	if !strings.Contains(text, "house says") {
		t.Errorf("delivered text %q, want the house provider's answer", text)
	}
	if !toolDone {
		t.Errorf("the extension's tool did not run")
	}
}

// fakeMCP serves an MCP server with a read tool and a write tool over HTTP.
func fakeMCP(t *testing.T) string {
	t.Helper()
	srv := mcp.NewServer(&mcp.Implementation{Name: "issues", Version: "1"}, nil)
	for _, name := range []string{"search", "close_issue"} {
		srv.AddTool(&mcp.Tool{Name: name, Description: name, InputSchema: map[string]any{"type": "object"}},
			func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
			})
	}
	ts := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil))
	t.Cleanup(ts.Close)
	return ts.URL
}

func writeFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "providers.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// The providers file is the no-Go path: it declares model endpoints, which
// specs can then name, and MCP tool providers, which are recorded with the
// tools it accepts. Starting again with the same file mints no new versions.
// With $FIBER_POSTGRES_TEST_URL set it runs again on Postgres, where the
// registry's provider table and the seeded records are the durable ones.
func TestAProvidersFileDeclaresModelsAndToolProviders(t *testing.T) {
	t.Run("memory", func(t *testing.T) { providersFileDeclares(t, "", "acme") })
	if url := os.Getenv("FIBER_POSTGRES_TEST_URL"); url != "" {
		t.Run("postgres", func(t *testing.T) {
			providersFileDeclares(t, url, fmt.Sprintf("acme-%d", time.Now().UnixNano()))
		})
	}
}

func providersFileDeclares(t *testing.T, postgres, tenant string) {
	url := fakeMCP(t)
	path := writeFile(t, `{
  "models": [{"name": "fireworks", "base_url": "https://api.fireworks.ai/inference/v1", "api_key_env": "FIREWORKS_API_KEY"}],
  "tool_providers": [{"name": "issues", "transport": "http", "endpoint": "`+url+`",
    "tools": [{"name": "search", "class": "read"}, {"name": "close_issue"}]}]
}`)
	ext, err := loadProvidersFile(path, tenant)
	if err != nil {
		t.Fatalf("loadProvidersFile: %v", err)
	}
	c, err := newCore(coreConfig{Tenant: tenant, Workspace: t.TempDir(), Postgres: postgres, Ext: ext})
	if err != nil {
		t.Fatalf("newCore: %v", err)
	}
	t.Cleanup(func() { _ = c.close() })
	ctx := context.Background()

	if _, err := c.store.Create(ctx, tenant, agentspec.AgentSpec{Name: "a", Model: "m", Provider: "fireworks", SystemPrompt: "x"}, "test"); err != nil {
		t.Errorf("an agent on the declared model endpoint was refused: %v", err)
	}
	snap, err := c.tools.Snapshot(ctx, tenant, "issues", 0)
	if err != nil {
		t.Fatal(err)
	}
	classes := map[string]toolprovider.Class{}
	for _, tool := range snap.Tools {
		classes[tool.Name] = tool.Class
	}
	if snap.Version != 1 || classes["search"] != toolprovider.ClassRead || classes["close_issue"] != toolprovider.ClassWrite {
		t.Fatalf("snapshot %d with %v, want version 1 with search read and close_issue write", snap.Version, classes)
	}

	// A second start with the same declaration changes nothing.
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := seedToolProviders(ctx, c.tools, tenant, ext.ToolProviders, mcptools.Dialer(), log); err != nil {
		t.Fatal(err)
	}
	p, _ := c.tools.GetProvider(ctx, tenant, "issues")
	again, _ := c.tools.Snapshot(ctx, tenant, "issues", 0)
	if p.Version != 1 || again.Version != 1 {
		t.Errorf("a repeated start minted provider version %d, snapshot %d; want both still 1", p.Version, again.Version)
	}
}

// A file that cannot be what its author meant stops the server at startup.
func TestAProvidersFileThatCannotBeRightIsAUsageError(t *testing.T) {
	for name, body := range map[string]string{
		"a key where its variable's name belongs": `{"models": [{"name": "fw", "api_key": "sk-123"}]}`,
		"a model with no name":                    `{"models": [{"base_url": "https://x"}]}`,
		"a model declared twice":                  `{"models": [{"name": "fw"}, {"name": "fw"}]}`,
		"a tool provider with a bad transport":    `{"tool_providers": [{"name": "x", "transport": "smtp", "endpoint": "x"}]}`,
		"not JSON":                                `models: []`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadProvidersFile(writeFile(t, body), "acme"); exitCodeFor(err) != ExitUsage {
				t.Fatalf("loadProvidersFile = %v, want a usage error", err)
			}
		})
	}
	if _, err := loadProvidersFile(filepath.Join(t.TempDir(), "missing.json"), "acme"); exitCodeFor(err) != ExitUsage {
		t.Errorf("a missing file: %v, want a usage error", err)
	}
}

// Go options and the file combine, and a name both declare is refused rather
// than one silently winning.
func TestExtensionsFromGoAndTheFileMustNotCollide(t *testing.T) {
	fn := func(agentrt.Spec) (saigetypes.Provider, error) { return nil, nil }
	goExt := Extensions{Providers: map[string]agentrt.ProviderFunc{"fw": fn}}
	if _, err := goExt.merge(Extensions{Providers: map[string]agentrt.ProviderFunc{"other": fn}}); err != nil {
		t.Fatalf("distinct names: %v", err)
	}
	if _, err := goExt.merge(Extensions{Providers: map[string]agentrt.ProviderFunc{"fw": fn}}); exitCodeFor(err) != ExitUsage {
		t.Fatalf("a name declared twice: %v, want a usage error", err)
	}
	seed := ToolProviderSeed{Provider: toolprovider.Provider{Tenant: "acme", Name: "issues"}}
	if _, err := (Extensions{ToolProviders: []ToolProviderSeed{seed}}).merge(Extensions{ToolProviders: []ToolProviderSeed{seed}}); exitCodeFor(err) != ExitUsage {
		t.Fatalf("a tool provider declared twice: %v, want a usage error", err)
	}
}
