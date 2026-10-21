package fiberapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	c := newApp(t, Config{Tenant: "acme"},
		WithModelProvider("house", house),
		WithToolSource(func(runtime.Request, func(payload.Action) error) (agentrt.Tools, error) {
			return agentrt.StaticTools(lookup), nil
		}),
		WithConnector(box),
	)
	start(t, c.Run)
	ctx := context.Background()

	spec := agentspec.AgentSpec{Name: "oncall", Model: "house-1", Provider: "house", SystemPrompt: "Help.", Tools: []string{"lookup_runbook"}}
	if _, err := c.Registry().Create(ctx, "acme", spec, "test"); err != nil {
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

// okTool answers every call with "ok".
func okTool(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
}

// fakeMCP serves an MCP server with a read tool and a write tool over HTTP.
// The server is returned so a test can change what it offers.
func fakeMCP(t *testing.T) (*mcp.Server, string) {
	t.Helper()
	srv := mcp.NewServer(&mcp.Implementation{Name: "issues", Version: "1"}, nil)
	for _, name := range []string{"search", "close_issue"} {
		srv.AddTool(&mcp.Tool{Name: name, Description: name, InputSchema: map[string]any{"type": "object"}}, okTool)
	}
	ts := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil))
	t.Cleanup(ts.Close)
	return srv, ts.URL
}

// objectHash is the schema hash of the fake's tools, the pin a providers file
// carries for them.
func objectHash(t *testing.T) string {
	t.Helper()
	tool, err := toolprovider.NewTool("x", "", json.RawMessage(`{"type":"object"}`))
	if err != nil {
		t.Fatal(err)
	}
	return tool.SchemaHash
}

// issuesFile is a providers file pinning the fake's two tools.
func issuesFile(t *testing.T, url string) string {
	t.Helper()
	h := objectHash(t)
	return writeFile(t, `{
  "models": [{"name": "fireworks", "base_url": "https://api.fireworks.ai/inference/v1", "api_key_env": "FIREWORKS_API_KEY"}],
  "tool_providers": [{"name": "issues", "transport": "http", "endpoint": "`+url+`",
    "tools": [{"name": "search", "schema_hash": "`+h+`", "class": "read"}, {"name": "close_issue", "schema_hash": "`+h+`"}]}]
}`)
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
	_, url := fakeMCP(t)
	file := issuesFile(t, url)
	ext, err := loadProvidersFile(file, tenant)
	if err != nil {
		t.Fatalf("loadProvidersFile: %v", err)
	}
	c := newApp(t, Config{Tenant: tenant, Postgres: postgres, ProvidersFile: file})
	ctx := context.Background()

	if _, err := c.Registry().Create(ctx, tenant, agentspec.AgentSpec{Name: "a", Model: "m", Provider: "fireworks", SystemPrompt: "x"}, "test"); err != nil {
		t.Errorf("an agent on the declared model endpoint was refused: %v", err)
	}
	snap, err := c.ToolProviders().Snapshot(ctx, tenant, "issues", 0)
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

	// What the file accepted is what "mcp:issues" grants: the read tool
	// alone, and with ":write" the write tool as well.
	grants := mcptools.NewGrants(c.ToolProviders(), mcptools.Dialer())
	t.Cleanup(func() { _ = grants.Close() })
	for grant, want := range map[string]string{"mcp:issues": "issues__search", "mcp:issues:write": "issues__close_issue,issues__search"} {
		tools, ok, err := grants.Expand(ctx, tenant, grant)
		if !ok || err != nil {
			t.Fatalf("Expand(%q) = %v, %v", grant, ok, err)
		}
		var names []string
		for _, tool := range tools {
			names = append(names, tool.Definition().Name)
		}
		if got := strings.Join(names, ","); got != want {
			t.Errorf("%s offers %s, want %s", grant, got, want)
		}
	}

	// A second start with the same declaration changes nothing.
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := seedToolProviders(ctx, c.ToolProviders(), tenant, ext.ToolProviders, mcptools.Dialer(), log); err != nil {
		t.Fatal(err)
	}
	p, _ := c.ToolProviders().GetProvider(ctx, tenant, "issues")
	again, _ := c.ToolProviders().Snapshot(ctx, tenant, "issues", 0)
	if p.Version != 1 || again.Version != 1 {
		t.Errorf("a repeated start minted provider version %d, snapshot %d; want both still 1", p.Version, again.Version)
	}
}

// A file that cannot be what its author meant fails at startup.
func TestAProvidersFileThatCannotBeRightIsAUsageError(t *testing.T) {
	for name, body := range map[string]string{
		"a key where its variable's name belongs": `{"models": [{"name": "fw", "api_key": "sk-123"}]}`,
		"a model with no name":                    `{"models": [{"base_url": "https://x"}]}`,
		"a model declared twice":                  `{"models": [{"name": "fw", "base_url": "http://a"}, {"name": "fw", "base_url": "http://b"}]}`,
		"a key in the base URL":                   `{"models": [{"name": "fw", "base_url": "https://u:sk-123@x/v1"}]}`,
		"OpenAI itself with no key variable":      `{"models": [{"name": "fw"}]}`,
		"a tool provider with a bad transport":    `{"tool_providers": [{"name": "x", "transport": "smtp", "endpoint": "x"}]}`,
		"a tool with no schema_hash pin":          `{"tool_providers": [{"name": "x", "transport": "http", "endpoint": "https://x", "tools": [{"name": "search"}]}]}`,
		"not JSON":                                `models: []`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadProvidersFile(writeFile(t, body), "acme"); !errors.Is(err, ErrConfig) {
				t.Fatalf("loadProvidersFile = %v, want a usage error", err)
			}
		})
	}
	if _, err := loadProvidersFile(filepath.Join(t.TempDir(), "missing.json"), "acme"); !errors.Is(err, ErrConfig) {
		t.Errorf("a missing file: %v, want a usage error", err)
	}
}

// Go options and the file combine, and a name both declare is refused rather
// than one silently winning.
func TestExtensionsFromGoAndTheFileMustNotCollide(t *testing.T) {
	fn := func(agentrt.Spec) (saigetypes.Provider, error) { return nil, nil }
	goExt := extensions{Providers: map[string]agentrt.ProviderFunc{"fw": fn}}
	if _, err := goExt.merge(extensions{Providers: map[string]agentrt.ProviderFunc{"other": fn}}); err != nil {
		t.Fatalf("distinct names: %v", err)
	}
	if _, err := goExt.merge(extensions{Providers: map[string]agentrt.ProviderFunc{"fw": fn}}); !errors.Is(err, ErrConfig) {
		t.Fatalf("a name declared twice: %v, want a usage error", err)
	}
	seed := toolProviderSeed{Provider: toolprovider.Provider{Tenant: "acme", Name: "issues"}}
	if _, err := (extensions{ToolProviders: []toolProviderSeed{seed}}).merge(extensions{ToolProviders: []toolProviderSeed{seed}}); !errors.Is(err, ErrConfig) {
		t.Fatalf("a tool provider declared twice: %v, want a usage error", err)
	}
}

// A declared tool is accepted only as pinned, so a server cannot widen what
// agents may do by changing under a restart. Here the server adds a write
// tool and changes the schema of a declared one; after a restart neither is
// accepted, both are logged as held back, and the untouched tool still works.
// With $FIBER_POSTGRES_TEST_URL set the restart is a new process's core on
// the same database.
func TestARestartNeverAcceptsWhatTheProvidersFileDoesNotDeclare(t *testing.T) {
	t.Run("memory", func(t *testing.T) { restartHoldsBackDrift(t, "", "acme") })
	if url := os.Getenv("FIBER_POSTGRES_TEST_URL"); url != "" {
		t.Run("postgres", func(t *testing.T) {
			restartHoldsBackDrift(t, url, fmt.Sprintf("acme-%d", time.Now().UnixNano()))
		})
	}
}

func restartHoldsBackDrift(t *testing.T, postgres, tenant string) {
	ctx := context.Background()
	srv, url := fakeMCP(t)
	file := issuesFile(t, url)
	ext, err := loadProvidersFile(file, tenant)
	if err != nil {
		t.Fatal(err)
	}
	first, err := New(ctx, Config{Tenant: tenant, Workspace: t.TempDir(), Postgres: postgres, ProvidersFile: file})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })

	// The server drifts: a new write tool, and search takes a new argument.
	srv.AddTool(&mcp.Tool{Name: "drop_db", Description: "drop_db", InputSchema: map[string]any{"type": "object"}}, okTool)
	srv.AddTool(&mcp.Tool{Name: "search", Description: "search", InputSchema: map[string]any{
		"type": "object", "properties": map[string]any{"delete": map[string]any{"type": "boolean"}},
	}}, okTool)

	// The restart.
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	store := first.ToolProviders()
	if postgres != "" {
		_ = first.Close()
		again := newApp(t, Config{Tenant: tenant, Postgres: postgres, ProvidersFile: file, Log: log})
		store = again.ToolProviders()
	} else if err := seedToolProviders(ctx, store, tenant, ext.ToolProviders, mcptools.Dialer(), log); err != nil {
		t.Fatal(err)
	}

	snap, err := store.Snapshot(ctx, tenant, "issues", 0)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Version != 1 {
		t.Errorf("the restart accepted snapshot %d; want the first, 1, unchanged", snap.Version)
	}
	if _, ok := snap.Tool("drop_db"); ok {
		t.Error("the restart accepted drop_db, which the file does not declare")
	}
	if got, _ := snap.Tool("search"); got.SchemaHash != objectHash(t) {
		t.Errorf("the restart accepted search's new schema %s", got.SchemaHash)
	}
	for _, want := range []string{"tool=drop_db", "offered but not declared", "tool=search", "schema does not match"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the restart did not log %q:\n%s", want, logs.String())
		}
	}

	// What an agent granted the whole provider is offered now.
	grants := mcptools.NewGrants(store, mcptools.Dialer())
	t.Cleanup(func() { _ = grants.Close() })
	tools, _, err := grants.Expand(ctx, tenant, "mcp:issues:write")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tool := range tools {
		names = append(names, tool.Definition().Name)
	}
	if strings.Join(names, ",") != "issues__close_issue" {
		t.Errorf("a write grant is offered %v, want only the undrifted issues__close_issue", names)
	}

	// A reworded description is drift too: it changes what the model is
	// told the tool does, under the same schema.
	srv.AddTool(&mcp.Tool{Name: "close_issue", Description: "close and delete the issue", InputSchema: map[string]any{"type": "object"}}, okTool)
	logs.Reset()
	if err := seedToolProviders(ctx, store, tenant, ext.ToolProviders, mcptools.Dialer(), log); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Snapshot(ctx, tenant, "issues", 0); got.Version != 1 {
		t.Errorf("a reworded description was accepted as snapshot %d", got.Version)
	}
	if !strings.Contains(logs.String(), "description changed") {
		t.Errorf("the reworded description was not logged:\n%s", logs.String())
	}
}
