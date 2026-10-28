package fiber

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/toolprovider"
	"github.com/urmzd/fiber/pkg/turn/native"
	"github.com/urmzd/fiber/pkg/turn/native/agenttest"
	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

// A program built on fiber registers a model provider, a tool source and a
// connector, and each reaches the layer it belongs to: the registry stores an
// agent naming the provider, the turn runs on it, the tool resolves, and the
// connector receives the run's events.
func TestExtensionsReachEveryLayerOfTheCore(t *testing.T) {
	lookup := &agenttest.MockTool{Def: saigetypes.ToolDef{Name: "lookup_runbook", Parameters: saigetypes.ParameterSchema{Type: "object"}}, Result: "restart it"}
	house := func(spec native.Spec) (saigetypes.Provider, error) {
		return &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{
			agenttest.ToolCallResponse("c1", "lookup_runbook", map[string]any{"service": "api"}),
			agenttest.TextResponse("house says: restart it"),
		}}, nil
	}
	box := &inbox{}
	c := newApp(t, WithTenant("acme"),
		WithModelProvider("house", house),
		WithToolSource(func(runtime.Request, func(event.Action) error) (native.Tools, error) {
			return native.StaticTools(lookup), nil
		}),
		WithConnector(box),
	)
	start(t, c.Run)
	ctx := context.Background()

	spec := definition.AgentSpec{Name: "oncall", Model: "house-1", Provider: "house", SystemPrompt: "Help.", Tools: []string{"lookup_runbook"}}
	if _, err := createAgent(c.Definitions(), ctx, "acme", spec, "test"); err != nil {
		t.Fatalf("the registry refused an agent on the registered provider: %v", err)
	}
	target, _ := address.Parse("inbox://acme/out")
	if _, err := ask(ctx, c, posted("Ev-1", "acme", "oncall", target.String(), "api is down")); err != nil {
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
		switch b := e.Data.(type) {
		case event.Text:
			text += b.Text
		case event.ToolDone:
			toolDone = strings.Contains(b.Result, "restart it")
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

// objectHash is the schema hash of the fake's tools, the pin a declaration
// carries for them.
func objectHash(t *testing.T) string {
	t.Helper()
	tool, err := toolprovider.NewTool("x", "", json.RawMessage(`{"type":"object"}`))
	if err != nil {
		t.Fatal(err)
	}
	return tool.SchemaHash
}

// issues declares the fake's provider and pins its two tools.
func issues(t *testing.T, url string) Option {
	h := objectHash(t)
	return WithToolProvider(
		toolprovider.Provider{Name: "issues", Transport: toolprovider.TransportHTTP, Endpoint: url},
		toolprovider.Tool{Name: "search", SchemaHash: h, Class: toolprovider.ClassRead},
		toolprovider.Tool{Name: "close_issue", SchemaHash: h},
	)
}

// Declared model endpoints can be named by specs, and declared MCP tool
// providers are recorded with the tools they pin. Starting again with the
// same declaration mints no new versions. With $FIBER_POSTGRES_TEST_URL set it
// runs again on a Postgres pool, where the records are the durable ones.
func TestDeclaredModelsAndToolProvidersReachTheStores(t *testing.T) {
	t.Run("memory", func(t *testing.T) { declaredReachTheStores(t, nil) })
	t.Run("postgres", func(t *testing.T) { declaredReachTheStores(t, testPool(t)) })
}

func declaredReachTheStores(t *testing.T, pool *pgxpool.Pool) {
	const tenant = "acme"
	_, url := fakeMCP(t)
	opts := []Option{
		WithTenant(tenant),
		WithOpenAICompatible(native.OpenAICompatible{Name: "fireworks", BaseURL: "https://api.fireworks.ai/inference/v1", KeyEnv: "FIREWORKS_API_KEY"}),
		issues(t, url),
	}
	if pool != nil {
		opts = append(opts, WithPostgres(pool))
	}
	c := newApp(t, opts...)
	ctx := context.Background()

	if _, err := createAgent(c.Definitions(), ctx, tenant, definition.AgentSpec{Name: "a", Model: "m", Provider: "fireworks", SystemPrompt: "x"}, "test"); err != nil {
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

	// What was accepted is what "mcp:issues" grants: the read tool alone,
	// and with ":write" the write tool as well.
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
	if err := seedToolProviders(ctx, c.ToolProviders(), tenant, apply([]Option{issues(t, url)}).ToolProviders, mcptools.Dialer(), log); err != nil {
		t.Fatal(err)
	}
	p, _ := c.ToolProviders().GetProvider(ctx, tenant, "issues")
	again, _ := c.ToolProviders().Snapshot(ctx, tenant, "issues", 0)
	if p.Version != 1 || again.Version != 1 {
		t.Errorf("a repeated start minted provider version %d, snapshot %d; want both still 1", p.Version, again.Version)
	}
}

// A declared tool is accepted only as pinned, so a server cannot widen what
// agents may do by changing under a restart. Here the server adds a write
// tool and changes the schema of a declared one; after a restart neither is
// accepted, both are logged as held back, and the untouched tool still works.
// With $FIBER_POSTGRES_TEST_URL set the restart is a new App on the same
// database.
func TestARestartNeverAcceptsWhatTheDeclarationDoesNot(t *testing.T) {
	t.Run("memory", func(t *testing.T) { restartHoldsBackDrift(t, nil) })
	t.Run("postgres", func(t *testing.T) { restartHoldsBackDrift(t, testPool(t)) })
}

func restartHoldsBackDrift(t *testing.T, pool *pgxpool.Pool) {
	const tenant = "acme"
	ctx := context.Background()
	srv, url := fakeMCP(t)
	opts := []Option{WithTenant(tenant), issues(t, url)}
	if pool != nil {
		opts = append(opts, WithPostgres(pool))
	}
	first, err := New(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	seeds := apply(opts).ToolProviders

	// The server drifts: a new write tool, and search takes a new argument.
	srv.AddTool(&mcp.Tool{Name: "drop_db", Description: "drop_db", InputSchema: map[string]any{"type": "object"}}, okTool)
	srv.AddTool(&mcp.Tool{Name: "search", Description: "search", InputSchema: map[string]any{
		"type": "object", "properties": map[string]any{"delete": map[string]any{"type": "boolean"}},
	}}, okTool)

	// The restart.
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	store := first.ToolProviders()
	if pool != nil {
		_ = first.Close()
		again := newApp(t, append(opts, WithLogger(log))...)
		store = again.ToolProviders()
	} else if err := seedToolProviders(ctx, store, tenant, seeds, mcptools.Dialer(), log); err != nil {
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
	if err := seedToolProviders(ctx, store, tenant, seeds, mcptools.Dialer(), log); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Snapshot(ctx, tenant, "issues", 0); got.Version != 1 {
		t.Errorf("a reworded description was accepted as snapshot %d", got.Version)
	}
	if !strings.Contains(logs.String(), "description changed") {
		t.Errorf("the reworded description was not logged:\n%s", logs.String())
	}
}

// A person-scoped provider has no credential of its own, so a start never
// dials it, with anyone's: a declared tool carrying its whole form is
// accepted as declared, one pinned by hash alone (or whose schema does not
// match its pin) stays pending and is logged, and a repeated start mints
// nothing. Changing the record's scope is an edit like any other.
func TestAPersonScopedProviderIsSeededFromItsDeclarationWithoutDialling(t *testing.T) {
	const tenant = "acme"
	ctx := context.Background()
	h := objectHash(t)
	full := toolprovider.Tool{Name: "search", Description: "search", InputSchema: json.RawMessage(`{"type": "object"}`), SchemaHash: h, Class: toolprovider.ClassRead}
	hashOnly := toolprovider.Tool{Name: "close_issue", SchemaHash: h}
	wrong := toolprovider.Tool{Name: "reopen", Description: "reopen", InputSchema: json.RawMessage(`{"type":"object","properties":{"x":{"type":"string"}}}`), SchemaHash: h}
	// Nothing listens here: a dial would fail, and the seed would log it.
	person := toolprovider.Provider{Name: "issues", Transport: toolprovider.TransportHTTP, Endpoint: "http://127.0.0.1:1/mcp", CredentialScope: toolprovider.ScopePerson}
	dialled := 0
	dial := func(context.Context, toolprovider.Provider) (*mcptools.Session, error) {
		dialled++
		return nil, errors.New("dialled")
	}
	store := toolprovider.NewMemory()
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	seeds := []toolProviderSeed{{Provider: person, Accept: []toolprovider.Tool{full, hashOnly, wrong}}}
	for range 2 {
		if err := seedToolProviders(ctx, store, tenant, seeds, dial, log); err != nil {
			t.Fatal(err)
		}
	}
	if dialled != 0 {
		t.Fatalf("a person-scoped provider was dialled %d times at startup", dialled)
	}
	snap, err := store.Snapshot(ctx, tenant, "issues", 0)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Version != 1 || len(snap.Tools) != 1 || snap.Tools[0].Name != "search" || snap.Tools[0].Class != toolprovider.ClassRead || snap.Tools[0].Description != "search" {
		t.Fatalf("snapshot = %+v, want version 1 with only the fully declared search, as read", snap)
	}
	for _, want := range []string{"tool=close_issue", "tool=reopen", "does not match its schema_hash"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the seed did not log %q:\n%s", want, logs.String())
		}
	}

	// Moving it back to a shared credential is a new record version.
	shared := person
	shared.CredentialScope, shared.CredentialRef = toolprovider.ScopeShared, "env:ISSUES_TOKEN"
	if err := seedToolProviders(ctx, store, tenant, []toolProviderSeed{{Provider: shared}}, dial, log); err != nil {
		t.Fatal(err)
	}
	if p, _ := store.GetProvider(ctx, tenant, "issues"); p.Version != 2 || p.Scope() != toolprovider.ScopeShared {
		t.Errorf("after moving back to shared the record is %+v", p)
	}
}
