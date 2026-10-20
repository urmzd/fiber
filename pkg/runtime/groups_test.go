package runtime_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/urmzd/playscape/pkg/controlplane"
	"github.com/urmzd/playscape/pkg/metrics"
	"github.com/urmzd/playscape/pkg/node/inproc"
	"github.com/urmzd/playscape/pkg/tool"
	"github.com/urmzd/playscape/pkg/workspace"
	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// mcpServer is an MCP server with a read tool, a write tool and a tool nobody
// accepted.
func mcpServer() *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{Name: "issues", Version: "1"}, nil)
	text := func(s string) *mcp.CallToolResult {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: s}}}
	}
	for name, answer := range map[string]string{"search": "3 open issues", "close_issue": "closed", "drop_db": "dropped"} {
		srv.AddTool(&mcp.Tool{Name: name, Description: name, InputSchema: map[string]any{"type": "object"}},
			func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) { return text(answer), nil })
	}
	return srv
}

// groupsSandbox builds a sandbox whose "issues" MCP provider has search
// accepted as read and close_issue as write.
func groupsSandbox(t *testing.T, logs *bytes.Buffer, script map[string][][]saigetypes.Delta) (*runtime.Sandbox, *specs) {
	t.Helper()
	ctx := context.Background()
	store := toolprovider.NewMemory()
	p, err := store.PutProvider(ctx, toolprovider.Provider{Tenant: "acme", Name: "issues", Transport: toolprovider.TransportHTTP, Endpoint: "http://127.0.0.1:1/mcp"})
	if err != nil {
		t.Fatal(err)
	}
	srv := mcpServer()
	dial := func(ctx context.Context, p toolprovider.Provider) (*mcptools.Session, error) {
		ct, st := mcp.NewInMemoryTransports()
		ss, err := srv.Connect(ctx, st, nil)
		if err != nil {
			return nil, err
		}
		t.Cleanup(func() { _ = ss.Close() })
		return mcptools.Connect(ctx, p, mcptools.WithTransport(ct))
	}
	s, err := dial(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	offered, err := s.Offered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	var accepted []toolprovider.Tool
	for _, o := range offered {
		switch o.Name {
		case "search":
			o.Class = toolprovider.ClassRead
			accepted = append(accepted, o)
		case "close_issue":
			o.Class = toolprovider.ClassWrite
			accepted = append(accepted, o)
		}
	}
	if _, err := store.Accept(ctx, "acme", "issues", 0, accepted, "admin"); err != nil {
		t.Fatal(err)
	}
	grants := mcptools.NewGrants(store, dial)
	t.Cleanup(func() { _ = grants.Close() })

	ws, err := workspace.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tools := tool.NewRegistry()
	history := newSpecs()
	sb, err := runtime.NewSandbox(controlplane.NewMemory(inproc.NewFactory(tools, ws), metrics.Nop()), tools, history,
		runtime.WithSandboxLogger(slog.New(slog.NewTextHandler(logs, nil))),
		runtime.WithCatalog(runtime.Connectors(nil)),
		runtime.WithToolGroups(grants),
		runtime.WithAgentOptions(scripted(script)))
	if err != nil {
		t.Fatal(err)
	}
	return sb, history
}

func runTurn(t *testing.T, s *runtime.Sandbox, rev agentrt.Revision) (runtime.Outcome, []agentrt.Chunk, error) {
	t.Helper()
	var chunks []agentrt.Chunk
	out, err := s.Execute(context.Background(), runtime.Request{RunID: "run-" + rev.Spec.Name, Tenant: "acme", Attempt: 1, Text: "go", Revision: rev},
		func(_ context.Context, c agentrt.Chunk) error {
			chunks = append(chunks, c)
			return nil
		})
	return out, chunks, err
}

// "mcp:issues" in a spec is the whole of wiring an agent to an MCP server: the
// model is offered the provider's accepted read tools as "issues_<tool>",
// calls one, and reads what the server answered.
func TestAnMCPGrantGivesTheAgentTheProvidersAcceptedTools(t *testing.T) {
	var logs bytes.Buffer
	sb, history := groupsSandbox(t, &logs, map[string][][]saigetypes.Delta{
		"triage": {
			agenttest.ToolCallResponse("c1", "issues_search", map[string]any{"q": "open"}),
			agenttest.TextResponse("There are three."),
		},
	})
	rev := history.revise("triage", "Triage.")
	rev.Spec.Tools = []string{"mcp:issues"}

	out, chunks, err := runTurn(t, sb, rev)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out.Text != "There are three." {
		t.Errorf("answer %q", out.Text)
	}
	if done := toolResult(t, chunks); done.Result != "3 open issues" || done.Error != "" {
		t.Errorf("tool result %+v, want the MCP server's answer", done)
	}
}

// A read grant does not offer the write tool, and no grant offers a tool the
// admin never accepted: a model that calls one anyway gets an unknown-tool
// error and nothing reaches the server.
func TestAnMCPGrantNeverOffersHeldBackOrUngrantedTools(t *testing.T) {
	for _, name := range []string{"issues_close_issue", "issues_drop_db"} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			sb, history := groupsSandbox(t, &logs, map[string][][]saigetypes.Delta{
				"triage": {
					agenttest.ToolCallResponse("c1", name, map[string]any{}),
					agenttest.TextResponse("done"),
				},
			})
			rev := history.revise("triage", "Triage.")
			rev.Spec.Tools = []string{"mcp:issues"}
			_, chunks, err := runTurn(t, sb, rev)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if done := toolResult(t, chunks); done.Error == "" || strings.Contains(done.Result, "closed") || strings.Contains(done.Result, "dropped") {
				t.Errorf("calling %s under a read grant ran it: %+v", name, done)
			}
		})
	}
}

// A provider that cannot be expanded right now costs the agent its tools, not
// the run, as any unavailable tool does. An approval on one of its tools has
// nothing to approve and must not fail the run either.
func TestAnUnexpandableMCPGrantIsDroppedWithItsApprovals(t *testing.T) {
	var logs bytes.Buffer
	sb, history := groupsSandbox(t, &logs, map[string][][]saigetypes.Delta{
		"triage": {agenttest.TextResponse("I have no tracker here.")},
	})
	rev := history.revise("triage", "Triage.")
	rev.Spec.Tools = []string{"mcp:tracker:write", "mcp:issues:write"}
	rev.Spec.Approvals = []string{"tracker_close", "issues_close_issue", "issues_drop_db"}

	out, _, err := runTurn(t, sb, rev)
	if err != nil {
		t.Fatalf("Execute = %v, want the run to go ahead", err)
	}
	if out.Text != "I have no tracker here." {
		t.Errorf("answer %q", out.Text)
	}
	if !strings.Contains(logs.String(), "mcp:tracker:write") || !strings.Contains(logs.String(), "not found") {
		t.Errorf("the log does not report the unexpandable grant:\n%s", logs.String())
	}
}

// A sandbox without groups leaves "mcp:" grants to the catalog, which cannot
// resolve them: they are dropped like any unknown tool.
func TestWithoutToolGroupsAnMCPGrantIsAnUnavailableTool(t *testing.T) {
	ws, err := workspace.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tools := tool.NewRegistry()
	history := newSpecs()
	sb, err := runtime.NewSandbox(controlplane.NewMemory(inproc.NewFactory(tools, ws), metrics.Nop()), tools, history,
		runtime.WithCatalog(runtime.Connectors(nil)),
		runtime.WithAgentOptions(agentrt.WithStrictTools()))
	if err != nil {
		t.Fatal(err)
	}
	rev := history.revise("triage", "Triage.")
	rev.Spec.Tools = []string{"mcp:issues"}
	// The error crosses the playscape task boundary as text.
	if _, _, err := runTurn(t, sb, rev); err == nil || !strings.Contains(err.Error(), `no such tool: "mcp:issues"`) {
		t.Fatalf("a strict sandbox with no groups: %v, want no such tool", err)
	}
}
