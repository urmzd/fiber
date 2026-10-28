package runtime_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/urmzd/playscape/pkg/controlplane"
	"github.com/urmzd/playscape/pkg/metrics"
	"github.com/urmzd/playscape/pkg/node/inproc"
	"github.com/urmzd/playscape/pkg/tool"
	"github.com/urmzd/playscape/pkg/workspace"
	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/toolprovider"
	"github.com/urmzd/fiber/pkg/turn/native"
)

// issuesServer is an MCP server with a read tool, a write tool and a tool
// nobody accepted. ran counts the calls that reached it, by tool.
func issuesServer(ran map[string]*atomic.Int32) *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{Name: "issues", Version: "1"}, nil)
	for name, answer := range map[string]string{"search": "3 open issues", "close_issue": "closed", "drop_db": "dropped"} {
		ran[name] = &atomic.Int32{}
		srv.AddTool(&mcp.Tool{Name: name, Description: name, InputSchema: map[string]any{"type": "object"}},
			func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				ran[name].Add(1)
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: answer}}}, nil
			})
	}
	return srv
}

// groupsSandbox builds a sandbox whose tenant acme has an "issues" MCP
// provider with search accepted as read and close_issue as write, supplied
// to turns through WithToolGroupSource.
func groupsSandbox(t *testing.T, script map[string][][]saigetypes.Delta) (*runtime.Sandbox, *specs, map[string]*atomic.Int32) {
	t.Helper()
	ctx := context.Background()
	store := toolprovider.NewMemory()
	p, err := store.PutProvider(ctx, toolprovider.Provider{Tenant: "acme", Name: "issues", Transport: toolprovider.TransportHTTP, Endpoint: "http://127.0.0.1:1/mcp"})
	if err != nil {
		t.Fatal(err)
	}
	ran := map[string]*atomic.Int32{}
	srv := issuesServer(ran)
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
	_ = s.Close()
	if err != nil {
		t.Fatal(err)
	}
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
		runtime.WithCatalog(runtime.Connectors(nil)),
		runtime.WithToolGroupSource(grants),
		runtime.WithAgentOptions(scripted(script)))
	if err != nil {
		t.Fatal(err)
	}
	return sb, history, ran
}

func runTurnAs(t *testing.T, s *runtime.Sandbox, tenant string, rev native.Revision) (runtime.Outcome, []event.Body, error) {
	t.Helper()
	var chunks []event.Body
	out, err := s.Execute(context.Background(), withPin(runtime.Request{RunID: "run-" + rev.Spec.Name, Tenant: tenant, Attempt: 1, Text: "go"}, rev),
		func(_ context.Context, c event.Body) error {
			chunks = append(chunks, c)
			return nil
		})
	return out, chunks, err
}

// "mcp:issues" in a spec is the whole of wiring an agent to an MCP server: the
// model is offered the provider's accepted read tools as "issues__<tool>",
// calls one, and reads what the server answered.
func TestAStoreBackedGroupGrantGivesTheAgentTheProvidersTools(t *testing.T) {
	sb, history, ran := groupsSandbox(t, map[string][][]saigetypes.Delta{
		"triage": {
			agenttest.ToolCallResponse("c1", "issues__search", map[string]any{}),
			agenttest.TextResponse("There are three."),
		},
	})
	history.revise("triage", "Triage.")
	rev := history.edit("triage", func(s *native.Spec) { s.Tools = []string{native.MCPGroup("issues")} })

	out, chunks, err := runTurnAs(t, sb, "acme", rev)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out.Text != "There are three." {
		t.Errorf("answer %q", out.Text)
	}
	if done := toolResult(t, chunks); done.Result != "3 open issues" || done.Error != "" {
		t.Errorf("tool result %+v, want the MCP server's answer", done)
	}
	if ran["search"].Load() != 1 {
		t.Errorf("search ran %d times, want 1", ran["search"].Load())
	}
}

// The grant's class decides what the model may call: under a read grant the
// write tool is not offered and a call to it never reaches the server; under
// a write grant it runs. A tool the admin never accepted runs under neither.
func TestAGroupGrantsClassDecidesWhichToolsRun(t *testing.T) {
	for _, tc := range []struct {
		grant, call string
		runs        bool
	}{
		{native.MCPGroup("issues"), "issues__close_issue", false},
		{native.MCPWriteGroup("issues"), "issues__close_issue", true},
		{native.MCPWriteGroup("issues"), "issues__drop_db", false},
	} {
		t.Run(tc.grant+"/"+tc.call, func(t *testing.T) {
			sb, history, ran := groupsSandbox(t, map[string][][]saigetypes.Delta{
				"triage": {
					agenttest.ToolCallResponse("c1", tc.call, map[string]any{}),
					agenttest.TextResponse("done"),
				},
			})
			history.revise("triage", "Triage.")
			rev := history.edit("triage", func(s *native.Spec) { s.Tools = []string{tc.grant} })
			_, chunks, err := runTurnAs(t, sb, "acme", rev)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			done := toolResult(t, chunks)
			reached := ran["close_issue"].Load() + ran["drop_db"].Load()
			if tc.runs && (done.Error != "" || reached != 1) {
				t.Errorf("%s under %s did not run: %+v", tc.call, tc.grant, done)
			}
			if !tc.runs && (done.Error == "" || reached != 0) {
				t.Errorf("%s under %s reached the server: %+v", tc.call, tc.grant, done)
			}
		})
	}
}

// A read grant with an approval on the provider's write tool would read as
// guarded and grant nothing to guard; the turn refuses to build instead of
// letting the approval lapse.
func TestAnApprovalOnAToolTheGrantsClassExcludesRefusesToBuild(t *testing.T) {
	sb, history, _ := groupsSandbox(t, map[string][][]saigetypes.Delta{"triage": {agenttest.TextResponse("ok")}})
	history.revise("triage", "Triage.")
	rev := history.edit("triage", func(s *native.Spec) {
		s.Tools = []string{native.MCPGroup("issues")}
		s.Approvals = []string{"issues__close_issue"}
	})
	if _, _, err := runTurnAs(t, sb, "acme", rev); err == nil || !strings.Contains(err.Error(), "issues__close_issue") {
		t.Fatalf("Execute = %v, want the approval refused", err)
	}

	rev = history.edit("triage", func(s *native.Spec) { s.Tools = []string{native.MCPWriteGroup("issues")} })
	if _, _, err := runTurnAs(t, sb, "acme", rev); err != nil {
		t.Fatalf("under the write grant: %v", err)
	}
}

// Groups are expanded for the turn's tenant: another tenant granting the same
// provider name reaches acme's record and tools no more than it could name
// them. For globex the group does not exist, so its turn runs without it.
func TestAGroupGrantExpandsForTheTurnsTenantOnly(t *testing.T) {
	sb, history, ran := groupsSandbox(t, map[string][][]saigetypes.Delta{"triage": {agenttest.TextResponse("ok")}})
	history.revise("triage", "Triage.")
	rev := history.edit("triage", func(s *native.Spec) { s.Tools = []string{native.MCPGroup("issues")} })
	if _, _, err := runTurnAs(t, sb, "globex", rev); err != nil {
		t.Fatalf("globex's turn: %v, want it to run without the group", err)
	}
	if ran["search"].Load() != 0 {
		t.Fatal("globex's turn reached acme's server")
	}
}
