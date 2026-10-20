package runtime_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/urmzd/playscape/pkg/controlplane"
	"github.com/urmzd/playscape/pkg/metrics"
	"github.com/urmzd/playscape/pkg/node/inproc"
	"github.com/urmzd/playscape/pkg/tool"
	"github.com/urmzd/playscape/pkg/workspace"
	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/runtime"
)

type identityActor struct{ seen string }

func (*identityActor) Name() string { return "identity" }
func (a *identityActor) Actions() []connector.Action {
	return []connector.Action{{Name: "identity_read", Schema: json.RawMessage(`{"type":"object"}`), Invoke: func(ctx context.Context, _ address.Address, _ json.RawMessage) (connector.Result, error) {
		a.seen = connector.RunID(ctx)
		return connector.Result{Summary: a.seen}, nil
	}}}
}

func TestCatalogBindsTheActualRunOverCallerContextAndSourceParameters(t *testing.T) {
	actor := &identityActor{}
	reg := connector.NewRegistry()
	if err := reg.Register(actor); err != nil {
		t.Fatal(err)
	}
	catalog, err := runtime.Connectors(reg)(runtime.Request{RunID: "actual-run", Source: "identity://acme/subject?write_run=forged-source"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tool, err := catalog.Lookup("identity_read")
	if err != nil {
		t.Fatal(err)
	}
	result, err := tool.Execute(connector.WithRunID(context.Background(), "forged-context"), map[string]any{})
	if err != nil || result != "actual-run" || actor.seen != "actual-run" {
		t.Fatalf("result %q seen %q error %v", result, actor.seen, err)
	}
}

// A spec granting a connector's tool group reaches that connector's actions
// through the sandbox, which wraps the deployment catalog for every turn.
func TestAToolGroupResolvesThroughTheSandbox(t *testing.T) {
	ws, err := workspace.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tools := tool.NewRegistry()
	plane := controlplane.NewMemory(inproc.NewFactory(tools, ws), metrics.Nop())
	history := newSpecs()
	lead := history.revise("lead", "Lead.")
	lead.Spec.Tools = []string{"group:identity"}

	reg := connector.NewRegistry()
	if err := reg.Register(&identityActor{}); err != nil {
		t.Fatal(err)
	}
	s, err := runtime.NewSandbox(plane, tools, history,
		runtime.WithCatalog(runtime.Connectors(reg)),
		runtime.WithAgentOptions(scripted(map[string][][]saigetypes.Delta{
			"lead": {
				agenttest.ToolCallResponse("call-1", "identity_read", map[string]any{}),
				agenttest.TextResponse("done"),
			},
		})))
	if err != nil {
		t.Fatal(err)
	}
	out, chunks, err := runLead(t, s, lead)
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "done" {
		t.Fatalf("answer = %q", out.Text)
	}
	if done := toolResult(t, chunks); done.Result != "run-1" || done.Error != "" {
		t.Fatalf("tool result = %+v, want identity_read to run under run-1", done)
	}
}
