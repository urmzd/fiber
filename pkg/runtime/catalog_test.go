package runtime_test

import (
	"context"
	"encoding/json"
	"testing"

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
