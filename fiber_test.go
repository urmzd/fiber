package fiber

import (
	"testing"

	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/toolprovider"
	"github.com/urmzd/fiber/pkg/turn/native"
	"github.com/urmzd/fiber/pkg/turn/native/agenttest"
	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

type named string

func (n named) Name() string { return string(n) }

// Every option lands in the field of the composition root's extensions it
// names, and nothing else; a nil or empty one adds nothing.
func TestOptionsBecomeTheCompositionRootsExtensions(t *testing.T) {
	fn := func(native.Spec) (saigetypes.Provider, error) { return &agenttest.ScriptedProvider{}, nil }
	ext := apply([]Option{
		WithModelProvider("house", fn),
		WithModelProvider("", fn),
		WithModelProvider("nil", nil),
		WithOpenAICompatible(native.OpenAICompatible{Name: "fireworks", BaseURL: "https://api.fireworks.ai/inference/v1"}),
		WithTools(&agenttest.MockTool{Def: saigetypes.ToolDef{Name: "lookup"}}),
		WithToolSource(nil),
		WithConnector(named("chat")),
		WithConnector(nil),
		WithToolProvider(toolprovider.Provider{Name: "issues"}, toolprovider.Tool{Name: "search", SchemaHash: "abc", Class: toolprovider.ClassRead}),
		nil,
	})
	if len(ext.Providers) != 2 || ext.Providers["house"] == nil || ext.Providers["fireworks"] == nil {
		t.Errorf("providers %v, want house and fireworks", ext.Providers)
	}
	if len(ext.Catalogs) != 1 {
		t.Fatalf("%d catalogs, want the one WithTools added", len(ext.Catalogs))
	}
	tools, err := ext.Catalogs[0](runtime.Request{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tools.Lookup("lookup"); err != nil {
		t.Errorf("WithTools' tool does not resolve: %v", err)
	}
	if len(ext.Connectors) != 1 || ext.Connectors[0].Name() != "chat" {
		t.Errorf("connectors %v, want chat", ext.Connectors)
	}
	if len(ext.ToolProviders) != 1 || ext.ToolProviders[0].Provider.Name != "issues" || len(ext.ToolProviders[0].Accept) != 1 {
		t.Errorf("tool providers %+v, want issues with search", ext.ToolProviders)
	}
}
