package mcptools_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

func noop(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return textResult("ok"), nil
}

var defsSchema = map[string]any{
	"type":       "object",
	"$defs":      map[string]any{"thing": map[string]any{"type": "string"}},
	"properties": map[string]any{"x": map[string]any{"$ref": "#/$defs/thing"}},
}

func TestAToolTheGrantDeniesCannotStopTheGrantedToolsLoading(t *testing.T) {
	ctx := context.Background()
	store := toolprovider.NewMemory()
	f := newFake()
	f.srv.AddTool(&mcp.Tool{Name: "write_defs", Description: "Write with $defs", InputSchema: defsSchema}, noop)
	s := connectInMemory(t, f, provider(store, t))
	snap := accept(t, store, s, 0, map[string]toolprovider.Class{"search": toolprovider.ClassRead, "write_defs": ""})

	ts, err := s.Toolset(ctx, snap, toolprovider.GrantRead)
	if err != nil {
		t.Fatalf("a write tool's unsupported schema broke a read-only toolset: %v", err)
	}
	if got := names(ts.All()); !slices.Equal(got, []string{"fake__search"}) {
		t.Errorf("All = %v, want only the read tool", got)
	}
	if _, err := ts.Lookup("fake__write_defs"); !errors.Is(err, mcptools.ErrDenied) {
		t.Errorf("Lookup of the denied write tool: %v, want ErrDenied", err)
	}
	if _, err := s.Toolset(ctx, snap, toolprovider.GrantWrite); err == nil {
		t.Error("a write grant converted a $defs schema it cannot represent")
	}
}

func TestANullablePropertyKeepsItsTypeAndIsMarkedNullable(t *testing.T) {
	ctx := context.Background()
	store := toolprovider.NewMemory()
	f := newFake()
	f.srv.AddTool(&mcp.Tool{Name: "nullable", Description: "Nullable fields", InputSchema: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"any_of":  map[string]any{"description": "a or null", "anyOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "null"}}},
			"one_of":  map[string]any{"oneOf": []any{map[string]any{"type": "null"}, map[string]any{"type": "integer"}}},
			"array":   map[string]any{"type": []any{"boolean", "null"}},
			"nested":  map[string]any{"type": "object", "properties": map[string]any{"inner": map[string]any{"anyOf": []any{map[string]any{"type": "number"}, map[string]any{"type": "null"}}}}},
			"listing": map[string]any{"type": "array", "items": map[string]any{"type": []any{"string", "null"}}},
		},
	}}, noop)
	s := connectInMemory(t, f, provider(store, t))
	snap := accept(t, store, s, 0, map[string]toolprovider.Class{"nullable": toolprovider.ClassRead})

	ts, err := s.Toolset(ctx, snap, toolprovider.GrantRead)
	if err != nil {
		t.Fatal(err)
	}
	tool, err := ts.Lookup("fake__nullable")
	if err != nil {
		t.Fatal(err)
	}
	props := tool.Definition().Parameters.Properties
	want := map[string]string{"any_of": "string", "one_of": "integer", "array": "boolean"}
	for name, typ := range want {
		p := props[name]
		if p.Type != typ || !p.Nullable {
			t.Errorf("%s = {type %q nullable %v}, want {type %q nullable true}", name, p.Type, p.Nullable, typ)
		}
	}
	if d := props["any_of"].Description; d != "a or null" {
		t.Errorf("any_of lost its description: %q", d)
	}
	if in := props["nested"].Properties["inner"]; in.Type != "number" || !in.Nullable {
		t.Errorf("nested.inner = %+v, want nullable number", in)
	}
	if it := props["listing"].Items; it == nil || it.Type != "string" || !it.Nullable {
		t.Errorf("listing items = %+v, want nullable string", it)
	}
}

func TestAnUnsupportedUnionIsRefusedNotDropped(t *testing.T) {
	for name, prop := range map[string]map[string]any{
		"any_of_two_types": {"anyOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "integer"}}},
		"type_array":       {"type": []any{"string", "integer"}},
		"all_of":           {"allOf": []any{map[string]any{"type": "string"}}},
		"ref":              {"$ref": "#/elsewhere"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := toolprovider.NewMemory()
			f := newFake()
			f.srv.AddTool(&mcp.Tool{Name: "union", Description: "Union", InputSchema: map[string]any{
				"type": "object", "properties": map[string]any{"v": prop},
			}}, noop)
			s := connectInMemory(t, f, provider(store, t))
			snap := accept(t, store, s, 0, map[string]toolprovider.Class{"union": toolprovider.ClassRead})
			ts, err := s.Toolset(ctx, snap, toolprovider.GrantRead)
			if err == nil {
				var p saigetypes.PropertyDef
				if tool, lerr := ts.Lookup("fake__union"); lerr == nil {
					p = tool.Definition().Parameters.Properties["v"]
				}
				t.Fatalf("an unsupported schema was accepted as %+v", p)
			}
			if !strings.Contains(err.Error(), "not supported") && !strings.Contains(err.Error(), "no single type") {
				t.Errorf("err = %v, want it to say the schema is not supported", err)
			}
		})
	}
}

func TestServerSuppliedToolNamesAreRedactedInErrors(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	leaky := "leak_" + token
	f.srv.AddTool(&mcp.Tool{Name: leaky, Description: "Name carries the credential", InputSchema: defsSchema}, noop)
	url, _ := httpFake(t, f)
	s, err := mcptools.Connect(ctx, httpProvider(url+"/mcp"), mcptools.WithResolver(resolver(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	offered, err := s.Offered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var tools []toolprovider.Tool
	for _, o := range offered {
		if o.Name == leaky || o.Name == "search" {
			o.Class = toolprovider.ClassRead
			tools = append(tools, o)
		}
	}
	snap := toolprovider.Snapshot{Tenant: tenant, Provider: "fake", Version: 1, Tools: tools}

	// The leaky tool's schema cannot be converted, so the error names it.
	_, err = s.Toolset(ctx, snap, toolprovider.GrantRead)
	if err == nil {
		t.Fatal("Toolset accepted a $defs schema")
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("the credential came back in a schema error: %v", err)
	}
	if !strings.Contains(err.Error(), mcptools.Redacted) {
		t.Errorf("err = %v, want the credential replaced by %s", err, mcptools.Redacted)
	}

	// Lookup lists the accepted names when a tool is missing.
	ts, err := s.Toolset(ctx, toolprovider.Snapshot{Tenant: tenant, Provider: "fake", Version: 1, Tools: tools[:0]}, toolprovider.GrantRead)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ts.Lookup("fake__" + leaky); err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("Lookup error = %v, want an error without the credential", err)
	}
}
