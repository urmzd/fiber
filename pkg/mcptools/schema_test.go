package mcptools_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/toolprovider"
	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

func noop(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return textResult("ok"), nil
}

// recursiveSchema is a schema saige cannot represent: a tree whose node
// refers to itself has no finite inline form.
var recursiveSchema = map[string]any{
	"type": "object",
	"$defs": map[string]any{"node": map[string]any{"type": "object", "properties": map[string]any{
		"children": map[string]any{"type": "array", "items": map[string]any{"$ref": "#/$defs/node"}},
	}}},
	"properties": map[string]any{"root": map[string]any{"$ref": "#/$defs/node"}},
}

func TestAToolTheGrantDeniesCannotStopTheGrantedToolsLoading(t *testing.T) {
	ctx := context.Background()
	store := toolprovider.NewMemory()
	f := newFake()
	f.srv.AddTool(&mcp.Tool{Name: "write_defs", Description: "Write with a recursive schema", InputSchema: recursiveSchema}, noop)
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
		t.Error("a write grant converted a recursive schema it cannot represent")
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
	f.srv.AddTool(&mcp.Tool{Name: leaky, Description: "Name carries the credential", InputSchema: recursiveSchema}, noop)
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
		t.Fatal("Toolset accepted a recursive schema")
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

// TestNoErrorCarriesTheCredential drives every error path an exported
// function has with a tool whose name is the credential, and checks that no
// error text contains it.
func TestNoErrorCarriesTheCredential(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	leaky := "leak_" + token
	f.srv.AddTool(&mcp.Tool{Name: leaky, Description: "fails", InputSchema: map[string]any{"type": "object"}},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			r := textResult("failure")
			r.IsError = true
			return r, nil
		})
	url, _ := httpFake(t, f)
	s, err := mcptools.Connect(ctx, httpProvider(url+"/mcp"), mcptools.WithResolver(resolver(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var errs []error
	check := func(where string, err error) {
		t.Helper()
		if err == nil {
			t.Errorf("%s: want an error", where)
			return
		}
		errs = append(errs, err)
		if strings.Contains(err.Error(), token) {
			t.Errorf("%s leaked the credential: %v", where, err)
		}
	}

	offered, err := s.Offered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var tools []toolprovider.Tool
	for _, o := range offered {
		if o.Name == leaky {
			tools = append(tools, o) // no class: a write tool
		}
	}
	snap := toolprovider.Snapshot{Tenant: tenant, Provider: "fake", Version: 1, Tools: tools}

	read, err := s.Toolset(ctx, snap, toolprovider.GrantRead)
	if err != nil {
		t.Fatal(err)
	}
	_, err = read.Lookup("fake__" + leaky)
	check("Lookup of a denied tool", err)
	_, err = read.Lookup("fake__missing_" + token)
	check("Lookup of a missing tool", err)

	write, err := s.Toolset(ctx, snap, toolprovider.GrantWrite)
	if err != nil {
		t.Fatal(err)
	}
	tool, err := write.Lookup("fake__" + leaky)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tool.Execute(ctx, nil)
	check("Execute of a failing tool", err)
	if !errors.Is(err, mcptools.ErrToolFailed) {
		t.Errorf("Execute: %v, want ErrToolFailed", err)
	}

	// The server rewords the tool: the call is held back, and says so
	// without the name.
	f.srv.AddTool(&mcp.Tool{Name: leaky, Description: "reworded", InputSchema: map[string]any{"type": "object"}}, noop)
	_, err = tool.Execute(ctx, nil)
	check("Execute of a changed tool", err)
	if !errors.Is(err, mcptools.ErrHeldBack) {
		t.Errorf("Execute: %v, want ErrHeldBack", err)
	}

	_, err = s.Toolset(ctx, toolprovider.Snapshot{Tenant: tenant, Provider: "other_" + token}, toolprovider.GrantRead)
	check("Toolset of another provider's snapshot", err)
	if len(errs) != 5 {
		t.Errorf("checked %d errors, want 5", len(errs))
	}
}

func TestARootCompositionIsRefused(t *testing.T) {
	for name, schema := range map[string]map[string]any{
		"all_of": {"type": "object", "allOf": []any{map[string]any{"type": "object", "required": []any{"a"}, "properties": map[string]any{"a": map[string]any{"type": "string"}}}}},
		"any_of": {"type": "object", "anyOf": []any{map[string]any{"type": "object"}, map[string]any{"type": "object", "properties": map[string]any{"b": map[string]any{"type": "string"}}}}},
		"ref":    {"type": "object", "$ref": "#/elsewhere"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := toolprovider.NewMemory()
			f := newFake()
			f.srv.AddTool(&mcp.Tool{Name: "root", Description: "Root composition", InputSchema: schema}, noop)
			s := connectInMemory(t, f, provider(store, t))
			snap := accept(t, store, s, 0, map[string]toolprovider.Class{"root": toolprovider.ClassRead})
			if _, err := s.Toolset(ctx, snap, toolprovider.GrantRead); err == nil || !strings.Contains(err.Error(), "not supported") {
				t.Fatalf("Toolset = %v, want the root schema refused as not supported", err)
			}
		})
	}
}
