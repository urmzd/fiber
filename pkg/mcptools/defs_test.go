package mcptools_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/toolprovider"
	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

// pydanticSchema is the shape pydantic emits for a tool taking a nested
// model: shared shapes in $defs, used by plain, list, Optional and
// described references, one definition referring to another, and one unused.
var pydanticSchema = map[string]any{
	"type":     "object",
	"title":    "CreateOrderArgs",
	"required": []any{"customer", "lines"},
	"$defs": map[string]any{
		"Address": map[string]any{"type": "object", "title": "Address", "required": []any{"city"}, "properties": map[string]any{
			"city": map[string]any{"type": "string"},
			"zip":  map[string]any{"anyOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "null"}}},
		}},
		"Customer": map[string]any{"type": "object", "description": "Who orders", "properties": map[string]any{
			"name":    map[string]any{"type": "string"},
			"address": map[string]any{"$ref": "#/$defs/Address"},
		}},
		"Line": map[string]any{"type": "object", "properties": map[string]any{
			"sku": map[string]any{"type": "string"},
			"qty": map[string]any{"type": "integer"},
		}},
		"Kind":   map[string]any{"type": "string", "enum": []any{"retail", "wholesale"}},
		"Unused": map[string]any{"allOf": []any{}},
	},
	"properties": map[string]any{
		"customer": map[string]any{"$ref": "#/$defs/Customer"},
		"lines":    map[string]any{"type": "array", "items": map[string]any{"$ref": "#/$defs/Line"}},
		"ship_to":  map[string]any{"anyOf": []any{map[string]any{"$ref": "#/$defs/Address"}, map[string]any{"type": "null"}}},
		"kind":     map[string]any{"$ref": "#/$defs/Kind", "description": "Order kind", "default": "retail"},
	},
}

func definitionOf(t *testing.T, schema map[string]any) (saigetypes.ParameterSchema, error) {
	t.Helper()
	ctx := context.Background()
	store := toolprovider.NewMemory()
	f := newFake()
	f.srv.AddTool(&mcp.Tool{Name: "subject", Description: "Schema under test", InputSchema: schema}, noop)
	s := connectInMemory(t, f, provider(store, t))
	snap := accept(t, store, s, 0, map[string]toolprovider.Class{"subject": toolprovider.ClassRead})
	ts, err := s.Toolset(ctx, snap, toolprovider.GrantRead)
	if err != nil {
		return saigetypes.ParameterSchema{}, err
	}
	tool, err := ts.Lookup("fake__subject")
	if err != nil {
		t.Fatal(err)
	}
	return tool.Definition().Parameters, nil
}

func TestLocalDefsReferencesAreInlinedWithTheSchemaTheyName(t *testing.T) {
	params, err := definitionOf(t, pydanticSchema)
	if err != nil {
		t.Fatalf("a pydantic-style $defs schema was refused: %v", err)
	}
	p := params.Properties

	cust := p["customer"]
	if cust.Type != "object" || cust.Description != "Who orders" || cust.Properties["name"].Type != "string" {
		t.Errorf("customer = %+v, want the Customer definition", cust)
	}
	addr := cust.Properties["address"]
	if addr.Type != "object" || addr.Properties["city"].Type != "string" || !slices.Equal(addr.Required, []string{"city"}) {
		t.Errorf("customer.address = %+v, want the Address definition, reached through Customer", addr)
	}
	if zip := addr.Properties["zip"]; zip.Type != "string" || !zip.Nullable {
		t.Errorf("customer.address.zip = %+v, want a nullable string", zip)
	}
	if it := p["lines"].Items; it == nil || it.Type != "object" || it.Properties["qty"].Type != "integer" {
		t.Errorf("lines items = %+v, want the Line definition", it)
	}
	if ship := p["ship_to"]; ship.Type != "object" || !ship.Nullable || ship.Properties["city"].Type != "string" {
		t.Errorf("ship_to = %+v, want a nullable Address", ship)
	}
	kind := p["kind"]
	if kind.Type != "string" || !slices.Equal(kind.Enum, []string{"retail", "wholesale"}) {
		t.Errorf("kind = %+v, want the Kind enum", kind)
	}
	if kind.Description != "Order kind" || kind.Default != "retail" {
		t.Errorf("kind = %+v, want the keywords beside $ref kept", kind)
	}
	if !slices.Equal(params.Required, []string{"customer", "lines"}) {
		t.Errorf("required = %v", params.Required)
	}
}

func TestADefinitionUsedTwiceIsCopiedNotShared(t *testing.T) {
	params, err := definitionOf(t, map[string]any{
		"type":  "object",
		"$defs": map[string]any{"N": map[string]any{"anyOf": []any{map[string]any{"type": "number"}, map[string]any{"type": "null"}}}},
		"properties": map[string]any{
			"a": map[string]any{"$ref": "#/$defs/N", "description": "first"},
			"b": map[string]any{"$ref": "#/$defs/N", "description": "second"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	a, b := params.Properties["a"], params.Properties["b"]
	if a.Description != "first" || b.Description != "second" || a.Type != "number" || b.Type != "number" || !a.Nullable || !b.Nullable {
		t.Errorf("a = %+v, b = %+v, want two independent nullable numbers", a, b)
	}
}

func TestAnUnresolvableReferenceIsRefusedWithItsReason(t *testing.T) {
	str := map[string]any{"type": "string"}
	for name, tc := range map[string]struct {
		schema map[string]any
		want   string
	}{
		"not_local": {map[string]any{"type": "object", "properties": map[string]any{
			"v": map[string]any{"$ref": "https://example.com/s.json"},
		}}, `$.v uses $ref "https://example.com/s.json"`},
		"definitions_keyword": {map[string]any{"type": "object", "definitions": map[string]any{"s": str}, "properties": map[string]any{
			"v": map[string]any{"$ref": "#/definitions/s"},
		}}, "only local #/$defs/<name> references are resolved"},
		"missing": {map[string]any{"type": "object", "$defs": map[string]any{"a": str}, "properties": map[string]any{
			"v": map[string]any{"$ref": "#/$defs/b"},
		}}, "names no definition in $defs (have a)"},
		"no_defs": {map[string]any{"type": "object", "properties": map[string]any{
			"v": map[string]any{"$ref": "#/$defs/b"},
		}}, "names no definition in $defs (have none)"},
		"into_a_definition": {map[string]any{"type": "object", "$defs": map[string]any{"a": map[string]any{"type": "object", "properties": map[string]any{"x": str}}}, "properties": map[string]any{
			"v": map[string]any{"$ref": "#/$defs/a/properties/x"},
		}}, "names no definition"},
		"self": {recursiveSchema, "recursive (node -> node)"},
		"mutual": {map[string]any{"type": "object", "$defs": map[string]any{
			"A": map[string]any{"type": "object", "properties": map[string]any{"b": map[string]any{"$ref": "#/$defs/B"}}},
			"B": map[string]any{"type": "object", "properties": map[string]any{"a": map[string]any{"$ref": "#/$defs/A"}}},
		}, "properties": map[string]any{"v": map[string]any{"$ref": "#/$defs/A"}}}, "recursive (A -> B -> A)"},
		"not_a_string": {map[string]any{"type": "object", "properties": map[string]any{
			"v": map[string]any{"$ref": 7},
		}}, "not a string"},
		"defs_not_an_object": {map[string]any{"type": "object", "$defs": []any{}}, "$.$defs is not an object"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := definitionOf(t, tc.schema)
			if err == nil {
				t.Fatal("an unresolvable reference was accepted")
			}
			if !errors.Is(err, mcptools.ErrUnsupportedSchema) {
				t.Errorf("err = %v, want ErrUnsupportedSchema", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want it to say %q", err, tc.want)
			}
		})
	}
}

func TestADefinitionThatExpandsWithoutBoundIsRefused(t *testing.T) {
	// Each level references the one below twice: 2^12 expansions from a
	// dozen definitions.
	defs := map[string]any{"L12": map[string]any{"type": "string"}}
	for i := 11; i >= 0; i-- {
		next := "#/$defs/L" + strconv.Itoa(i+1)
		defs["L"+strconv.Itoa(i)] = map[string]any{"type": "object", "properties": map[string]any{
			"a": map[string]any{"$ref": next}, "b": map[string]any{"$ref": next},
		}}
	}
	_, err := definitionOf(t, map[string]any{"type": "object", "$defs": defs, "properties": map[string]any{"v": map[string]any{"$ref": "#/$defs/L0"}}})
	if !errors.Is(err, mcptools.ErrUnsupportedSchema) || !strings.Contains(err.Error(), "expands more than") {
		t.Fatalf("err = %v, want the expansion refused as unbounded", err)
	}
}

func TestARefusedSchemaIsLoggedWithTheToolAndTheReason(t *testing.T) {
	ctx := context.Background()
	store := toolprovider.NewMemory()
	f := newFake()
	f.srv.AddTool(&mcp.Tool{Name: "tree", Description: "Recursive", InputSchema: recursiveSchema}, noop)
	f.srv.AddTool(&mcp.Tool{Name: "order", Description: "Pydantic", InputSchema: pydanticSchema}, noop)
	var logs strings.Builder
	var mu sync.Mutex
	ct, st := mcp.NewInMemoryTransports()
	ss, err := f.srv.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	s, err := mcptools.Connect(ctx, provider(store, t), mcptools.WithTransport(ct), mcptools.WithLogger(slogTo(&logs, &mu)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	snap := accept(t, store, s, 0, map[string]toolprovider.Class{"order": toolprovider.ClassRead})
	if _, err := s.Toolset(ctx, snap, toolprovider.GrantRead); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := logs.String()
	mu.Unlock()
	if !strings.Contains(got, `"msg":"mcptools: inlined $defs"`) || !strings.Contains(got, `"tool":"order"`) || !strings.Contains(got, `"refs":5`) {
		t.Errorf("no debug record of the inlining for order:\n%s", got)
	}

	snap = accept(t, store, s, snap.Version, map[string]toolprovider.Class{"order": toolprovider.ClassRead, "tree": toolprovider.ClassRead})
	if _, err := s.Toolset(ctx, snap, toolprovider.GrantRead); err == nil {
		t.Fatal("a recursive schema was accepted")
	}
	mu.Lock()
	got = logs.String()
	mu.Unlock()
	for _, want := range []string{`"level":"WARN"`, `"msg":"mcptools: input schema refused"`, `"tool":"tree"`, `"provider":"fake"`, `recursive (node -> node)`, `"msg":"mcptools: refused input schema"`} {
		if !strings.Contains(got, want) {
			t.Errorf("log lacks %s:\n%s", want, got)
		}
	}
}
