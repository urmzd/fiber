package mcptools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/toolprovider"
)

// Toolset is the callable view of one provider for one grant: the accepted
// tools the server still offers unchanged, minus those the grant does not
// permit. Its Lookup has the shape of pkg/agentrt's Tools interface.
type Toolset struct {
	session *Session
	grant   toolprovider.Grant
	diff    toolprovider.Diff
	byName  map[string]*callable
	denied  map[string]toolprovider.Tool
	names   []string
}

// Toolset lists the server's tools, compares them with accepted and returns
// the callable view for grant. accepted must be a snapshot of this session's
// provider.
func (s *Session) Toolset(ctx context.Context, accepted toolprovider.Snapshot, grant toolprovider.Grant) (*Toolset, error) {
	ts, err := s.toolset(ctx, accepted, grant)
	return ts, s.red.err(err)
}

func (s *Session) toolset(ctx context.Context, accepted toolprovider.Snapshot, grant toolprovider.Grant) (*Toolset, error) {
	if accepted.Provider != s.provider.Name || (accepted.Tenant != "" && accepted.Tenant != s.provider.Tenant) {
		return nil, fmt.Errorf("mcptools: snapshot of %s/%q given to a session on %s/%q",
			accepted.Tenant, accepted.Provider, s.provider.Tenant, s.provider.Name)
	}
	diff, err := s.diff(ctx, accepted)
	if err != nil {
		return nil, err
	}
	ts := &Toolset{session: s, grant: grant, diff: diff, byName: map[string]*callable{}, denied: map[string]toolprovider.Tool{}}
	for _, t := range diff.Unchanged {
		// A tool the grant does not permit is never converted, so its schema
		// cannot keep the tools the grant does permit from loading.
		if !grant.Permits(t.Class) {
			ts.denied[toolprovider.QualifiedName(s.provider.Name, t.Name)] = t
			continue
		}
		c, err := newCallable(s, t, grant)
		if err != nil {
			return nil, err
		}
		ts.byName[c.def.Name] = c
		ts.names = append(ts.names, c.def.Name)
	}
	slices.Sort(ts.names)
	return ts, nil
}

// Diff is the comparison this toolset was built from, for reporting which
// tools were held back.
func (ts *Toolset) Diff() toolprovider.Diff { return ts.diff }

// Lookup returns the callable with the qualified name, or ErrHeldBack for a
// tool that is not accepted as offered, or ErrDenied for one the grant does
// not permit.
func (ts *Toolset) Lookup(name string) (saigetypes.Tool, error) {
	c, err := ts.lookup(name)
	if err != nil {
		return nil, ts.session.red.err(err)
	}
	return c, nil
}

func (ts *Toolset) lookup(name string) (*callable, error) {
	if t, ok := ts.denied[name]; ok {
		return nil, fmt.Errorf("%w: %q is a %s tool and the grant is %s",
			ErrDenied, name, t.Class.Effective(), grantName(ts.grant))
	}
	c, ok := ts.byName[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q is not an accepted tool of provider %q, have %v",
			ErrHeldBack, name, ts.session.provider.Name, ts.names)
	}
	return c, nil
}

// All returns every callable the grant permits, sorted by name.
func (ts *Toolset) All() []saigetypes.Tool {
	out := []saigetypes.Tool{}
	for _, n := range ts.names {
		if c := ts.byName[n]; ts.grant.Permits(c.tool.Class) {
			out = append(out, c)
		}
	}
	return out
}

func grantName(g toolprovider.Grant) string {
	if g == toolprovider.GrantWrite {
		return "write"
	}
	return "read-only"
}

// callable is one accepted tool bound to a session and a grant.
type callable struct {
	session *Session
	tool    toolprovider.Tool
	grant   toolprovider.Grant
	def     saigetypes.ToolDef
}

func newCallable(s *Session, t toolprovider.Tool, grant toolprovider.Grant) (*callable, error) {
	params, err := parameterSchema(t.InputSchema)
	if err != nil {
		return nil, fmt.Errorf("mcptools: tool %q of %q: %w", t.Name, s.provider.Name, err)
	}
	capability := saigetypes.ToolCapabilityWrite
	if t.Class.Effective() == toolprovider.ClassRead {
		capability = saigetypes.ToolCapabilityRead
	}
	return &callable{session: s, tool: t, grant: grant, def: saigetypes.ToolDef{
		Name:        toolprovider.QualifiedName(s.provider.Name, t.Name),
		Description: t.Description,
		Parameters:  params,
		Capability:  capability,
	}}, nil
}

func (c *callable) Definition() saigetypes.ToolDef { return c.def }

// Execute checks the grant, checks the server still offers the tool exactly
// as accepted, and only then calls it.
func (c *callable) Execute(ctx context.Context, args map[string]any) (string, error) {
	out, err := c.execute(ctx, args)
	if err != nil {
		return "", c.session.red.err(err)
	}
	return c.session.red.text(out), nil
}

func (c *callable) execute(ctx context.Context, args map[string]any) (string, error) {
	if !c.grant.Permits(c.tool.Class) {
		return "", fmt.Errorf("%w: %q is a %s tool and the grant is %s",
			ErrDenied, c.def.Name, c.tool.Class.Effective(), grantName(c.grant))
	}
	offered, ok, err := c.session.current(ctx, c.tool.Name)
	if err != nil {
		return "", err
	}
	switch {
	case !ok:
		return "", fmt.Errorf("%w: %q is no longer offered by %q", ErrHeldBack, c.def.Name, c.session.provider.Name)
	case offered.Description != c.tool.Description || offered.SchemaHash != c.tool.SchemaHash:
		return "", fmt.Errorf("%w: %q changed on %q since it was accepted", ErrHeldBack, c.def.Name, c.session.provider.Name)
	}
	if args == nil {
		args = map[string]any{}
	}
	log := c.session.logger.With("tool", c.def.Name)
	res, err := c.session.cs.CallTool(ctx, &mcp.CallToolParams{Name: c.tool.Name, Arguments: args})
	if err != nil {
		// The logger scrubs the credential itself, so the raw error is safe here.
		log.Warn("mcptools: call failed", "err", err)
		return "", fmt.Errorf("mcptools: call %q: %w", c.def.Name, err)
	}
	raw := render(res)
	if res.IsError {
		log.Warn("mcptools: tool reported failure", "result", raw)
		return "", fmt.Errorf("%w: %s: %s", ErrToolFailed, c.def.Name, raw)
	}
	log.Debug("mcptools: call", "class", string(c.tool.Class.Effective()))
	return raw, nil
}

// render turns a tool result into the text a model reads. Text blocks are
// kept; anything a text-only tool result cannot carry is named instead of
// silently dropped.
func render(res *mcp.CallToolResult) string {
	var parts []string
	for _, c := range res.Content {
		switch v := c.(type) {
		case *mcp.TextContent:
			parts = append(parts, v.Text)
		case *mcp.ImageContent:
			parts = append(parts, fmt.Sprintf("[image %s omitted]", v.MIMEType))
		case *mcp.AudioContent:
			parts = append(parts, fmt.Sprintf("[audio %s omitted]", v.MIMEType))
		case *mcp.ResourceLink:
			parts = append(parts, fmt.Sprintf("[resource %s]", v.URI))
		case *mcp.EmbeddedResource:
			if v.Resource != nil && v.Resource.Text != "" {
				parts = append(parts, v.Resource.Text)
			} else if v.Resource != nil {
				parts = append(parts, fmt.Sprintf("[resource %s omitted]", v.Resource.URI))
			}
		default:
			parts = append(parts, "[unsupported content omitted]")
		}
	}
	if len(parts) == 0 && res.StructuredContent != nil {
		if b, err := json.Marshal(res.StructuredContent); err == nil {
			return string(b)
		}
	}
	return strings.Join(parts, "\n")
}

// marshalSchema renders the SDK's decoded input schema back to JSON.
func marshalSchema(schema any) (json.RawMessage, error) {
	if schema == nil {
		return nil, nil
	}
	if raw, ok := schema.(json.RawMessage); ok {
		return raw, nil
	}
	b, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("input schema: %w", err)
	}
	return b, nil
}

// parameterSchema decodes an accepted JSON schema into saige's form, with the
// same rules pkg/agentrt applies to connector actions: an object, or nothing,
// and no $defs, whose references saige cannot resolve. A nullable property,
// written as anyOf/oneOf of one type and {"type":"null"} or as a type array of
// one type and "null", becomes that type with Nullable set. Any other anyOf,
// oneOf, allOf, $ref or multi-type array is refused rather than dropped, so a
// tool is never offered to a model with a schema that means something else.
func parameterSchema(raw json.RawMessage) (saigetypes.ParameterSchema, error) {
	if len(raw) == 0 {
		return saigetypes.ParameterSchema{Type: saigetypes.SchemaObject}, nil
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return saigetypes.ParameterSchema{}, fmt.Errorf("schema is not JSON Schema: %w", err)
	}
	if defs, ok := doc["$defs"].(map[string]any); ok && len(defs) > 0 {
		return saigetypes.ParameterSchema{}, errors.New("schema uses $defs/$ref, which is not supported")
	}
	if err := normalizeNode(doc, "$", 0); err != nil {
		return saigetypes.ParameterSchema{}, err
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return saigetypes.ParameterSchema{}, fmt.Errorf("schema is not JSON Schema: %w", err)
	}
	var s saigetypes.ParameterSchema
	if err := json.Unmarshal(b, &s); err != nil {
		return saigetypes.ParameterSchema{}, fmt.Errorf("schema is not JSON Schema: %w", err)
	}
	if s.Type == "" {
		s.Type = saigetypes.SchemaObject
	}
	if s.Type != saigetypes.SchemaObject {
		return saigetypes.ParameterSchema{}, fmt.Errorf("schema type %q, want object", s.Type)
	}
	return s, nil
}

// maxSchemaDepth bounds recursion through nested properties and items.
const maxSchemaDepth = 32

// normalizeProperties rewrites node's properties and items in place, see
// parameterSchema. The root goes through normalizeNode too, so a root
// allOf, $ref or union is refused rather than read as an empty object.
func normalizeProperties(node map[string]any, path string, depth int) error {
	if depth > maxSchemaDepth {
		return fmt.Errorf("schema nests deeper than %d levels", maxSchemaDepth)
	}
	if props, ok := node["properties"].(map[string]any); ok {
		for name, v := range props {
			p, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("schema at %s is not an object", path+"."+name)
			}
			if err := normalizeNode(p, path+"."+name, depth+1); err != nil {
				return err
			}
		}
	}
	if items, ok := node["items"].(map[string]any); ok {
		if err := normalizeNode(items, path+"[]", depth+1); err != nil {
			return err
		}
	}
	return nil
}

func normalizeNode(p map[string]any, path string, depth int) error {
	if _, ok := p["$ref"]; ok {
		return fmt.Errorf("schema at %s uses $ref, which is not supported", path)
	}
	if _, ok := p["allOf"]; ok {
		return fmt.Errorf("schema at %s uses allOf, which is not supported", path)
	}
	for _, key := range []string{"anyOf", "oneOf"} {
		alts, ok := p[key]
		if !ok {
			continue
		}
		list, _ := alts.([]any)
		var only map[string]any
		nulls := 0
		for _, a := range list {
			m, ok := a.(map[string]any)
			if !ok {
				return fmt.Errorf("schema at %s: %s entry is not an object", path, key)
			}
			if t, _ := m["type"].(string); t == "null" && len(m) == 1 {
				nulls++
				continue
			}
			if only != nil {
				return fmt.Errorf("schema at %s uses %s of several types, which is not supported", path, key)
			}
			only = m
		}
		if only == nil || nulls != 1 {
			return fmt.Errorf("schema at %s uses %s other than one type or null, which is not supported", path, key)
		}
		delete(p, key)
		for k, v := range only {
			if _, set := p[k]; !set {
				p[k] = v
			}
		}
		p["nullable"] = true
	}
	if types, ok := p["type"].([]any); ok {
		var only string
		nulls := 0
		for _, t := range types {
			s, _ := t.(string)
			switch {
			case s == "null":
				nulls++
			case s != "" && only == "":
				only = s
			default:
				return fmt.Errorf("schema at %s has several types, which is not supported", path)
			}
		}
		if only == "" || nulls > 1 {
			return fmt.Errorf("schema at %s has no single type", path)
		}
		p["type"] = only
		if nulls == 1 {
			p["nullable"] = true
		}
	}
	return normalizeProperties(p, path, depth)
}
