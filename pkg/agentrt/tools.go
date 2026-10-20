package agentrt

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/connector"
)

// Tools resolves the names in Spec.Tools to executable tools.
//
// A spec grants tools by NAME, and names are resolved per deployment: the same
// agent definition runs in a process with a Jira connector and in one without,
// and which of those is an error is a deployment decision, not a definition
// one. Lookup therefore returns an error rather than a bool, so the message can
// say what was missing.
type Tools interface {
	Lookup(name string) (saigetypes.Tool, error)
}

// ToolsFunc adapts a function into Tools.
type ToolsFunc func(name string) (saigetypes.Tool, error)

// Lookup implements Tools.
func (f ToolsFunc) Lookup(name string) (saigetypes.Tool, error) { return f(name) }

// ErrNoSuchTool reports a spec naming a tool this deployment has not loaded.
var ErrNoSuchTool = fmt.Errorf("%w: no such tool", ErrInvalid)

// ToolGroupPrefix marks an entry of Spec.Tools as a tool group rather than a
// tool: "group:github" grants every tool in the group named github.
const ToolGroupPrefix = "group:"

// ErrNoSuchToolGroup reports a spec granting a group this deployment cannot
// resolve.
var ErrNoSuchToolGroup = fmt.Errorf("%w: no such tool group", ErrInvalid)

// ToolGroups is the optional face of a Tools catalog that also resolves tool
// groups. Group returns the names of the tools in the group, each of which
// Lookup resolves.
//
// # When a group is expanded
//
// A group is expanded when a Runner is built, against the catalog that
// Runner is given, exactly as a single tool name is resolved then. The spec,
// its hash and its pinned revision record the group, not its members, so a
// deployment that adds an action to a connector adds it to every agent
// granted that connector's group on the next run. That is the point of a
// group, and also its cost: the tool definitions are the first bytes of the
// cached prefix, so such a deployment change invalidates the prompt cache for
// those agents once. Grant tools one by one when the set must not move.
//
// A tool granted both by name and through a group is granted once.
type ToolGroups interface {
	Group(name string) ([]string, error)
}

// ConnectorTools resolves tool names against the actions every registered
// connector contributes, and binds each one to a target.
//
// The target is where the action lands: the surface the tag came from, or a
// route's destination. It is bound here rather than passed by the model,
// because an action's target is an authorization fact — the tag arrived on that
// surface — and a model that could name its own target could post anywhere the
// credential reaches.
//
// after is called for every action that runs, successfully or not. pkg/runtime
// passes a function that publishes envelope.KindActionTaken, which is what puts
// "the work got done" on the bus next to the conversation.
func ConnectorTools(reg *connector.Registry, target address.Address, after func(payload.Action) error) Tools {
	return ConnectorToolsForRun(reg, target, "", after)
}

// ConnectorToolsForRun also binds the durable run identity for actions whose
// authorization must not be replayed from another run.
//
// The catalog it returns also resolves tool groups (see ToolGroups): the group
// "group:<connector>" is every action that connector contributes.
func ConnectorToolsForRun(reg *connector.Registry, target address.Address, runID string, after func(payload.Action) error) Tools {
	return &connectorTools{reg: reg, target: target, runID: runID, after: after}
}

type connectorTools struct {
	reg    *connector.Registry
	target address.Address
	runID  string
	after  func(payload.Action) error
}

func (c *connectorTools) Lookup(name string) (saigetypes.Tool, error) {
	if c.reg == nil {
		return nil, fmt.Errorf("%w: %q (no connector registry configured)", ErrNoSuchTool, name)
	}
	for _, a := range c.reg.Actions() {
		if a.Name == name {
			invoke := a.Invoke
			if invoke != nil {
				runID := c.runID
				a.Invoke = func(ctx context.Context, target address.Address, args json.RawMessage) (connector.Result, error) {
					return invoke(connector.WithRunID(ctx, runID), target, args)
				}
			}
			return ActionTool(a, c.target, c.after)
		}
	}
	return nil, fmt.Errorf("%w: %q is not contributed by any registered connector %v", ErrNoSuchTool, name, c.reg.Names())
}

// Group implements ToolGroups: a group is named after a connector, and its
// members are the actions that connector contributes.
func (c *connectorTools) Group(name string) ([]string, error) {
	if c.reg == nil {
		return nil, fmt.Errorf("%w: %q (no connector registry configured)", ErrNoSuchToolGroup, name)
	}
	return ConnectorGroup(c.reg, name)
}

// ConnectorGroup returns the names of the actions the connector called name
// contributes, sorted. A connector that is not registered, or that
// contributes no actions, is ErrNoSuchToolGroup: a grant that grants nothing
// is a mistake to report, not an empty set to accept.
func ConnectorGroup(reg *connector.Registry, name string) ([]string, error) {
	conn, err := reg.Get(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %q is not a registered connector %v", ErrNoSuchToolGroup, name, reg.Names())
	}
	actor, ok := conn.(connector.Actor)
	if !ok || len(actor.Actions()) == 0 {
		return nil, fmt.Errorf("%w: connector %q contributes no actions", ErrNoSuchToolGroup, name)
	}
	out := make([]string, 0, len(actor.Actions()))
	for _, a := range actor.Actions() {
		out = append(out, a.Name)
	}
	sort.Strings(out)
	return out, nil
}

// ConnectorGroups returns the tool group grant of every registered connector
// that contributes actions, "group:<connector>", sorted. A host lists them
// next to the action names when it reports what a spec may grant.
func ConnectorGroups(reg *connector.Registry) []string {
	var out []string
	for _, name := range reg.Names() {
		if _, err := ConnectorGroup(reg, name); err == nil {
			out = append(out, ToolGroupPrefix+name)
		}
	}
	return out
}

// StaticTools resolves names against a fixed set, for deployments (and tests)
// whose tools are not connector actions.
func StaticTools(tools ...saigetypes.Tool) Tools {
	byName := make(map[string]saigetypes.Tool, len(tools))
	for _, t := range tools {
		byName[t.Definition().Name] = t
	}
	return ToolsFunc(func(name string) (saigetypes.Tool, error) {
		t, ok := byName[name]
		if !ok {
			names := make([]string, 0, len(byName))
			for n := range byName {
				names = append(names, n)
			}
			sort.Strings(names)
			return nil, fmt.Errorf("%w: %q, have %v", ErrNoSuchTool, name, names)
		}
		return t, nil
	})
}

// ActionTool wraps a connector's native verb as a tool the model can call.
//
// connector.Action describes its arguments as JSON Schema (what a connector
// author writes and what every other tool protocol speaks), and saige's
// ParameterSchema decodes that JSON directly. See parameterSchema for the
// checks applied on top.
func ActionTool(a connector.Action, target address.Address, after func(payload.Action) error) (saigetypes.Tool, error) {
	schema, err := parameterSchema(a.Schema)
	if err != nil {
		return nil, fmt.Errorf("agentrt: action %q: %w", a.Name, err)
	}
	if a.Invoke == nil {
		return nil, fmt.Errorf("%w: action %q has no Invoke", ErrInvalid, a.Name)
	}
	return &actionTool{action: a, target: target, after: after, def: saigetypes.ToolDef{
		Name:        a.Name,
		Description: a.Description,
		Parameters:  schema,
	}}, nil
}

type actionTool struct {
	action connector.Action
	target address.Address
	after  func(payload.Action) error
	def    saigetypes.ToolDef
}

func (t *actionTool) Definition() saigetypes.ToolDef { return t.def }

// Execute invokes the connector action and reports it.
//
// A failed action returns its error to the model as text rather than as a Go
// error, so the agent can react (apologize, try a different verb, ask a human)
// instead of the whole turn dying on one refused API call. The report goes out
// either way: an attempted action that failed is exactly as auditable as one
// that worked.
func (t *actionTool) Execute(ctx context.Context, args map[string]any) (string, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("agentrt: action %q: marshal arguments: %w", t.action.Name, err)
	}
	res, invokeErr := t.action.Invoke(ctx, t.target, raw)

	rec := payload.Action{
		Name:    t.action.Name,
		Target:  t.target.String(),
		Summary: res.Summary,
		Address: res.Address.String(),
		Data:    res.Data,
	}
	if invokeErr != nil {
		rec.Error = invokeErr.Error()
	}
	if t.after != nil {
		if err := t.after(rec); err != nil {
			return "", fmt.Errorf("agentrt: action %q: report: %w", t.action.Name, err)
		}
	}
	if invokeErr != nil {
		return "action failed: " + invokeErr.Error(), nil
	}
	if res.Summary == "" {
		return "done", nil
	}
	return res.Summary, nil
}

// schemaRefs captures the JSON Schema keyword ParameterSchema does not model
// and that would change a schema's meaning if dropped.
type schemaRefs struct {
	Defs map[string]json.RawMessage `json:"$defs,omitempty"`
}

// parameterSchema decodes a JSON Schema object into saige's form. An empty
// schema is legal and means "no arguments"; anything that is not an object is
// rejected, since a tool call's arguments are always a named set.
func parameterSchema(raw json.RawMessage) (saigetypes.ParameterSchema, error) {
	if len(raw) == 0 {
		return saigetypes.ParameterSchema{Type: "object"}, nil
	}
	var s saigetypes.ParameterSchema
	if err := json.Unmarshal(raw, &s); err != nil {
		return saigetypes.ParameterSchema{}, fmt.Errorf("schema is not JSON Schema: %w", err)
	}
	var refs schemaRefs
	if err := json.Unmarshal(raw, &refs); err != nil {
		return saigetypes.ParameterSchema{}, fmt.Errorf("schema is not JSON Schema: %w", err)
	}
	if s.Type == "" {
		s.Type = "object"
	}
	if s.Type != "object" {
		return saigetypes.ParameterSchema{}, fmt.Errorf("%w: schema type %q, want object", ErrInvalid, s.Type)
	}
	if len(refs.Defs) > 0 {
		// $ref resolution is not implemented, and a tool whose arguments
		// depend on unresolved refs would be shown to the model with holes
		// in it. Refusing names the limitation at build time.
		return saigetypes.ParameterSchema{}, fmt.Errorf("%w: schema uses $defs/$ref, which is not supported", ErrInvalid)
	}
	return s, nil
}
