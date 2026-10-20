package agentrt_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/agentrt"
)

// mcpTool stands in for a pkg/mcptools callable: a tool named
// "<provider>__<tool>".
type mcpTool struct{ name string }

func (t mcpTool) Definition() saigetypes.ToolDef {
	return saigetypes.ToolDef{Name: t.name, Parameters: saigetypes.ParameterSchema{Type: "object"}}
}
func (t mcpTool) Execute(context.Context, map[string]any) (string, error) {
	return "ran " + t.name, nil
}

func group(provider string, tools ...string) agentrt.ToolGroup {
	g := agentrt.ToolGroup{Name: agentrt.MCPGroup(provider)}
	for _, n := range tools {
		g.Tools = append(g.Tools, mcpTool{name: n})
	}
	return g
}

// groupCatalog resolves single tools from a fixed set and groups from a map,
// like a per-turn catalog serving a tenant's MCP providers.
type groupCatalog struct {
	agentrt.Tools
	groups map[string]agentrt.ToolGroup
}

func (c groupCatalog) ToolGroup(name string) (agentrt.ToolGroup, error) {
	g, ok := c.groups[name]
	if !ok {
		return agentrt.ToolGroup{}, fmt.Errorf("%w: %q", agentrt.ErrNoSuchToolGroup, name)
	}
	return g, nil
}

// toolsShown builds a runner, runs one turn, and returns the tool names the
// model was shown.
func toolsShown(t *testing.T, tools, approvals []string, opts ...agentrt.Option) ([]string, error) {
	t.Helper()
	var shown []string
	spy := func(agentrt.Spec) (saigetypes.Provider, error) {
		return providerFunc(func(_ context.Context, _ []saigetypes.Message, defs []saigetypes.ToolDef) (<-chan saigetypes.Delta, error) {
			for _, d := range defs {
				shown = append(shown, d.Name)
			}
			return scripted(agenttest.TextResponse("ok")), nil
		}), nil
	}
	rev := offlineRevision(1, "You help.")
	rev.Spec.Provider = "spy"
	rev.Spec.Tools = tools
	rev.Spec.Approvals = approvals
	r, err := agentrt.New(rev, append([]agentrt.Option{agentrt.WithProvider("spy", spy)}, opts...)...)
	if err != nil {
		return nil, err
	}
	collect(t, r, agentrt.Turn{Text: "go"})
	slices.Sort(shown)
	return shown, nil
}

// The model is shown the group's members, each under its own name, and never
// a tool named after the group.
func TestAnMCPGroupRegistersItsMembersNeverItself(t *testing.T) {
	t.Parallel()
	urmzd := group("urmzd", "urmzd__create_issue", "urmzd__search")
	docs := group("docs", "docs__lookup")
	catalog := groupCatalog{
		Tools:  agentrt.StaticTools(mcpTool{name: "fetch"}),
		groups: map[string]agentrt.ToolGroup{docs.Name: docs},
	}

	tests := []struct {
		name  string
		tools []string
		opts  []agentrt.Option
		want  []string
	}{
		{name: "a registered group", tools: []string{"mcp:urmzd"},
			opts: []agentrt.Option{agentrt.WithToolGroups(urmzd)},
			want: []string{"urmzd__create_issue", "urmzd__search"}},
		{name: "a group the catalog resolves", tools: []string{"mcp:docs"},
			opts: []agentrt.Option{agentrt.WithTools(catalog)},
			want: []string{"docs__lookup"}},
		{name: "groups beside a single tool", tools: []string{"fetch", "mcp:docs", "mcp:urmzd"},
			opts: []agentrt.Option{agentrt.WithTools(catalog), agentrt.WithToolGroups(urmzd)},
			want: []string{"docs__lookup", "fetch", "urmzd__create_issue", "urmzd__search"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			shown, err := toolsShown(t, tt.tools, nil, tt.opts...)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if !slices.Equal(shown, tt.want) {
				t.Errorf("model was shown %v, want %v", shown, tt.want)
			}
		})
	}
}

// A group that grants nothing fails the build loudly, like a missing tool.
func TestAnMCPGroupThatCannotBeResolvedRefusesToBuild(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		opts []agentrt.Option
	}{
		{name: "not registered and no catalog"},
		{name: "a catalog without groups", opts: []agentrt.Option{agentrt.WithTools(agentrt.StaticTools())}},
		{name: "a catalog that does not know it", opts: []agentrt.Option{agentrt.WithTools(groupCatalog{Tools: agentrt.StaticTools()})}},
		{name: "a group with no tools", opts: []agentrt.Option{agentrt.WithToolGroups(group("urmzd"))}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := toolsShown(t, []string{"mcp:urmzd"}, nil, tt.opts...)
			if !errors.Is(err, agentrt.ErrNoSuchToolGroup) {
				t.Fatalf("New: %v, want ErrNoSuchToolGroup", err)
			}
		})
	}
}

// An approval on a member is enforced, and one on a tool the group does not
// hold refuses to build: the guard never silently lapses.
func TestApprovalsHoldForToolsGrantedThroughAnMCPGroup(t *testing.T) {
	t.Parallel()
	urmzd := agentrt.WithToolGroups(group("urmzd", "urmzd__create_issue", "urmzd__search"))

	if _, err := toolsShown(t, []string{"mcp:urmzd"}, []string{"urmzd__create_issue"}, urmzd); err != nil {
		t.Errorf("approval for a member: New: %v", err)
	}
	if _, err := toolsShown(t, []string{"mcp:urmzd"}, []string{"urmzd__delete_repo"}, urmzd); !errors.Is(err, agentrt.ErrInvalid) {
		t.Errorf("approval for a tool outside the group: New: %v, want ErrInvalid", err)
	}
}
