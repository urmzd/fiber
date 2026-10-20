package agentrt_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/connector"
)

// namedConnector contributes the given actions under a chosen name.
type namedConnector struct {
	name    string
	actions []connector.Action
}

func (c *namedConnector) Name() string                { return c.name }
func (c *namedConnector) Actions() []connector.Action { return c.actions }

// silentConnector is an actor that contributes no actions.
type silentConnector struct{}

func (silentConnector) Name() string                { return "quiet" }
func (silentConnector) Actions() []connector.Action { return nil }

func noop(context.Context, address.Address, json.RawMessage) (connector.Result, error) {
	return connector.Result{Summary: "ok"}, nil
}

func action(name string) connector.Action {
	return connector.Action{Name: name, Schema: json.RawMessage(`{"type":"object"}`), Invoke: noop}
}

func groupRegistry(t *testing.T) *connector.Registry {
	t.Helper()
	reg := connector.NewRegistry()
	for _, c := range []connector.Connector{
		&namedConnector{name: "github", actions: []connector.Action{action("github_comment"), action("github_add_labels")}},
		&namedConnector{name: "jira", actions: []connector.Action{action("jira_comment")}},
		silentConnector{},
	} {
		if err := reg.Register(c); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}
	return reg
}

// toolsShown builds a runner for tools, runs one turn, and returns the tool
// names the model was shown.
func toolsShown(t *testing.T, tools []string, catalog agentrt.Tools, approvals ...string) ([]string, error) {
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
	r, err := agentrt.New(rev, agentrt.WithProvider("spy", spy), agentrt.WithTools(catalog))
	if err != nil {
		return nil, err
	}
	collect(t, r, agentrt.Turn{Text: "go"})
	return shown, nil
}

func TestAToolGroupGrantsEveryActionOfItsConnector(t *testing.T) {
	t.Parallel()
	catalog := agentrt.ConnectorTools(groupRegistry(t), address.Address{}, nil)

	tests := []struct {
		name  string
		tools []string
		want  []string
	}{
		{name: "one group", tools: []string{"group:github"}, want: []string{"github_add_labels", "github_comment"}},
		{name: "group and a single tool", tools: []string{"group:github", "jira_comment"}, want: []string{"github_add_labels", "github_comment", "jira_comment"}},
		{name: "a tool granted twice is granted once", tools: []string{"github_comment", "group:github"}, want: []string{"github_add_labels", "github_comment"}},
		{name: "two groups", tools: []string{"group:jira", "group:github"}, want: []string{"github_add_labels", "github_comment", "jira_comment"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			shown, err := toolsShown(t, tt.tools, catalog)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			slices.Sort(shown)
			if !slices.Equal(shown, tt.want) {
				t.Errorf("model was shown %v, want %v", shown, tt.want)
			}
		})
	}
}

// A group that grants nothing fails the build loudly, like a missing tool.
func TestAToolGroupThatCannotBeResolvedRefusesToBuild(t *testing.T) {
	t.Parallel()
	reg := groupRegistry(t)

	tests := []struct {
		name    string
		tools   []string
		catalog agentrt.Tools
	}{
		{name: "unregistered connector", tools: []string{"group:slack"}, catalog: agentrt.ConnectorTools(reg, address.Address{}, nil)},
		{name: "connector with no actions", tools: []string{"group:quiet"}, catalog: agentrt.ConnectorTools(reg, address.Address{}, nil)},
		{name: "catalog without groups", tools: []string{"group:github"}, catalog: agentrt.StaticTools()},
		{name: "no catalog at all", tools: []string{"group:github"}, catalog: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := toolsShown(t, tt.tools, tt.catalog)
			if !errors.Is(err, agentrt.ErrNoSuchToolGroup) {
				t.Fatalf("New: %v, want ErrNoSuchToolGroup", err)
			}
		})
	}
}

// An approval on a tool granted through a group is enforced, and one on a
// tool the group does not hold refuses to build: the guard never silently
// lapses.
func TestApprovalsHoldForToolsGrantedThroughAGroup(t *testing.T) {
	t.Parallel()
	catalog := agentrt.ConnectorTools(groupRegistry(t), address.Address{}, nil)

	if _, err := toolsShown(t, []string{"group:github"}, catalog, "github_comment"); err != nil {
		t.Errorf("approval for a group member: New: %v", err)
	}
	if _, err := toolsShown(t, []string{"group:github"}, catalog, "jira_comment"); !errors.Is(err, agentrt.ErrInvalid) {
		t.Errorf("approval for a tool outside the group: New: %v, want ErrInvalid", err)
	}
}

func TestConnectorGroupsListsOnlyConnectorsWithActions(t *testing.T) {
	t.Parallel()
	got := agentrt.ConnectorGroups(groupRegistry(t))
	if want := []string{"group:github", "group:jira"}; !slices.Equal(got, want) {
		t.Errorf("ConnectorGroups = %v, want %v", got, want)
	}
}
