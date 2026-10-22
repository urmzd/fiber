package agentrt_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/agentrt"
)

// ResolveTools is what a host shows before it runs an agent, so it must say
// exactly what a turn of the agent would offer: the same definitions the
// model is sent, in the same order, and the same grants left out with the
// same reasons and notes.
func TestResolveToolsReportsWhatATurnOffersTheModel(t *testing.T) {
	t.Parallel()

	refusal := notedErr{operator: "issues: carol has no credential", model: "they have not connected one for issues"}
	catalog := noted{groupCatalog: groupCatalog{Tools: agentrt.StaticTools(lookupTool("lookup"), lookupTool("search"))}, err: refusal}
	rev := offlineRevision(1, "You look things up.")
	rev.Spec.Provider = "spy"
	rev.Spec.Tools = []string{"search", "jira_transition", agentrt.AskPersonToolName, agentrt.MCPGroup("issues"), "lookup"}
	rev.Spec.Approvals = []string{"lookup"}

	var shown [][]saigetypes.ToolDef
	var msgs []saigetypes.Message
	opts := []agentrt.Option{agentrt.WithProvider("spy", spyRequests(&shown, &msgs)), agentrt.WithTools(catalog)}
	r, err := agentrt.New(rev, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	collect(t, r, agentrt.Turn{Text: "look it up"})
	if len(shown) == 0 {
		t.Fatal("the model was never asked")
	}

	got, err := agentrt.ResolveTools(rev, opts...)
	if err != nil {
		t.Fatalf("ResolveTools: %v", err)
	}
	if !reflect.DeepEqual(got.Tools, shown[0]) {
		t.Errorf("ResolveTools reports\n%+v\nthe model was shown\n%+v", got.Tools, shown[0])
	}
	if !reflect.DeepEqual(got.Unavailable, r.Unavailable()) {
		t.Errorf("ResolveTools leaves out %+v, the runner left out %+v", got.Unavailable, r.Unavailable())
	}
	var names []string
	for _, d := range got.Tools {
		names = append(names, d.Name)
	}
	if want := []string{agentrt.AskPersonToolName, "lookup", "search"}; !reflect.DeepEqual(names, want) {
		t.Errorf("tools %v, want %v sorted by name", names, want)
	}
	if len(got.Unavailable) != 2 || got.Unavailable[0].Name != "jira_transition" ||
		got.Unavailable[1].Name != agentrt.MCPGroup("issues") || got.Unavailable[1].Note != refusal.model {
		t.Errorf("unavailable %+v, want jira_transition, then the group with the model's note", got.Unavailable)
	}
}

// Which tools an agent is granted does not depend on its model, so listing
// them builds no model client: an agent whose provider has no key can still
// be shown its tools, where New refuses to build it.
func TestResolveToolsNeedsNoModelCredential(t *testing.T) {
	t.Setenv(agentrt.EnvAnthropicKey, "")

	rev := offlineRevision(1, "You look things up.")
	rev.Spec.Provider = agentrt.ProviderAnthropic
	rev.Spec.Tools = []string{"lookup"}
	opts := []agentrt.Option{agentrt.WithTools(agentrt.StaticTools(lookupTool("lookup")))}
	if _, err := agentrt.New(rev, opts...); err == nil || !strings.Contains(err.Error(), agentrt.EnvAnthropicKey) {
		t.Fatalf("New = %v, want it refused for want of %s", err, agentrt.EnvAnthropicKey)
	}
	got, err := agentrt.ResolveTools(rev, opts...)
	if err != nil {
		t.Fatalf("ResolveTools = %v, want the tools listed without a key", err)
	}
	if len(got.Tools) != 1 || got.Tools[0].Name != "lookup" {
		t.Errorf("tools %+v, want lookup", got.Tools)
	}

	// And no tool is called to list it.
	called := &agenttest.MockTool{Def: saigetypes.ToolDef{Name: "lookup", Parameters: saigetypes.ParameterSchema{Type: "object"}}}
	if _, err := agentrt.ResolveTools(rev, agentrt.WithTools(agentrt.StaticTools(called))); err != nil {
		t.Fatal(err)
	}
	if len(called.Calls) != 0 {
		t.Errorf("listing the tools called one %d times", len(called.Calls))
	}
}

// The listing refuses what the build refuses, so a host never shows tools
// for an agent that would not start.
func TestResolveToolsRefusesWhatNewRefuses(t *testing.T) {
	t.Parallel()

	catalog := agentrt.ToolsFunc(func(name string) (saigetypes.Tool, error) {
		if name == "lookup" {
			return lookupTool("lookup"), nil
		}
		return nil, fmt.Errorf("%w: %q", agentrt.ErrNoSuchTool, name)
	})
	for _, tc := range []struct {
		name      string
		tools     []string
		approvals []string
		opts      []agentrt.Option
		want      error
	}{
		{name: "an approval for a tool it is not granted", tools: []string{"lookup"}, approvals: []string{"deploy"}, want: agentrt.ErrInvalid},
		{name: "a missing tool under strict tools", tools: []string{"lookup", "deploy"}, opts: []agentrt.Option{agentrt.WithStrictTools()}, want: agentrt.ErrNoSuchTool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rev := offlineRevision(1, "You look things up.")
			rev.Spec.Tools, rev.Spec.Approvals = tc.tools, tc.approvals
			opts := append([]agentrt.Option{agentrt.WithTools(catalog)}, tc.opts...)
			_, newErr := agentrt.New(rev, opts...)
			_, err := agentrt.ResolveTools(rev, opts...)
			if !errors.Is(err, tc.want) || newErr == nil || err.Error() != newErr.Error() {
				t.Errorf("ResolveTools = %v, New = %v; want both to refuse with %v", err, newErr, tc.want)
			}
		})
	}
}
