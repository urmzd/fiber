package agentspec_test

import (
	"errors"
	"testing"

	"github.com/urmzd/fiber/pkg/agentspec"
)

// An MCP grant is read only unless it says write, so forgetting the class can
// only ever grant less.
func TestAnMCPGrantIsReadOnlyUnlessItSaysWrite(t *testing.T) {
	cases := []struct {
		name  string
		ok    bool
		want  agentspec.MCPGrant
		valid bool
	}{
		{"search", false, agentspec.MCPGrant{}, true},
		{"mcp:github", true, agentspec.MCPGrant{Provider: "github"}, true},
		{"mcp:github:read", true, agentspec.MCPGrant{Provider: "github"}, true},
		{"mcp:github:write", true, agentspec.MCPGrant{Provider: "github", Write: true}, true},
		{"mcp:github:admin", true, agentspec.MCPGrant{}, false},
		{"mcp:", true, agentspec.MCPGrant{}, false},
		{"mcp:Git_Hub", true, agentspec.MCPGrant{}, false},
	}
	for _, tc := range cases {
		g, ok, err := agentspec.ParseMCPGrant(tc.name)
		if ok != tc.ok || (err == nil) != tc.valid || (err == nil && g != tc.want) {
			t.Errorf("ParseMCPGrant(%q) = %+v, %v, %v; want %+v, %v, valid %v", tc.name, g, ok, err, tc.want, tc.ok, tc.valid)
		}
		if err != nil && !errors.Is(err, agentspec.ErrInvalid) {
			t.Errorf("ParseMCPGrant(%q) error %v does not match ErrInvalid", tc.name, err)
		}
	}
}

// A definition is checked for the grant's shape, not for whether the provider
// exists: that is a property of the deployment. Approvals may name tools a
// provider grant expands to.
func TestADefinitionMayGrantAnMCPProviderAndRequireApprovalForItsTools(t *testing.T) {
	spec := valid()
	spec.Tools = []string{"mcp:github:write"}
	spec.Approvals = []string{"github_create_issue"}
	if err := spec.Validate(); err != nil {
		t.Fatalf("Validate = %v, want accepted", err)
	}

	for name, mutate := range map[string]func(*agentspec.AgentSpec){
		"a malformed grant":             func(s *agentspec.AgentSpec) { s.Tools = []string{"mcp:github:all"} },
		"one provider granted twice":    func(s *agentspec.AgentSpec) { s.Tools = []string{"mcp:github", "mcp:github:write"} },
		"an approval outside the grant": func(s *agentspec.AgentSpec) { s.Approvals = []string{"jira_create_issue"} },
	} {
		s := spec
		mutate(&s)
		if err := s.Validate(); !errors.Is(err, agentspec.ErrInvalid) {
			t.Errorf("%s: Validate = %v, want ErrInvalid", name, err)
		}
	}
}
