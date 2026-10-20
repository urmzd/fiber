package agentspec_test

import (
	"errors"
	"testing"

	"github.com/urmzd/fiber/pkg/agentspec"
)

// An MCP tool group is a grant like a tool: it must name a group, and an approval
// may name a tool the group could hold but never the group itself, since an
// approval guards specific calls.
func TestAToolGroupGrantIsValidatedLikeATool(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		tools     []string
		approvals []string
		ok        bool
	}{
		{name: "a group", tools: []string{"mcp:urmzd"}, ok: true},
		{name: "a group beside tools", tools: []string{"mcp:urmzd", "search"}, ok: true},
		{name: "a group with no name", tools: []string{"mcp:"}, ok: false},
		{name: "a group name with spaces", tools: []string{"mcp: urmzd"}, ok: false},
		{name: "a group granted twice", tools: []string{"mcp:urmzd", "mcp:urmzd"}, ok: false},
		{name: "approval for a tool a group may hold", tools: []string{"mcp:urmzd"}, approvals: []string{"urmzd__create_issue"}, ok: true},
		{name: "approval naming the group", tools: []string{"mcp:urmzd"}, approvals: []string{"mcp:urmzd"}, ok: false},
		{name: "approval with no group or tool granted", tools: []string{"search"}, approvals: []string{"urmzd__create_issue"}, ok: false},
		{name: "a write grant", tools: []string{"mcp:urmzd:write"}, ok: true},
		{name: "a read grant spelled out", tools: []string{"mcp:urmzd:read"}, ok: true},
		{name: "a read and a write grant of one provider", tools: []string{"mcp:urmzd", "mcp:urmzd:write"}, ok: true},
		{name: "a class with no provider", tools: []string{"mcp::write"}, ok: false},
		{name: "a class after a padded provider", tools: []string{"mcp: urmzd:write"}, ok: false},
		{name: "approval for a tool a write grant may hold", tools: []string{"mcp:urmzd:write"}, approvals: []string{"urmzd__create_issue"}, ok: true},
		{name: "approval naming a write grant", tools: []string{"mcp:urmzd:write"}, approvals: []string{"mcp:urmzd:write"}, ok: false},
	} {
		s := valid()
		s.Tools = tc.tools
		s.Approvals = tc.approvals
		err := s.Validate()
		if tc.ok != (err == nil) || (err != nil && !errors.Is(err, agentspec.ErrInvalid)) {
			t.Errorf("%s: err = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

// A group grant is read only unless it says write, so leaving the class off
// can only ever grant less. A last segment that is not a class stays part of
// the group's name, which keeps every grant that validated before classes
// existed valid and meaning what it meant.
func TestAnMCPGrantIsReadOnlyUnlessItSaysWrite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		group bool
		want  agentspec.MCPGrant
		valid bool
	}{
		{"search", false, agentspec.MCPGrant{}, true},
		{"mcp:github", true, agentspec.MCPGrant{Provider: "github"}, true},
		{"mcp:github:read", true, agentspec.MCPGrant{Provider: "github"}, true},
		{"mcp:github:write", true, agentspec.MCPGrant{Provider: "github", Write: true}, true},
		{"mcp:github:admin", true, agentspec.MCPGrant{Provider: "github:admin"}, true},
		{"mcp:", true, agentspec.MCPGrant{}, false},
		{"mcp::write", true, agentspec.MCPGrant{}, false},
		{"mcp:github ", true, agentspec.MCPGrant{}, false},
	} {
		g, ok, err := agentspec.ParseMCPGrant(tc.name)
		if ok != tc.group || (err == nil) != tc.valid || (err == nil && g != tc.want) {
			t.Errorf("ParseMCPGrant(%q) = %+v, %v, %v; want %+v, %v, valid %v", tc.name, g, ok, err, tc.want, tc.group, tc.valid)
		}
		if err != nil && !errors.Is(err, agentspec.ErrInvalid) {
			t.Errorf("ParseMCPGrant(%q) error %v does not match ErrInvalid", tc.name, err)
		}
		if err == nil && ok && tc.name != "mcp:github:read" && g.String() != tc.name {
			t.Errorf("ParseMCPGrant(%q).String() = %q, want the grant back", tc.name, g.String())
		}
	}
}
