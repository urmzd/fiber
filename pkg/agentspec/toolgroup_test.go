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
