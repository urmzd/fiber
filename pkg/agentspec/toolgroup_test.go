package agentspec_test

import (
	"errors"
	"testing"

	"github.com/urmzd/fiber/pkg/agentspec"
)

// A tool group is a grant like a tool: it must name a group, and an approval
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
		{name: "a group", tools: []string{"group:github"}, ok: true},
		{name: "a group beside tools", tools: []string{"group:github", "search"}, ok: true},
		{name: "a group with no name", tools: []string{"group:"}, ok: false},
		{name: "a group name with spaces", tools: []string{"group: github"}, ok: false},
		{name: "a group granted twice", tools: []string{"group:github", "group:github"}, ok: false},
		{name: "approval for a tool a group may hold", tools: []string{"group:github"}, approvals: []string{"github_comment"}, ok: true},
		{name: "approval naming the group", tools: []string{"group:github"}, approvals: []string{"group:github"}, ok: false},
		{name: "approval with no group or tool granted", tools: []string{"search"}, approvals: []string{"github_comment"}, ok: false},
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
