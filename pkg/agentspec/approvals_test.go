package agentspec_test

import (
	"errors"
	"testing"

	"github.com/urmzd/mandatum/pkg/agentspec"
)

// Approvals came after revisions were already stored with their hashes. A
// spec that requires none hashes exactly as it did in v0.10.0, so no stored
// revision reads as changed; one that requires some hashes differently.
func TestApprovalsLeaveTheHashOfASpecWithoutThemUnchanged(t *testing.T) {
	t.Parallel()
	const before = "sha256:3dbf7901c3a3336ee0afd3806eca2f03c1348dbaa8c0dc49dcc9614d5d91da95" // valid().Hash() in v0.10.0
	if got := valid().Hash(); got != before {
		t.Fatalf("a spec without approvals hashes to %s, want %s as before approvals existed", got, before)
	}
	guarded := valid()
	guarded.Approvals = []string{"fetch"}
	if guarded.Hash() == before {
		t.Fatal("requiring an approval did not change the hash")
	}
	reordered := valid()
	reordered.Tools = []string{"fetch", "search"}
	reordered.Approvals = []string{"search", "fetch"}
	both := valid()
	both.Approvals = []string{"fetch", "search"}
	if reordered.Hash() != both.Hash() {
		t.Error("the order approvals are listed in changed the hash")
	}
}

// An approval names a tool the agent is granted, or the delegation tool of an
// agent it may spawn: a guard on a tool it cannot call would never be asked.
func TestAnApprovalMustNameAGrantedTool(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		approvals []string
		ok        bool
	}{
		{[]string{"search"}, true},
		{[]string{"tag_linter"}, true},
		{[]string{"deploy"}, false},
		{[]string{"tag_someone"}, false},
		{[]string{"search", "search"}, false},
		{[]string{""}, false},
	} {
		s := valid()
		s.Approvals = tc.approvals
		err := s.Validate()
		if tc.ok != (err == nil) || (err != nil && !errors.Is(err, agentspec.ErrInvalid)) {
			t.Errorf("approvals %q: err = %v, want ok=%v", tc.approvals, err, tc.ok)
		}
	}
}
