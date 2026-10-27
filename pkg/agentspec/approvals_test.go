package agentspec_test

import (
	"errors"
	"testing"

	"github.com/urmzd/fiber/pkg/agentspec"
)

// Approvals came after revisions were already stored with their hashes. A
// spec that requires none hashes exactly as it did in v0.10.0, so no stored
// revision reads as changed; one that requires some hashes differently.
func TestApprovalsLeaveTheHashOfASpecWithoutThemUnchanged(t *testing.T) {
	t.Parallel()
	const before = "sha256:31deb5030b98b85b432d94ec650d6e134c0b1ccc6234d94f8869e2ed69d62435" // valid().Hash() under the fiber/agentspec/v1 domain, unchanged by later fields
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

// Who may answer is part of what a revision is: it is hashed when set, a
// spec that names approvers or self-approval must require some approval, and
// a spec that sets none of it hashes as before.
func TestWhoMayAnswerIsPartOfTheRevision(t *testing.T) {
	t.Parallel()
	base := valid()
	base.Approvals = []string{"fetch"}
	for _, mod := range []func(*agentspec.AgentSpec){
		func(s *agentspec.AgentSpec) { s.Approvers = []string{"carol"} },
		func(s *agentspec.AgentSpec) { s.SelfApproval = true },
		func(s *agentspec.AgentSpec) { s.Answerers = []string{"role:oncall"} },
	} {
		s := base
		mod(&s)
		if s.Hash() == base.Hash() {
			t.Errorf("%+v hashes like a spec without it", s)
		}
		if err := s.Validate(); err != nil {
			t.Errorf("%+v: %v", s, err)
		}
	}
	orphan := valid()
	orphan.Approvers = []string{"carol"}
	if err := orphan.Validate(); !errors.Is(err, agentspec.ErrInvalid) {
		t.Errorf("approvers without approvals: %v", err)
	}
	answerers := valid()
	answerers.Answerers = []string{"carol"}
	if err := answerers.Validate(); err != nil {
		t.Errorf("answerers without approvals are for ask_person: %v", err)
	}
}

// Tagged-only is part of what a revision is: it is hashed and survives
// Normalize when set, and a spec without it hashes as before.
func TestTaggedOnlyIsPartOfTheRevision(t *testing.T) {
	t.Parallel()
	base := valid()
	s := base
	s.TaggedOnly = true
	if s.Hash() == base.Hash() {
		t.Error("a tagged-only spec hashes like one without it")
	}
	if !s.Normalize().TaggedOnly {
		t.Error("Normalize dropped tagged_only")
	}
	if err := s.Validate(); err != nil {
		t.Errorf("tagged-only: %v", err)
	}
}
