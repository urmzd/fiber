package definition_test

import (
	"strings"
	"testing"

	"github.com/urmzd/fiber/pkg/definition"
)

func TestPolicyZeroGrantsNothing(t *testing.T) {
	var p definition.Policy
	if p.MayWrite("github") {
		t.Error("the zero policy grants MCP writes")
	}
	if p.MayUseArea("notes/a") || p.MayUseArea("") {
		t.Error("the zero policy grants a workspace area")
	}
	if len(p.Subscriptions) != 0 || len(p.Approvers) != 0 || p.SelfApproval || len(p.Knowledge) != 0 {
		t.Errorf("the zero policy holds grants: %+v", p)
	}
	content := "---\napiVersion: saige/v1\nname: docs-bot\nversion: 1.0.0\nmodel: offline/echo\ntools:\n  mcp:\n    - server: github\n---\nAnswer.\n"
	a, err := definition.Parse(content, p, definition.Checks{})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Spec.Access.WorkspaceAreas) != 0 || len(a.Spec.Access.Spawn) != 0 {
		t.Errorf("a zero policy compiled to access %+v", a.Spec.Access)
	}
	if strings.Join(a.Spec.Tools, ",") != "mcp:github" {
		t.Errorf("a zero policy compiled tools %v, want the read-only grant", a.Spec.Tools)
	}
	if a.Spec.SelfApproval || len(a.Spec.Approvers) > 0 || len(a.Spec.Answerers) > 0 || a.Spec.TaggedOnly {
		t.Errorf("a zero policy compiled answer rules %+v", a.Spec)
	}
}

func TestPolicyHashDetectsAnEdit(t *testing.T) {
	base := definition.Policy{WorkspaceAreas: []string{"b/", "a/"}, Approvers: []string{"ada"}}
	reordered := definition.Policy{WorkspaceAreas: []string{"a/", "b/"}, Approvers: []string{"ada"}}
	if base.Hash() != reordered.Hash() {
		t.Error("order changed the hash")
	}
	if !strings.HasPrefix(base.Hash(), "sha256:") {
		t.Errorf("hash %q has no algorithm", base.Hash())
	}
	for name, edit := range map[string]definition.Policy{
		"area":          {WorkspaceAreas: []string{"a/", "c/"}, Approvers: []string{"ada"}},
		"approver":      {WorkspaceAreas: []string{"a/", "b/"}, Approvers: []string{"eve"}},
		"self approval": {WorkspaceAreas: []string{"a/", "b/"}, Approvers: []string{"ada"}, SelfApproval: true},
		"mcp write":     {WorkspaceAreas: []string{"a/", "b/"}, Approvers: []string{"ada"}, MCPWrite: []string{"github"}},
	} {
		if edit.Hash() == base.Hash() {
			t.Errorf("an edit to the %s kept the hash", name)
		}
	}
}

func TestPolicyValidateRefusesBadEntries(t *testing.T) {
	for name, p := range map[string]definition.Policy{
		"empty area":         {WorkspaceAreas: []string{""}},
		"duplicate approver": {Approvers: []string{"a", "a"}},
		"unknown fallback":   {FallbackOn: []string{"sometimes"}},
		"any with others":    {FallbackOn: []string{"any", "auth"}},
		"no channel":         {Subscriptions: []definition.Subscription{{Types: []string{"message."}}}},
		"no types":           {Subscriptions: []definition.Subscription{{Channel: "slack://T01"}}},
		"corpus without uri": {Knowledge: []definition.Corpus{{Name: "docs"}}},
	} {
		if err := p.Validate(); err == nil {
			t.Errorf("%s: validated", name)
		}
	}
	if err := (definition.Policy{}).Validate(); err != nil {
		t.Errorf("zero policy: %v", err)
	}
}
