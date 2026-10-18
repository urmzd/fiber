package toolprovider_test

import (
	"testing"

	"github.com/urmzd/fiber/pkg/toolprovider"
)

// A tool is held back when anything a model would be told about it changes:
// its description or its schema. Key order and whitespace are not a change.
func TestCompareHoldsBackEveryChangeAModelWouldSee(t *testing.T) {
	read := tool(t, "search", "Search issues", `{"type":"object","properties":{"q":{"type":"string"}}}`)
	read.Class = toolprovider.ClassRead
	accepted := toolprovider.Snapshot{Version: 1, Tools: []toolprovider.Tool{read, tool(t, "gone", "", "")}}

	offered := []toolprovider.Tool{
		tool(t, "search", "Search issues", "{ \"properties\": {\"q\": {\"type\": \"string\"}}, \"type\": \"object\" }"),
		tool(t, "new_tool", "", ""),
	}
	d := toolprovider.Compare(accepted, offered)
	if len(d.Unchanged) != 1 || d.Unchanged[0].Class != toolprovider.ClassRead {
		t.Fatalf("reformatted schema: unchanged = %+v, want the accepted read tool", d.Unchanged)
	}
	if len(d.Added) != 1 || d.Added[0].Name != "new_tool" {
		t.Fatalf("added = %+v", d.Added)
	}
	if len(d.Removed) != 1 || d.Removed[0].Name != "gone" {
		t.Fatalf("removed = %+v", d.Removed)
	}

	changes := map[string]toolprovider.Tool{
		"description": tool(t, "search", "Search and DELETE issues", `{"type":"object","properties":{"q":{"type":"string"}}}`),
		"schema":      tool(t, "search", "Search issues", `{"type":"object","properties":{"q":{"type":"string"},"force":{"type":"boolean"}}}`),
	}
	for what, changed := range changes {
		d := toolprovider.Compare(accepted, []toolprovider.Tool{changed})
		if len(d.Unchanged) != 0 || len(d.Changed) != 1 {
			t.Errorf("%s change: diff = %+v, want the tool held back as changed", what, d)
			continue
		}
		if p := d.Pending(); len(p) != 1 || p[0].Description != changed.Description || p[0].SchemaHash != changed.SchemaHash {
			t.Errorf("%s change: pending = %+v, want the offered form", what, p)
		}
	}
}

func TestClassDefaultsToWriteAndGrantDefaultsToRead(t *testing.T) {
	cases := []struct {
		grant toolprovider.Grant
		class toolprovider.Class
		want  bool
	}{
		{toolprovider.GrantRead, toolprovider.ClassRead, true},
		{toolprovider.GrantRead, toolprovider.ClassWrite, false},
		{toolprovider.GrantRead, "", false},
		{toolprovider.GrantRead, "readonly", false},
		{toolprovider.GrantWrite, toolprovider.ClassWrite, true},
		{toolprovider.GrantWrite, toolprovider.ClassRead, true},
		{"", toolprovider.ClassWrite, false},
		{"", toolprovider.ClassRead, true},
		{"admin", toolprovider.ClassWrite, false},
	}
	for _, c := range cases {
		if got := c.grant.Permits(c.class); got != c.want {
			t.Errorf("Grant(%q).Permits(%q) = %v, want %v", c.grant, c.class, got, c.want)
		}
	}
}
