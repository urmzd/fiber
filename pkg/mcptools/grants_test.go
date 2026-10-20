package mcptools_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// dialFake dials f over a fresh in-process transport and counts the dials.
func dialFake(t *testing.T, f *fake, dials *atomic.Int32) mcptools.Dial {
	t.Helper()
	return func(ctx context.Context, p toolprovider.Provider) (*mcptools.Session, error) {
		dials.Add(1)
		ct, st := mcp.NewInMemoryTransports()
		ss, err := f.srv.Connect(ctx, st, nil)
		if err != nil {
			return nil, err
		}
		t.Cleanup(func() { _ = ss.Close() })
		return mcptools.Connect(ctx, p, mcptools.WithTransport(ct))
	}
}

// grantsFixture is a provider with search accepted as read and create_issue
// as write; the server's other three tools were never accepted.
func grantsFixture(t *testing.T) (*mcptools.Grants, toolprovider.Store, *fake, *atomic.Int32) {
	t.Helper()
	store := toolprovider.NewMemory()
	f := newFake()
	accept(t, store, connectInMemory(t, f, provider(store, t)), 0, map[string]toolprovider.Class{
		"search": toolprovider.ClassRead, "create_issue": toolprovider.ClassWrite,
	})
	var dials atomic.Int32
	g := mcptools.NewGrants(store, dialFake(t, f, &dials))
	t.Cleanup(func() { _ = g.Close() })
	return g, store, f, &dials
}

// "mcp:<provider>" stands for the provider's accepted tools under the grant's
// class: read only unless the grant says write. A tool nobody accepted never
// appears, whatever the class.
func TestAProviderGrantExpandsToItsAcceptedToolsUnderItsClass(t *testing.T) {
	ctx := context.Background()
	g, _, _, _ := grantsFixture(t)

	for _, tc := range []struct {
		grant string
		want  []string
	}{
		{"mcp:fake", []string{"fake_search"}},
		{"mcp:fake:read", []string{"fake_search"}},
		{"mcp:fake:write", []string{"fake_create_issue", "fake_search"}},
	} {
		tools, ok, err := g.Expand(ctx, tenant, tc.grant)
		if !ok || err != nil {
			t.Fatalf("Expand(%q) = ok %v, %v", tc.grant, ok, err)
		}
		if got := names(tools); !slices.Equal(got, tc.want) {
			t.Errorf("Expand(%q) = %v, want %v", tc.grant, got, tc.want)
		}
	}

	if _, ok, err := g.Expand(ctx, tenant, "search"); ok || err != nil {
		t.Errorf("Expand of an ordinary tool name = ok %v, %v; want not a group", ok, err)
	}
	if _, ok, err := g.Expand(ctx, tenant, "mcp:missing"); !ok || !errors.Is(err, toolprovider.ErrNotFound) {
		t.Errorf("Expand of an unknown provider = ok %v, %v; want ErrNotFound", ok, err)
	}
	if _, ok, err := g.Expand(ctx, "globex", "mcp:fake"); !ok || !errors.Is(err, toolprovider.ErrNotFound) {
		t.Errorf("another tenant expanding the provider = ok %v, %v; want ErrNotFound", ok, err)
	}
	if _, ok, err := g.Expand(ctx, tenant, "mcp:fake:admin"); !ok || !errors.Is(err, agentspec.ErrInvalid) {
		t.Errorf("Expand of a malformed grant = ok %v, %v; want agentspec.ErrInvalid", ok, err)
	}
}

// An expanded tool is bound to its grant's class: a write grant can call a
// write tool, and a read grant's tools all report read capability (the call
// itself is refused under a read grant; see
// TestAReadOnlyGrantCannotReachAWriteTool).
func TestAnExpandedToolCarriesItsGrantsClass(t *testing.T) {
	ctx := context.Background()
	g, _, f, _ := grantsFixture(t)

	tools, _, err := g.Expand(ctx, tenant, "mcp:fake:write")
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range tools {
		if tool.Definition().Name != "fake_create_issue" {
			continue
		}
		if _, err := tool.Execute(ctx, nil); err != nil {
			t.Fatalf("a write tool under a write grant: %v", err)
		}
	}
	if f.writes.Load() != 1 {
		t.Fatalf("writes = %d, want the one call", f.writes.Load())
	}
	read, _, err := g.Expand(ctx, tenant, "mcp:fake")
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range read {
		if tool.Definition().Capability != "read" {
			t.Errorf("%s under a read grant has capability %q", tool.Definition().Name, tool.Definition().Capability)
		}
	}
}

// A session is kept for the provider's record version: a second turn reuses
// it, and a changed record (a new endpoint, a rotated credential) is dialled
// afresh.
func TestGrantsReuseASessionUntilTheProviderRecordChanges(t *testing.T) {
	ctx := context.Background()
	g, store, _, dials := grantsFixture(t)

	for range 3 {
		if _, _, err := g.Expand(ctx, tenant, "mcp:fake"); err != nil {
			t.Fatal(err)
		}
	}
	if dials.Load() != 1 {
		t.Fatalf("three expansions dialled %d times, want 1", dials.Load())
	}
	p, err := store.GetProvider(ctx, tenant, "fake")
	if err != nil {
		t.Fatal(err)
	}
	p.Endpoint = "http://127.0.0.1:2/mcp"
	if _, err := store.PutProvider(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, _, err := g.Expand(ctx, tenant, "mcp:fake"); err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 2 {
		t.Fatalf("after the record changed, dials = %d, want 2", dials.Load())
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := g.Expand(ctx, tenant, "mcp:fake"); err == nil {
		t.Fatal("Expand after Close succeeded")
	}
}
