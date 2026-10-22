package mcptools_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

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
// as write; the server's other tools were never accepted.
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
		{"mcp:fake", []string{"fake__search"}},
		{"mcp:fake:read", []string{"fake__search"}},
		{"mcp:fake:write", []string{"fake__create_issue", "fake__search"}},
	} {
		tools, ok, err := g.Expand(ctx, tenant, tc.grant)
		if !ok || err != nil {
			t.Fatalf("Expand(%q) = ok %v, %v", tc.grant, ok, err)
		}
		if got := names(tools); !slices.Equal(got, tc.want) {
			t.Errorf("Expand(%q) = %v, want %v", tc.grant, got, tc.want)
		}
		for _, tool := range tools {
			if p, ok := tool.(interface{ ActsForPerson() bool }); !ok || p.ActsForPerson() {
				t.Errorf("%s of a shared provider says it acts as a person", tool.Definition().Name)
			}
		}
	}

	// Names that are not a store-backed grant are left to someone else: an
	// ordinary tool, and a group name no provider can have (a host may
	// register such a group itself).
	for _, name := range []string{"search", "mcp:Fake", "mcp:fake:admin"} {
		if _, ok, err := g.Expand(ctx, tenant, name); ok || err != nil {
			t.Errorf("Expand(%q) = ok %v, %v; want not this source's", name, ok, err)
		}
	}
	if _, ok, err := g.Expand(ctx, tenant, "mcp:missing"); !ok || !errors.Is(err, toolprovider.ErrNotFound) {
		t.Errorf("Expand of an unknown provider = ok %v, %v; want ErrNotFound", ok, err)
	}
}

// A provider is a tenant's: another tenant's grant of the same name expands
// against its own record and accepted list, or not at all.
func TestAGrantExpandsPerTenant(t *testing.T) {
	ctx := context.Background()
	g, store, f, dials := grantsFixture(t)

	if _, ok, err := g.Expand(ctx, "globex", "mcp:fake"); !ok || !errors.Is(err, toolprovider.ErrNotFound) {
		t.Fatalf("a tenant without the provider = ok %v, %v; want ErrNotFound", ok, err)
	}
	p, err := store.PutProvider(ctx, toolprovider.Provider{Tenant: "globex", Name: "fake",
		Transport: toolprovider.TransportHTTP, Endpoint: "http://127.0.0.1:1/mcp"})
	if err != nil {
		t.Fatal(err)
	}
	offered, err := connectInMemory(t, f, p).Offered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Accept(ctx, "globex", "fake", 0,
		pick(t, offered, map[string]toolprovider.Class{"create_issue": toolprovider.ClassWrite}), "admin"); err != nil {
		t.Fatal(err)
	}

	acme, _, err := g.Expand(ctx, tenant, "mcp:fake:write")
	if err != nil {
		t.Fatal(err)
	}
	globex, _, err := g.Expand(ctx, "globex", "mcp:fake:write")
	if err != nil {
		t.Fatal(err)
	}
	if got := names(acme); !slices.Equal(got, []string{"fake__create_issue", "fake__search"}) {
		t.Errorf("acme = %v", got)
	}
	if got := names(globex); !slices.Equal(got, []string{"fake__create_issue"}) {
		t.Errorf("globex = %v, want only what globex's admin accepted", got)
	}
	if dials.Load() != 2 {
		t.Errorf("two tenants dialled %d sessions, want one each", dials.Load())
	}
}

// A read-only grant cannot reach a write tool: it is not among the group's
// members, and a member obtained under the read grant is refused at call time
// whatever it is asked to run. A write grant reaches it.
func TestAReadOnlyGroupGrantCannotCallAWriteTool(t *testing.T) {
	ctx := context.Background()
	g, _, f, _ := grantsFixture(t)

	read, _, err := g.Expand(ctx, tenant, "mcp:fake")
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range read {
		if tool.Definition().Name == "fake__create_issue" {
			t.Fatal("a read grant expanded to the write tool")
		}
		if tool.Definition().Capability != "read" {
			t.Errorf("%s under a read grant has capability %q", tool.Definition().Name, tool.Definition().Capability)
		}
	}

	write, _, err := g.Expand(ctx, tenant, "mcp:fake:write")
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range write {
		if tool.Definition().Name != "fake__create_issue" {
			continue
		}
		if _, err := tool.Execute(ctx, nil); err != nil {
			t.Fatalf("a write tool under a write grant: %v", err)
		}
	}
	if f.writes.Load() != 1 {
		t.Fatalf("writes = %d, want the one call under the write grant", f.writes.Load())
	}
}

// A session is kept for the provider's record version: later turns reuse it,
// a changed record (a new endpoint, a rotated credential) is dialled afresh,
// and nothing is expanded after Close.
func TestGrantsReuseASessionUntilTheProviderRecordChanges(t *testing.T) {
	ctx := context.Background()
	g, store, _, dials := grantsFixture(t)

	for _, grant := range []string{"mcp:fake", "mcp:fake:write", "mcp:fake"} {
		if _, _, err := g.Expand(ctx, tenant, grant); err != nil {
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
	for range 2 {
		if _, _, err := g.Expand(ctx, tenant, "mcp:fake"); err != nil {
			t.Fatal(err)
		}
	}
	if dials.Load() != 2 {
		t.Fatalf("after the record changed, dials = %d, want 2", dials.Load())
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := g.Expand(ctx, tenant, "mcp:fake"); !errors.Is(err, mcptools.ErrClosed) {
		t.Fatalf("Expand after Close = %v, want ErrClosed", err)
	}
}
