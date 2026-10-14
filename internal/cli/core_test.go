package cli

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/agentspec"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// inbox is a sink that records what was delivered where.
type inbox struct {
	mu   sync.Mutex
	seen map[string][]envelope.Event
}

func (i *inbox) Name() string { return "inbox" }

func (i *inbox) Deliver(_ context.Context, target address.Address, e envelope.Event) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.seen == nil {
		i.seen = map[string][]envelope.Event{}
	}
	i.seen[target.String()] = append(i.seen[target.String()], e)
	return nil
}

func (i *inbox) at(target string) []envelope.Event {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]envelope.Event(nil), i.seen[target]...)
}

// completed reports whether target has received a run's final event.
func (i *inbox) completed(target string) bool {
	for _, e := range i.at(target) {
		if e.Kind == envelope.KindCompleted {
			return true
		}
	}
	return false
}

// A tag id is whatever the caller sent, so two tenants can send the same one.
// Routes are registered and runs are started under an identity that includes
// the tenant, so the second tenant gets its own run delivered to its own
// target. It does not join the first tenant's run, and it does not replace
// where that run's events go. This is checked on what was actually delivered,
// because a collision here would be invisible to any read that filters by
// tenant afterwards.
func TestATenantReusingAnotherTenantsTagIDCannotJoinOrRerouteItsRun(t *testing.T) {
	c, err := newCore(coreConfig{Tenant: "acme", Workspace: t.TempDir()})
	if err != nil {
		t.Fatalf("newCore: %v", err)
	}
	t.Cleanup(func() { _ = c.close() })

	box := &inbox{}
	if err := c.sinks.Register(box); err != nil {
		t.Fatalf("register sink: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var g group
	c.startDelivery(ctx, &g)
	c.startExecution(ctx, &g)
	t.Cleanup(func() {
		cancel()
		_ = g.wait(10 * time.Second)
	})

	spec := agentspec.AgentSpec{Name: "docs-bot", Model: "m", Provider: agentspec.ProviderOffline, SystemPrompt: "Answer."}
	for _, tenant := range []string{"acme", "globex"} {
		if _, err := c.store.Create(ctx, tenant, spec, "test"); err != nil {
			t.Fatalf("create agent for %s: %v", tenant, err)
		}
	}

	tagFor := func(tenant string) (envelope.Tag, string) {
		target, err := address.Parse("inbox://" + tenant + "/out")
		if err != nil {
			t.Fatalf("address: %v", err)
		}
		return envelope.Tag{
			ID: "Ev-shared", Tenant: tenant, Agent: "docs-bot", Origin: "test",
			Text: tenant + " asks", Deliver: []envelope.Route{{Target: target}}, At: time.Now(),
		}, target.String()
	}
	acmeTag, acmeOut := tagFor("acme")
	globexTag, globexOut := tagFor("globex")

	a, err := c.Invoke(ctx, acmeTag)
	if err != nil {
		t.Fatalf("invoke for acme: %v", err)
	}
	b, err := c.Invoke(ctx, globexTag)
	if err != nil {
		t.Fatalf("invoke for globex: %v", err)
	}
	if a.RunID == b.RunID {
		t.Fatalf("both tenants were given run %q", a.RunID)
	}

	deadline := time.Now().Add(15 * time.Second)
	for !box.completed(acmeOut) || !box.completed(globexOut) {
		if time.Now().After(deadline) {
			t.Fatalf("runs did not both complete: acme got %d events, globex got %d", len(box.at(acmeOut)), len(box.at(globexOut)))
		}
		time.Sleep(20 * time.Millisecond)
	}

	for _, want := range []struct{ out, tenant, runID string }{{acmeOut, "acme", a.RunID}, {globexOut, "globex", b.RunID}} {
		for _, e := range box.at(want.out) {
			if e.Tenant != want.tenant || e.RunID != want.runID {
				t.Errorf("%s received an event of tenant %q run %q; want only tenant %q run %q", want.out, e.Tenant, e.RunID, want.tenant, want.runID)
			}
		}
	}

	// Each run is readable by its own tenant and absent to the other.
	if run, err := c.Run(ctx, "acme", a.RunID); err != nil || !strings.HasPrefix(run.Tag.Text, "acme") {
		t.Errorf("acme reading its run: %+v, %v", run.Tag.Text, err)
	}
	if _, err := c.Run(ctx, "globex", a.RunID); err == nil {
		t.Error("globex could read acme's run")
	}
}
