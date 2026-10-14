package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/gen/mandatum/v1/mandatumv1connect"
	"github.com/urmzd/mandatum/internal/server"
	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/agentspec"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/runtime"
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

// Stop, end to end through the composition root: a run that is asked to stop
// before any worker has picked it up ends stopped, its surface is delivered
// lifecycle.stopped and never completed, its record reads as stopped, and
// another tenant asking for the same run changes nothing.
func TestAStopThroughTheCoreEndsTheRunAndIsDeliveredAsStopped(t *testing.T) {
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
	t.Cleanup(func() {
		cancel()
		_ = g.wait(10 * time.Second)
	})

	spec := agentspec.AgentSpec{Name: "docs-bot", Model: "m", Provider: agentspec.ProviderOffline, SystemPrompt: "Answer."}
	if _, err := c.store.Create(ctx, "acme", spec, "test"); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	target, err := address.Parse("inbox://acme/out")
	if err != nil {
		t.Fatal(err)
	}
	acc, err := c.Invoke(ctx, envelope.Tag{
		ID: "Ev-1", Tenant: "acme", Agent: "docs-bot", Origin: "test",
		Text: "explain", Deliver: []envelope.Route{{Target: target}}, At: time.Now(),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	// Another tenant, and the tag's own id, address nothing.
	if err := c.Stop(ctx, server.StopRequest{Tenant: "globex", RunID: acc.RunID, Reason: "theirs"}); err != nil {
		t.Fatalf("stop from another tenant: %v", err)
	}
	if err := c.Stop(ctx, server.StopRequest{Tenant: "acme", RunID: "Ev-1", Reason: "the tag id"}); err != nil {
		t.Fatalf("stop by tag id: %v", err)
	}
	if err := c.Stop(ctx, server.StopRequest{Tenant: "acme", RunID: acc.RunID, Reason: "Stopped by a user", By: "slack:U1"}); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := c.Stop(ctx, server.StopRequest{Tenant: "acme", RunID: "not a run id"}); !errors.Is(err, server.ErrInvalid) {
		t.Fatalf("stop with a malformed run id = %v, want server.ErrInvalid", err)
	}

	// Only now is there a worker.
	c.startExecution(ctx, &g)

	var last envelope.Event
	deadline := time.Now().Add(15 * time.Second)
	for {
		events := box.at(target.String())
		if n := len(events); n > 0 && events[n-1].Kind == envelope.KindStopped {
			last = events[n-1]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the run was never delivered as stopped: %d events", len(events))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if box.completed(target.String()) {
		t.Fatal("a stopped run was delivered as completed")
	}
	if !strings.Contains(string(last.Payload), "Stopped by a user") || !strings.Contains(string(last.Payload), "slack:U1") {
		t.Errorf("lifecycle.stopped payload = %s, want the acme request", last.Payload)
	}
	for _, e := range box.at(target.String()) {
		if e.Kind == envelope.KindStop {
			t.Fatal("the stop request itself was delivered to the surface")
		}
	}

	run, err := c.Run(ctx, "acme", acc.RunID)
	if err != nil {
		t.Fatalf("read run: %v", err)
	}
	if run.Status != server.StatusStopped || run.StopReason != "Stopped by a user" || run.StoppedBy != "slack:U1" {
		t.Errorf("run = status %q reason %q by %q, want stopped by slack:U1", run.Status, run.StopReason, run.StoppedBy)
	}
	if _, err := c.Run(ctx, "globex", acc.RunID); err == nil {
		t.Error("another tenant could read the run")
	}
}

// The edge in front of the real runtime: a stop for the caller's own run, for
// another tenant's run, and for a run that does not exist are answered with
// the same status and the same body, and only the first one does anything. The
// stopped run is then read back through GetRun as stopped, with why and who.
func TestTheStopRouteIsUniformAndGetRunReadsTheRunAsStopped(t *testing.T) {
	c, err := newCore(coreConfig{Tenant: "acme", Workspace: t.TempDir()})
	if err != nil {
		t.Fatalf("newCore: %v", err)
	}
	t.Cleanup(func() { _ = c.close() })
	auth, err := server.NewTokens(map[string]server.Identity{
		"tok-acme":   {Tenant: "acme", Subject: "ada"},
		"tok-globex": {Tenant: "globex", Subject: "mallory"},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Config{Auth: auth, Bus: c.broker, Invoker: c, Runs: c, Stopper: c})
	if err != nil {
		t.Fatal(err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	var g group
	t.Cleanup(func() {
		cancel()
		_ = g.wait(10 * time.Second)
	})
	spec := agentspec.AgentSpec{Name: "docs-bot", Model: "m", Provider: agentspec.ProviderOffline, SystemPrompt: "Answer."}
	if _, err := c.store.Create(ctx, "acme", spec, "test"); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	acc, err := c.Invoke(ctx, envelope.Tag{ID: "Ev-1", Tenant: "acme", Agent: "docs-bot", Origin: "test", Text: "explain", At: time.Now()})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	post := func(runID, token string) string {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/runs/"+runID+"/stop", strings.NewReader(`{"reason":"wrong repo","by":"someone-else"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(server.HeaderAuthorization, "Bearer "+token)
		res, err := httpSrv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = res.Body.Close() }()
		body, _ := io.ReadAll(res.Body)
		return fmt.Sprintf("%d %s", res.StatusCode, strings.ReplaceAll(string(body), runID, "<id>"))
	}
	unknown := runtime.RunID("acme", "never")
	foreign := post(acc.RunID, "tok-globex")
	missing := post(unknown, "tok-acme")
	own := post(acc.RunID, "tok-acme")
	if own != foreign || own != missing || !strings.HasPrefix(own, "202 ") {
		t.Fatalf("answers differ: own %q, another tenant %q, unknown %q", own, foreign, missing)
	}

	c.startExecution(ctx, &g)
	runs := mandatumv1connect.NewInvokeServiceClient(httpSrv.Client(), httpSrv.URL)
	deadline := time.Now().Add(15 * time.Second)
	for {
		req := connect.NewRequest(&mandatumv1.GetRunRequest{RunId: acc.RunID})
		req.Header().Set(server.HeaderAuthorization, "Bearer tok-acme")
		got, err := runs.GetRun(ctx, req)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if got.Msg.GetStatus() == mandatumv1.RunStatus_RUN_STATUS_STOPPED {
			// The body named someone else; an ordinary credential is itself.
			if got.Msg.GetStopReason() != "wrong repo" || got.Msg.GetStoppedBy() != "ada" {
				t.Fatalf("stop_reason %q stopped_by %q", got.Msg.GetStopReason(), got.Msg.GetStoppedBy())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the run never read as stopped: %v", got.Msg.GetStatus())
		}
		time.Sleep(20 * time.Millisecond)
	}
	req := connect.NewRequest(&mandatumv1.GetRunRequest{RunId: acc.RunID})
	req.Header().Set(server.HeaderAuthorization, "Bearer tok-globex")
	if _, err := runs.GetRun(ctx, req); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("another tenant reading the run = %v, want not found", err)
	}
}
