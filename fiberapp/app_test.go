package fiberapp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/knowledge"
	"github.com/urmzd/fiber/pkg/registry"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/thread"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// postgresEnv names a Postgres the backend tests run against when set.
const postgresEnv = "FIBER_POSTGRES_TEST_URL"

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

// newApp builds an App in a temporary workspace and closes it when the test
// ends.
func newApp(t *testing.T, cfg Config, opts ...Option) *App {
	t.Helper()
	if cfg.Workspace == "" {
		cfg.Workspace = t.TempDir()
	}
	app, err := New(context.Background(), cfg, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	return app
}

// start runs fn(ctx) until the test ends, and fails the test if it returns an
// error.
func start(t *testing.T, fn func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})
}

func awaitDelivered(t *testing.T, box *inbox, target string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !box.completed(target) {
		if time.Now().After(deadline) {
			t.Fatalf("the run did not complete; delivered %d events to %s", len(box.at(target)), target)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

var offline = agentspec.AgentSpec{Name: "docs-bot", Model: "m", Provider: agentspec.ProviderOffline, SystemPrompt: "Answer."}

// A tag id is whatever the caller sent, so two tenants can send the same one.
// Routes are registered and runs are started under an identity that includes
// the tenant, so the second tenant gets its own run delivered to its own
// target. It does not join the first tenant's run, and it does not replace
// where that run's events go.
func TestATenantReusingAnotherTenantsTagIDCannotJoinOrRerouteItsRun(t *testing.T) {
	box := &inbox{}
	app := newApp(t, Config{Tenant: "acme"}, WithConnector(box))
	start(t, app.Run)
	ctx := context.Background()

	for _, tenant := range []string{"acme", "globex"} {
		if _, err := app.Registry().Create(ctx, tenant, offline, "test"); err != nil {
			t.Fatalf("create agent for %s: %v", tenant, err)
		}
	}
	tagFor := func(tenant string) (envelope.Tag, string) {
		target := address.MustParse("inbox://" + tenant + "/out")
		return envelope.Tag{
			ID: "Ev-shared", Tenant: tenant, Agent: "docs-bot", Origin: "test",
			Text: tenant + " asks", Deliver: []envelope.Route{{Target: target}}, At: time.Now(),
		}, target.String()
	}
	acmeTag, acmeOut := tagFor("acme")
	globexTag, globexOut := tagFor("globex")
	a, err := app.Invoke(ctx, acmeTag)
	if err != nil {
		t.Fatalf("invoke for acme: %v", err)
	}
	b, err := app.Invoke(ctx, globexTag)
	if err != nil {
		t.Fatalf("invoke for globex: %v", err)
	}
	if a.RunID == b.RunID {
		t.Fatalf("both tenants were given run %q", a.RunID)
	}
	awaitDelivered(t, box, acmeOut)
	awaitDelivered(t, box, globexOut)
	for _, want := range []struct{ out, tenant, runID string }{{acmeOut, "acme", a.RunID}, {globexOut, "globex", b.RunID}} {
		for _, e := range box.at(want.out) {
			if e.Tenant != want.tenant || e.RunID != want.runID {
				t.Errorf("%s received an event of tenant %q run %q; want only tenant %q run %q", want.out, e.Tenant, e.RunID, want.tenant, want.runID)
			}
		}
	}
	if run, err := app.Status(ctx, "acme", a.RunID); err != nil || !strings.HasPrefix(run.Tag.Text, "acme") {
		t.Errorf("acme reading its run: %+v, %v", run.Tag.Text, err)
	}
	if _, err := app.Status(ctx, "globex", a.RunID); !errors.Is(err, ErrNotFound) {
		t.Errorf("globex reading acme's run = %v, want ErrNotFound", err)
	}
}

// A run asked to stop before any worker has picked it up ends stopped: its
// surface is delivered lifecycle.stopped and never completed, its record
// reads as stopped with why and who, and another tenant asking for the same
// run changes nothing.
func TestAStopEndsTheRunAndIsDeliveredAsStopped(t *testing.T) {
	box := &inbox{}
	app := newApp(t, Config{Tenant: "acme"}, WithConnector(box))
	start(t, app.RunDelivery)
	ctx := context.Background()
	if _, err := app.Registry().Create(ctx, "acme", offline, "test"); err != nil {
		t.Fatal(err)
	}
	target := address.MustParse("inbox://acme/out")
	acc, err := app.Invoke(ctx, envelope.Tag{
		ID: "Ev-1", Tenant: "acme", Agent: "docs-bot", Origin: "test",
		Text: "explain", Deliver: []envelope.Route{{Target: target}}, At: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range []runtime.StopRequest{
		{Tenant: "globex", RunID: acc.RunID, Reason: "theirs"},
		{Tenant: "acme", RunID: "Ev-1", Reason: "the tag id"},
		{Tenant: "acme", RunID: acc.RunID, Reason: "Stopped by a user", By: "slack:U1"},
	} {
		if err := app.Stop(ctx, req); err != nil {
			t.Fatalf("stop %+v: %v", req, err)
		}
	}
	if err := app.Stop(ctx, runtime.StopRequest{Tenant: "acme", RunID: "not a run id"}); !errors.Is(err, runtime.ErrInvalid) {
		t.Fatalf("stop with a malformed run id = %v, want runtime.ErrInvalid", err)
	}

	// Only now is there a worker.
	start(t, app.RunExecution)
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
	run, err := app.Status(ctx, "acme", acc.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != StatusStopped || !run.Status.Terminal() || run.StopReason != "Stopped by a user" || run.StoppedBy != "slack:U1" {
		t.Errorf("run = status %q reason %q by %q, want stopped by slack:U1", run.Status, run.StopReason, run.StoppedBy)
	}
}

// A run's usage is delivered on its terminal event and read back from the
// ledger by Status, and the two agree. A route that selects usage receives
// the usage.call events.
func TestStatusReadsTheUsageTheRunRecorded(t *testing.T) {
	box := &inbox{}
	app := newApp(t, Config{Tenant: "acme"}, WithConnector(box))
	start(t, app.Run)
	ctx := context.Background()
	if _, err := app.Registry().Create(ctx, "acme", offline, "test"); err != nil {
		t.Fatal(err)
	}
	target := address.MustParse("inbox://acme/out")
	acc, err := app.Invoke(ctx, envelope.Tag{
		ID: "Ev-1", Tenant: "acme", Agent: "docs-bot", Origin: "test",
		Text: "explain", Deliver: []envelope.Route{{Target: target, Kinds: []string{"usage", "lifecycle"}}}, At: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	awaitDelivered(t, box, target.String())
	var delivered *payload.RunUsage
	calls := 0
	for _, e := range box.at(target.String()) {
		switch e.Kind {
		case envelope.KindUsageCall:
			calls++
		case envelope.KindCompleted:
			body, err := payload.Decode[payload.Lifecycle](e)
			if err != nil {
				t.Fatal(err)
			}
			delivered = body.Usage
		}
	}
	if calls == 0 || delivered == nil || delivered.Total.ModelCalls != calls {
		t.Fatalf("delivered %d usage.call events and a completed event with usage %+v", calls, delivered)
	}
	run, err := app.Status(ctx, "acme", acc.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != StatusCompleted || run.Usage == nil || run.Usage.Total.ModelCalls != delivered.Total.ModelCalls {
		t.Fatalf("Status = %q usage %+v, want the usage the run delivered %+v", run.Status, run.Usage, delivered)
	}
}

// One Postgres URL selects Postgres for every store family, and a bus URL
// picks the bus by scheme; empty keeps each in memory.
func TestConfigSelectsEveryBackend(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		app := newApp(t, Config{Tenant: "acme"})
		want := map[string]bool{
			"bus":           isType[*bus.Memory](app.Bus()),
			"registry":      isType[*registry.Memory](app.Registry()),
			"knowledge":     isType[*knowledge.Memory](app.Knowledge()),
			"threads":       isType[*thread.Memory](app.Threads()),
			"tool provider": isType[*toolprovider.Memory](app.ToolProviders()),
		}
		for name, ok := range want {
			if !ok {
				t.Errorf("%s is not the memory backend", name)
			}
		}
	})
	url := os.Getenv(postgresEnv)
	if url == "" {
		return
	}
	t.Run("postgres", func(t *testing.T) {
		tenant := "acme-" + time.Now().Format("150405.000000")
		box := &inbox{}
		app := newApp(t, Config{Tenant: tenant, Postgres: url, Bus: url}, WithConnector(box))
		want := map[string]bool{
			"bus":           isType[*bus.Postgres](app.Bus()),
			"registry":      isType[*registry.Postgres](app.Registry()),
			"knowledge":     isType[*knowledge.Postgres](app.Knowledge()),
			"threads":       isType[*thread.Postgres](app.Threads()),
			"tool provider": isType[*toolprovider.Postgres](app.ToolProviders()),
		}
		for name, ok := range want {
			if !ok {
				t.Errorf("%s is not the postgres backend", name)
			}
		}
		// And a run goes end to end on it.
		start(t, app.Run)
		ctx := context.Background()
		if _, err := app.Registry().Create(ctx, tenant, offline, "test"); err != nil {
			t.Fatal(err)
		}
		target := address.MustParse("inbox://acme/pg")
		acc, err := app.Invoke(ctx, envelope.Tag{ID: "Ev-pg", Tenant: tenant, Agent: "docs-bot", Origin: "test",
			Text: "explain", Deliver: []envelope.Route{{Target: target}}, At: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		awaitDelivered(t, box, target.String())
		if run, err := app.Status(ctx, tenant, acc.RunID); err != nil || run.Status != StatusCompleted {
			t.Fatalf("Status = %+v, %v", run.Status, err)
		}
	})
}

func isType[T any](v any) bool {
	_, ok := v.(T)
	return ok
}

// A configuration that cannot be right fails New with ErrConfig, before
// anything is opened.
func TestAnImpossibleConfigIsErrConfig(t *testing.T) {
	for name, cfg := range map[string]Config{
		"slack with a token and no secret":  {Slack: Slack{Token: "xoxb"}},
		"github with a secret and no token": {GitHub: GitHub{WebhookSecret: "s"}},
		"jira with no secret":               {Jira: Jira{Sites: map[string]string{"a": "https://a"}, Email: "e", Token: "t"}},
		"a bus nothing implements":          {Bus: "kafka://broker"},
		"a providers file that is missing":  {ProvidersFile: "/does/not/exist.json"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg.Workspace = t.TempDir()
			app, err := New(context.Background(), cfg)
			if err == nil {
				_ = app.Close()
			}
			if !errors.Is(err, ErrConfig) {
				t.Fatalf("New = %v, want ErrConfig", err)
			}
		})
	}
}

// Webhook connectors are handlers the embedding program mounts; the App
// serves nothing itself.
func TestWebhookConnectorsAreMountableHandlers(t *testing.T) {
	app := newApp(t, Config{Tenant: "acme", GitHub: GitHub{Token: "ghs_test", WebhookSecret: "secret"}})
	if _, ok := app.Webhooks()["github"]; !ok {
		t.Fatalf("webhooks = %v, want github", app.Webhooks())
	}
	srv := httptest.NewServer(app.WebhookHandler())
	defer srv.Close()
	res, err := http.Post(srv.URL+"/connectors/github", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	// Unsigned, so refused by the connector itself.
	if res.StatusCode < 400 || res.StatusCode >= 500 {
		t.Fatalf("an unsigned delivery got %d, want a 4xx from the connector", res.StatusCode)
	}
}

// Index writes files into the knowledge store an agent's sources read.
func TestIndexGroundsAgentsInFiles(t *testing.T) {
	app := newApp(t, Config{Tenant: "acme"})
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/release.md", []byte("# Release\n\nMerging to main runs the release workflow."), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/logo.png", []byte{0x89, 'P', 'N', 'G'}, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := app.Index(ctx, "acme", "handbook", dir)
	if err != nil {
		t.Fatal(err)
	}
	if res.Files != 1 || res.Changed != 1 {
		t.Fatalf("Index = %+v, want one markdown file indexed", res)
	}
	again, err := app.Index(ctx, "acme", "handbook", dir)
	if err != nil || again.Changed != 0 {
		t.Fatalf("re-indexing unchanged files = %+v, %v; want nothing changed", again, err)
	}
	hits, err := app.Knowledge().Search(ctx, "acme", knowledge.Query{Text: "release workflow", Sources: []string{"handbook"}})
	if err != nil || len(hits) == 0 {
		t.Fatalf("Search = %v, %v", hits, err)
	}
}

// Every run this process executes holds its control subscriptions for its
// whole turn, each a pooled connection on Redis: one for stop requests, and
// one for requests to compact when the deployment takes them.
func TestTheBusPoolReservesEveryControlStreamOfEveryConcurrentRun(t *testing.T) {
	base := poolSize(0, 0) - 1
	for _, tc := range []struct {
		concurrency int
		cfg         Config
		want        int
	}{
		{0, Config{}, base + 1},
		{8, Config{}, base + 8},
		{8, Config{Compaction: agentrt.Compaction{OnRequest: true}}, base + 16},
	} {
		if got := poolSize(tc.concurrency, controlStreams(tc.cfg)); got != tc.want {
			t.Errorf("pool size for %d runs = %d, want %d", tc.concurrency, got, tc.want)
		}
	}
}
