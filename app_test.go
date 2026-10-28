package fiber

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urmzd/anchor/pkg/ledger"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/blob"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/connectors/webhook"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/knowledge"
	"github.com/urmzd/fiber/pkg/membership"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/thread"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// postgresEnv names a Postgres the backend tests run against when set.
const postgresEnv = "FIBER_POSTGRES_TEST_URL"

// inbox is a sink that records what was delivered where. It receives the
// run types of the inbox:// channels, or the channels and types it is given,
// rendered at target when one is set.
type inbox struct {
	channels []string
	types    []string
	target   address.Address

	mu   sync.Mutex
	seen map[string][]event.Envelope
}

func (i *inbox) Name() string { return "inbox" }

func (i *inbox) Subscriptions() []connector.Subscription {
	channels, types := i.channels, i.types
	if channels == nil {
		channels = []string{"inbox://"}
	}
	if types == nil {
		types = []string{"run"}
	}
	return []connector.Subscription{{Channels: channels, Types: types, Target: i.target}}
}

// posted is a message.posted.v1 asking agent, published on channel: where
// the run's events are published, and so where the inbox renders them.
func posted(id, tenant, agent, channel, text string) event.Envelope {
	return event.Envelope{
		ID: id, Source: channel, Type: event.TypeMessagePosted, Channel: channel,
		Time: time.Now().UTC(), Tenant: tenant,
		Data: event.MessagePosted{Request: event.Request{Text: text, Mentions: []string{agent}}},
	}
}

// ask accepts the run of the agent e mentions first.
func ask(ctx context.Context, app *App, e event.Envelope) (runtime.Acceptance, error) {
	r, _ := event.RequestOf(e.Data)
	return app.Runtime().Accept(ctx, e, r.Mentions[0])
}

func (i *inbox) Deliver(_ context.Context, target address.Address, e event.Envelope) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.seen == nil {
		i.seen = map[string][]event.Envelope{}
	}
	i.seen[target.String()] = append(i.seen[target.String()], e)
	return nil
}

func (i *inbox) at(target string) []event.Envelope {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]event.Envelope(nil), i.seen[target]...)
}

// completed reports whether target has received a run's final event.
func (i *inbox) completed(target string) bool {
	for _, e := range i.at(target) {
		if e.Type == event.TypeCompleted {
			return true
		}
	}
	return false
}

// newApp builds an App and closes it when the test ends.
func newApp(t *testing.T, opts ...Option) *App {
	t.Helper()
	app, err := New(context.Background(), opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	return app
}

var schemas atomic.Uint64

// testPool returns a pool on $FIBER_POSTGRES_TEST_URL whose search_path is a
// schema of its own, dropped when the test ends, or skips the test when the
// variable is unset. The variable only finds the test database; the App is
// handed the pool.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("set %s to run against Postgres", postgresEnv)
	}
	ctx := context.Background()
	schema := fmt.Sprintf("fibertest_app_%d_%d", time.Now().UnixNano(), schemas.Add(1))
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public" // pgvector lives in public
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+ident+" CASCADE")
		pool.Close()
	})
	return pool
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

// awaitEnded polls Status until the run is terminal and returns it. A run
// publishes its terminal event from inside its turn, and the worker records
// how the run ended in the ledger only after the turn returns, so a Status
// read the moment that event is delivered can still say accepted.
func awaitEnded(t *testing.T, app *App, tenant, runID string) Run {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		run, err := app.Status(context.Background(), tenant, runID)
		if err != nil {
			t.Fatalf("Status(%s): %v", runID, err)
		}
		if run.Status.Terminal() {
			return run
		}
		if time.Now().After(deadline) {
			t.Fatalf("the ledger never recorded run %s as ended: status %q", runID, run.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

var offline = definition.AgentSpec{Name: "docs-bot", Model: "m", Provider: definition.ProviderOffline, SystemPrompt: "Answer."}

// An event id is whatever the caller sent, so two tenants can send the same
// one. Runs are started and delivered under an identity that includes the
// tenant, so the second tenant gets its own run delivered on its own
// channel. It does not join the first tenant's run, and it does not receive
// that run's events.
func TestATenantReusingAnotherTenantsEventIDCannotJoinOrReceiveItsRun(t *testing.T) {
	box := &inbox{}
	app := newApp(t, WithTenant("acme"), WithConnector(box))
	start(t, app.Run)
	start(t, func(ctx context.Context) error { return app.Egress().Run(ctx, "globex") })
	ctx := context.Background()

	for _, tenant := range []string{"acme", "globex"} {
		if _, err := createAgent(app.Definitions(), ctx, tenant, offline, "test"); err != nil {
			t.Fatalf("create agent for %s: %v", tenant, err)
		}
	}
	acmeOut, globexOut := "inbox://acme/out", "inbox://globex/out"
	a, err := ask(ctx, app, posted("Ev-shared", "acme", "docs-bot", acmeOut, "acme asks"))
	if err != nil {
		t.Fatalf("accept for acme: %v", err)
	}
	b, err := ask(ctx, app, posted("Ev-shared", "globex", "docs-bot", globexOut, "globex asks"))
	if err != nil {
		t.Fatalf("accept for globex: %v", err)
	}
	if a.RunID == b.RunID {
		t.Fatalf("both tenants were given run %q", a.RunID)
	}
	awaitDelivered(t, box, acmeOut)
	awaitDelivered(t, box, globexOut)
	for _, want := range []struct{ out, tenant, runID string }{{acmeOut, "acme", a.RunID}, {globexOut, "globex", b.RunID}} {
		for _, e := range box.at(want.out) {
			if e.Tenant != want.tenant || e.Run != want.runID {
				t.Errorf("%s received an event of tenant %q run %q; want only tenant %q run %q", want.out, e.Tenant, e.Run, want.tenant, want.runID)
			}
		}
	}
	if run, err := app.Status(ctx, "acme", a.RunID); err != nil || !strings.HasPrefix(run.Trigger.Data.(event.MessagePosted).Text, "acme") {
		t.Errorf("acme reading its run: %+v, %v", run.Trigger, err)
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
	app := newApp(t, WithTenant("acme"), WithConnector(box))
	start(t, app.RunDelivery)
	ctx := context.Background()
	if _, err := createAgent(app.Definitions(), ctx, "acme", offline, "test"); err != nil {
		t.Fatal(err)
	}
	target := address.MustParse("inbox://acme/out")
	acc, err := ask(ctx, app, posted("Ev-1", "acme", "docs-bot", target.String(), "explain"))
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range []runtime.StopRequest{
		{Tenant: "globex", RunID: acc.RunID, Reason: "theirs"},
		{Tenant: "acme", RunID: "Ev-1", Reason: "the event id"},
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
	var last event.Envelope
	deadline := time.Now().Add(15 * time.Second)
	for {
		events := box.at(target.String())
		if n := len(events); n > 0 && events[n-1].Type == event.TypeStopped {
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
	if !strings.Contains(bodyJSON(last), "Stopped by a user") || !strings.Contains(bodyJSON(last), "slack:U1") {
		t.Errorf("run.stopped body = %s, want the acme request", bodyJSON(last))
	}
	run := awaitEnded(t, app, "acme", acc.RunID)
	if run.Status != StatusStopped || !run.Status.Terminal() || run.StopReason != "Stopped by a user" || run.StoppedBy != "slack:U1" {
		t.Errorf("run = status %q reason %q by %q, want stopped by slack:U1", run.Status, run.StopReason, run.StoppedBy)
	}
}

// A run's usage is delivered on its terminal event and read back from the
// ledger by Status, and the two agree. A sink that subscribes to usage
// receives the usage.call events.
func TestStatusReadsTheUsageTheRunRecorded(t *testing.T) {
	box := &inbox{types: []string{"run.usage", "run.accepted", "run.started", "run.completed", "run.failed", "run.stopped"}}
	app := newApp(t, WithTenant("acme"), WithConnector(box))
	start(t, app.Run)
	ctx := context.Background()
	if _, err := createAgent(app.Definitions(), ctx, "acme", offline, "test"); err != nil {
		t.Fatal(err)
	}
	target := address.MustParse("inbox://acme/out")
	acc, err := ask(ctx, app, posted("Ev-1", "acme", "docs-bot", target.String(), "explain"))
	if err != nil {
		t.Fatal(err)
	}
	awaitDelivered(t, box, target.String())
	var delivered *event.RunUsage
	calls := 0
	for _, e := range box.at(target.String()) {
		switch b := e.Data.(type) {
		case event.UsageCall:
			calls++
		case event.Completed:
			delivered = b.Usage
		}
	}
	if calls == 0 || delivered == nil || delivered.Total.ModelCalls != calls {
		t.Fatalf("delivered %d usage.call events and a completed event with usage %+v", calls, delivered)
	}
	run := awaitEnded(t, app, "acme", acc.RunID)
	if run.Status != StatusCompleted || run.Usage == nil || run.Usage.Total.ModelCalls != delivered.Total.ModelCalls {
		t.Fatalf("Status = %q usage %+v, want the usage the run delivered %+v", run.Status, run.Usage, delivered)
	}
}

// Every backend not given is the in-memory one, and WithPostgres puts every
// store family on the pool it is handed, migrated by New.
func TestEveryBackendIsMemoryUnlessGivenAndPostgresFillsEveryStore(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		app := newApp(t, WithTenant("acme"))
		for name, ok := range map[string]bool{
			"bus":           isType[*bus.Memory](app.Bus()),
			"definitions":   isType[*definition.Memory](app.Definitions()),
			"knowledge":     isType[*knowledge.Memory](app.Knowledge()),
			"threads":       isType[*thread.Memory](app.Threads()),
			"tool provider": isType[*toolprovider.Memory](app.ToolProviders()),
			"blobs":         isType[*blob.Memory](app.Blobs()),
		} {
			if !ok {
				t.Errorf("%s is not the memory backend", name)
			}
		}
	})
	t.Run("postgres", func(t *testing.T) {
		pool := testPool(t)
		pg := bus.NewPostgres(pool)
		if err := pg.Migrate(context.Background()); err != nil {
			t.Fatal(err)
		}
		box := &inbox{}
		app := newApp(t, WithTenant("acme"), WithPostgres(pool), WithBus(pg), WithConnector(box))
		for name, ok := range map[string]bool{
			"bus":           isType[*bus.Postgres](app.Bus()),
			"definitions":   isType[*definition.Postgres](app.Definitions()),
			"knowledge":     isType[*knowledge.Postgres](app.Knowledge()),
			"threads":       isType[*thread.Postgres](app.Threads()),
			"tool provider": isType[*toolprovider.Postgres](app.ToolProviders()),
		} {
			if !ok {
				t.Errorf("%s is not the postgres backend", name)
			}
		}
		// And a run goes end to end on it.
		start(t, app.Run)
		ctx := context.Background()
		if _, err := createAgent(app.Definitions(), ctx, "acme", offline, "test"); err != nil {
			t.Fatal(err)
		}
		target := address.MustParse("inbox://acme/pg")
		acc, err := ask(ctx, app, posted("Ev-pg", "acme", "docs-bot", target.String(), "explain"))
		if err != nil {
			t.Fatal(err)
		}
		awaitDelivered(t, box, target.String())
		if run := awaitEnded(t, app, "acme", acc.RunID); run.Status != StatusCompleted {
			t.Fatalf("Status = %q (%s), want completed", run.Status, run.Error)
		}
	})
}

func isType[T any](v any) bool {
	_, ok := v.(T)
	return ok
}

// A composition that cannot be right fails New with ErrConfig, before
// anything runs. A trigger connector with no tenant is one: it would resolve
// no mention and raise no tag a run could be accepted for.
func TestAnImpossibleCompositionIsErrConfig(t *testing.T) {
	issues := toolprovider.Provider{Name: "issues", Transport: toolprovider.TransportHTTP, Endpoint: "https://mcp.example.com"}
	for name, opts := range map[string][]Option{
		"slack with a token and no secret":  {WithSlack(Slack{Token: "xoxb"})},
		"github with a secret and no token": {WithGitHub(GitHub{WebhookSecret: "s"})},
		"jira with no secret":               {WithJira(Jira{Sites: map[string]string{"a": "https://a"}, Email: "e", Token: "t"})},
		"slack with no tenant":              {WithSlack(Slack{Token: "xoxb", SigningSecret: "s"})},
		"github with no tenant":             {WithGitHub(GitHub{Token: "ghs", WebhookSecret: "s"})},
		"jira with no tenant":               {WithJira(Jira{Sites: map[string]string{"a": "https://a"}, Email: "e", Token: "t", WebhookSecret: "s"})},
		"a webhook with no secret":          {WithWebhooks(map[string]webhook.Endpoint{"acme": {BaseURL: "https://hooks"}})},
		"a ledger with no queue":            {WithDurable(ledger.NewMemory(), nil)},
		"a tool provider declared twice":    {WithTenant("acme"), WithToolProvider(issues), WithToolProvider(issues)},
		"a tool pinned with no hash":        {WithTenant("acme"), WithToolProvider(issues, toolprovider.Tool{Name: "search"})},
	} {
		t.Run(name, func(t *testing.T) {
			app, err := New(context.Background(), opts...)
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
	app := newApp(t, WithTenant("acme"), WithGitHub(GitHub{Token: "ghs_test", WebhookSecret: "secret"}))
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
	app := newApp(t, WithTenant("acme"))
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

// An event published twice wakes each member once: the redelivery joins the
// run the first delivery started, and the run's events go to the sink that
// subscribes to the event's channel.
func TestRedeliveredEventJoinsItsRun(t *testing.T) {
	box := &inbox{channels: []string{"slack://T1/C1"}, target: address.MustParse("inbox://acme/out")}
	app := newApp(t, WithTenant("acme"), WithConnector(box))
	start(t, app.Run)
	ctx := context.Background()
	if _, err := createAgent(app.Definitions(), ctx, "acme", offline, "test"); err != nil {
		t.Fatal(err)
	}
	const channel = "slack://T1/C1"
	if err := app.Memberships().Join(ctx, "acme", channel, "docs-bot", membership.Rule{Types: []string{"message"}}); err != nil {
		t.Fatal(err)
	}
	e := event.Envelope{
		ID: "Ev-redelivered", Source: "inbox://acme/out", Type: event.TypeMessagePosted, Channel: channel,
		Time: time.Now().UTC(), Data: event.MessagePosted{Request: event.Request{Text: "explain", Mentions: []string{"docs-bot"}}},
	}
	for range 2 {
		if err := app.Publish(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	awaitDelivered(t, box, "inbox://acme/out")
	runID := runtime.RunKey("acme", e.ID, "docs-bot")
	run := awaitEnded(t, app, "acme", runID)
	if run.Agent != "docs-bot" || run.Channel != channel {
		t.Fatalf("run = %+v", run)
	}
	// The bus keeps an event once by its id, so a redelivery is what a
	// resubscribe replays: handed to the dispatcher again, it joins the run.
	e.Tenant = "acme"
	accs, err := app.Dispatcher().Handle(ctx, e)
	if err != nil || len(accs) != 1 || accs[0].RunID != runID {
		t.Fatalf("redelivery = %v, %v; want run %s again", accs, err, runID)
	}
	if again := awaitEnded(t, app, "acme", runID); !again.CreatedAt.Equal(run.CreatedAt) {
		t.Fatalf("the redelivery replaced the run: created %v, then %v", run.CreatedAt, again.CreatedAt)
	}
	runs := map[string]bool{}
	for _, got := range box.at("inbox://acme/out") {
		runs[got.Run] = true
		if got.Hops != 1 {
			t.Errorf("%s carries %d hops; want the trigger's plus one", got.Type, got.Hops)
		}
	}
	if len(runs) != 1 || !runs[runID] {
		t.Fatalf("delivered runs %v; want only %s", runs, runID)
	}
}
