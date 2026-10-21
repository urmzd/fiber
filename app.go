package fiber

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urmzd/anchor"
	"github.com/urmzd/anchor/pkg/ledger"
	dqueue "github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/worker"
	"github.com/urmzd/playscape/pkg/controlplane"
	"github.com/urmzd/playscape/pkg/metrics"
	"github.com/urmzd/playscape/pkg/node/inproc"
	"github.com/urmzd/playscape/pkg/tool"
	"github.com/urmzd/playscape/pkg/workspace"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/attachment/resolve"
	"github.com/urmzd/fiber/pkg/blob"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/connectors/author"
	"github.com/urmzd/fiber/pkg/durable"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/knowledge"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/registry"
	"github.com/urmzd/fiber/pkg/router"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/thread"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// App is fiber assembled: every long-lived component a process owns, in the
// order data flows through them.
//
//	registry  →  runtime  →  bus  →  router  →  connector sinks
//	                ↑                              ↓
//	            anchor worker              a surface somewhere
//
// Nothing here is a stub standing in for a deployment. The in-memory
// registry, bus, ledger and queue are complete implementations of the same
// interfaces the shared backends implement; which one runs is an Option that
// passes a constructed value, never a change to anything below it.
//
// An App listens on nothing. Connectors that receive webhooks are exposed as
// http.Handlers (Webhooks, WebhookHandler) for the embedding program to mount.
type App struct {
	opt options
	log *slog.Logger

	store   registry.Store
	docs    knowledge.Store
	threads thread.Store
	tools   toolprovider.Store
	blobs   blob.Store
	broker  bus.Bus
	sinks   *connector.Registry
	deliver *router.Router
	rt      *runtime.Runtime
	ledger  Ledger
	queue   dqueue.Queue

	webhooks map[string]http.Handler

	closeOnce sync.Once
	closers   []func() error
}

// Ledger is the run ledger an App executes from: anchor's ledger plus the
// point read Status polls with. ledger.NewMemory and durable.Open's ledger
// both satisfy it.
type Ledger interface {
	ledger.Ledger
	ledger.RunGetter
}

// New assembles the system from opts and returns before anything runs: call
// Run to start delivery and execution, and Close to release what New made.
// Every backend not passed by an option is the in-memory implementation;
// WithPostgres puts every store family on one pool, migrated here. New opens
// no connection of its own and reads no environment or file.
//
// It is deliberately linear: read it top to bottom and you have read how
// fiber fits together.
func New(ctx context.Context, opts ...Option) (*App, error) {
	o := apply(opts)
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	if o.Metrics == nil {
		o.Metrics = metrics.Nop()
	}
	a := &App{opt: o, log: o.Log, webhooks: map[string]http.Handler{}}
	fail := func(err error) (*App, error) {
		_ = a.Close()
		return nil, err
	}
	if err := checkSeeds(o.Tenant, o.ToolProviders); err != nil {
		return nil, err
	}
	if o.Workspace == "" {
		dir, err := os.MkdirTemp("", "fiber-")
		if err != nil {
			return nil, fmt.Errorf("fiber: workspace: %w", err)
		}
		a.opt.Workspace = dir
		a.closers = append(a.closers, func() error { return os.RemoveAll(dir) })
	}
	pool := o.Postgres
	var err error

	// The broker. Runs publish here; the router and every subscriber read
	// from here. Nothing else connects them.
	a.broker = o.Bus
	if a.broker == nil {
		a.broker = bus.NewMemory()
	}

	// The control plane: append-only agent definitions, scoped by tenant.
	// Its provider table includes the options', so an agent naming one can
	// be stored as well as run.
	if a.store, err = newRegistry(ctx, pool, o); err != nil {
		return fail(err)
	}
	// What agents know: the corpus a revision's sources are retrieved from.
	if a.docs, err = newKnowledge(ctx, pool, o); err != nil {
		return fail(err)
	}
	// Files. Content-addressed and tenant-scoped.
	a.blobs = o.Blobs
	if a.blobs == nil {
		a.blobs = blob.NewMemory()
	}
	// What a thread remembers: the conversation a run continues.
	if a.threads, err = newThreads(ctx, pool, o); err != nil {
		return fail(err)
	}
	// External tools: the MCP servers a tenant may reach and the tools an
	// admin accepted from each.
	if a.tools, err = newToolProviders(ctx, pool, o); err != nil {
		return fail(err)
	}
	// Sessions are opened on first use and kept per provider record version,
	// and for a person-scoped provider per person, with that person's own
	// credential. A credential reference resolves from this process's
	// environment ("env:NAME") and never reaches a log or an error.
	dial := mcptools.Dialer(mcptools.WithResolver(mcptools.EnvResolver()), mcptools.WithLogger(a.log))
	grants := mcptools.NewGrants(a.tools, dial, mcptools.WithPersonResolver(o.Persons))
	a.closers = append(a.closers, grants.Close)
	if err := seedToolProviders(ctx, a.tools, o.Tenant, o.ToolProviders, dial, a.log); err != nil {
		return fail(err)
	}

	// The mesh. Roles are derived from the interfaces each connector
	// implements, never declared, so registering is the whole wiring.
	if a.sinks, err = buildConnectors(o, a.store, a.blobs); err != nil {
		return fail(err)
	}
	// The author: the control plane offered to an agent as tools, and the
	// agent that holds them.
	if err := a.addAuthor(ctx); err != nil {
		return fail(err)
	}
	for _, t := range a.sinks.Triggers() {
		if h, ok := t.(http.Handler); ok {
			a.webhooks[t.Name()] = h
		}
	}

	// The router is just another bus subscriber. That indirection is why
	// origin and destination stay independent.
	a.deliver = router.New(a.broker, a.sinks, router.WithLogger(a.log))

	// The agent side. A turn runs as a playscape task under the NGAC policy
	// compiled from the pinned revision's Access grant.
	exec := o.Executor
	if exec == nil {
		if exec, err = a.newExecutor(grants); err != nil {
			return fail(err)
		}
	}

	// Durable execution: the ledger is truth, the queue is disposable flow.
	if a.ledger, a.queue, err = newDurable(ctx, pool, o); err != nil {
		return fail(err)
	}
	rtOpts := []runtime.Option{
		runtime.WithLogger(a.log),
		runtime.WithThreads(a.threads),
		// Before a run's history is pinned, its thread catches up with what
		// people said on the surface without tagging the agent.
		runtime.WithReconciler(listers(a.sinks)),
		runtime.WithMessageID(surfaceMessageID),
		// A stop asked for before a run's turn starts is recorded on the
		// run, so it does not depend on the bus still holding it.
		runtime.WithStopLedger(a.ledger),
		// An answered or stopped question wakes its run at once.
		runtime.WithWaker(a.queue),
	}
	if pool != nil && o.Ledger == nil {
		rtOpts = append(rtOpts, runtime.WithRecordReader(durable.NewRecords(pool)))
	}
	if o.Compaction.OnRequest {
		rtOpts = append(rtOpts, runtime.WithCompactRequests())
	}
	rtOpts = append(rtOpts, o.RuntimeOpts...)
	if a.rt, err = runtime.New(anchor.New(a.ledger, a.queue), registry.NewSpecs(a.store), a.broker, exec, rtOpts...); err != nil {
		return fail(fmt.Errorf("fiber: runtime: %w", err))
	}
	return a, nil
}

// Close releases what New made: MCP sessions and a workspace New created.
// Every backend passed by an option, the Postgres pool included, is the
// caller's and is left alone. It is idempotent. Stop Run first.
func (a *App) Close() error {
	var err error
	a.closeOnce.Do(func() {
		for i := len(a.closers) - 1; i >= 0; i-- {
			err = errors.Join(err, a.closers[i]())
		}
	})
	return err
}

// ControlStreams is how many control subscriptions a running turn holds for
// its whole turn: one for stop requests, and one more for requests to
// compact when compactOnRequest. On Redis each is a pooled connection, so a
// program sizing a Redis client for an App wants
// bus.RedisPoolSize(concurrency, ControlStreams(compactOnRequest)).
func ControlStreams(compactOnRequest bool) int {
	if compactOnRequest {
		return 2
	}
	return 1
}

func migrateErr(what string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrUnavailable, what, err)
}

// newRegistry picks the control plane's store: the one passed, Postgres on
// the pool, or memory. The Postgres schema is migrated here because a
// deployment against an unmigrated database has nothing useful to do but
// report that.
func newRegistry(ctx context.Context, pool *pgxpool.Pool, o options) (registry.Store, error) {
	if o.Registry != nil {
		return o.Registry, nil
	}
	providers := registry.WithProviders(o.providerNames()...)
	if pool == nil {
		return registry.NewMemory(providers), nil
	}
	store := registry.NewPostgres(pool, providers)
	if err := store.Migrate(ctx); err != nil {
		return nil, migrateErr("registry", err)
	}
	return store, nil
}

// newKnowledge picks the knowledge store; the embedder applies to the one New
// builds.
func newKnowledge(ctx context.Context, pool *pgxpool.Pool, o options) (knowledge.Store, error) {
	if o.Knowledge != nil {
		return o.Knowledge, nil
	}
	emb := o.Embedder
	if emb == nil {
		emb = knowledge.NewHashing(0)
	}
	if pool == nil {
		return knowledge.NewMemory(emb), nil
	}
	store := knowledge.NewPostgres(pool, emb)
	if err := store.Migrate(ctx); err != nil {
		return nil, migrateErr("knowledge", err)
	}
	return store, nil
}

// newThreads picks the conversation journal.
func newThreads(ctx context.Context, pool *pgxpool.Pool, o options) (thread.Store, error) {
	if o.Threads != nil {
		return o.Threads, nil
	}
	if pool == nil {
		return thread.NewMemory(), nil
	}
	store := thread.NewPostgres(pool)
	if err := store.Migrate(ctx); err != nil {
		return nil, migrateErr("threads", err)
	}
	return store, nil
}

// newToolProviders picks the MCP tool provider store.
func newToolProviders(ctx context.Context, pool *pgxpool.Pool, o options) (toolprovider.Store, error) {
	if o.Tools != nil {
		return o.Tools, nil
	}
	if pool == nil {
		return toolprovider.NewMemory(), nil
	}
	store := toolprovider.NewPostgres(pool)
	if err := store.Migrate(ctx); err != nil {
		return nil, migrateErr("tool providers", err)
	}
	return store, nil
}

// newDurable picks the run ledger and queue. On Postgres a run accepted
// before a restart is still there after it, and its lease expires and is
// reclaimed by whichever worker comes up next.
func newDurable(ctx context.Context, pool *pgxpool.Pool, o options) (Ledger, dqueue.Queue, error) {
	if o.Ledger != nil {
		if o.Queue == nil {
			return nil, nil, configf("WithDurable needs both a ledger and a queue")
		}
		return o.Ledger, o.Queue, nil
	}
	if pool == nil {
		return ledger.NewMemory(), dqueue.NewMemory(), nil
	}
	lgr, q, err := durable.Open(ctx, pool)
	if err != nil {
		return nil, nil, migrateErr("run ledger", err)
	}
	return lgr, q, nil
}

// surfaceMessageID names the surface message that raised a tag, from the
// metadata the triggers already set, so a later reconcile recognises it
// instead of journaling it a second time.
func surfaceMessageID(tag envelope.Tag) string {
	switch tag.Origin {
	case "slack":
		return tag.Meta["slack_ts"]
	case "github":
		return tag.Meta["github_comment_id"]
	}
	return ""
}

// newExecutor builds the sandbox a turn runs in.
//
// The tool registry and the workspace are shared with the control plane
// rather than owned by the sandbox: playscape resolves a tool by name on the
// node that executes it. The policy that scopes what that tool can see comes
// from the pinned revision, one deployment per (agent, revision), which is
// what stops a revised Access grant from widening a run already in flight.
func (a *App) newExecutor(groups runtime.ToolGroupSource) (runtime.Executor, error) {
	dir := a.opt.Workspace
	ws, err := workspace.NewLocal(dir)
	if err != nil {
		return nil, fmt.Errorf("fiber: workspace %q: %w", dir, err)
	}
	tools := tool.NewRegistry()
	plane := controlplane.NewMemory(inproc.NewFactory(tools, ws), a.opt.Metrics)

	agentOpts := []agentrt.Option{agentrt.WithAttachments(resolve.New(a.blobs))}
	// Extension providers come after the built-ins, so one named like a
	// built-in replaces it.
	for _, name := range slices.Sorted(maps.Keys(a.opt.Providers)) {
		agentOpts = append(agentOpts, agentrt.WithProvider(name, a.opt.Providers[name]))
	}
	if c := a.opt.Compaction; c.MaxInputTokens > 0 || c.OnRequest {
		agentOpts = append(agentOpts, agentrt.WithCompaction(c))
	}
	exec, err := runtime.NewSandbox(plane, tools, registry.NewSpecs(a.store),
		runtime.WithAgentOptions(agentOpts...),
		// A spec's tool names resolve against the actions every registered
		// connector advertises, then against the tool sources options added.
		runtime.WithCatalog(catalog(runtime.Connectors(a.sinks), a.opt.Catalogs)),
		// "mcp:<provider>" grants expand to that provider's accepted tools.
		runtime.WithToolGroupSource(groups),
		runtime.WithKnowledge(retrieverFor(a.docs)),
		runtime.WithSandboxLogger(a.log),
	)
	if err != nil {
		return nil, fmt.Errorf("fiber: sandbox: %w", err)
	}
	return exec, nil
}

// addAuthor registers the author connector and seeds the author agent for the
// deployment's tenant. Seeding is safe on every start: an unchanged
// definition is a no-op, and one an operator has edited or deleted is left
// alone.
func (a *App) addAuthor(ctx context.Context) error {
	au, err := author.New(author.Config{
		Store:      a.store,
		Providers:  a.opt.providerNames(),
		Principals: author.PrincipalFunc(a.principal),
		Catalog: func() []string {
			var names []string
			for _, act := range a.sinks.Actions() {
				names = append(names, act.Name)
			}
			return names
		},
	})
	if err != nil {
		return fmt.Errorf("fiber: author: %w", err)
	}
	if err := a.sinks.Register(au); err != nil {
		return fmt.Errorf("fiber: register %s: %w", au.Name(), err)
	}
	if a.opt.Tenant == "" {
		return nil
	}
	provider, model := a.opt.AuthorProvider, a.opt.AuthorModel
	if provider == "" {
		provider = agentspec.ProviderOffline
	}
	if model == "" {
		model = "claude-sonnet-5-5"
	}
	if _, err := author.Seed(ctx, a.store, a.opt.Tenant, author.Spec(model, provider)); err != nil {
		return fmt.Errorf("fiber: seed author: %w", err)
	}
	return nil
}

// principal resolves a run to the tenant and actor on its accepted tag. It is
// the only way an action learns who it acts as, and it reads the ledger: the
// tag there was stamped by whoever accepted it, which is the one account of
// identity a model cannot rewrite.
func (a *App) principal(ctx context.Context, runID string) (author.Principal, error) {
	if a.ledger == nil {
		return author.Principal{}, errors.New("fiber: run ledger not ready")
	}
	rec, err := a.ledger.GetRun(ctx, runID)
	if err != nil {
		return author.Principal{}, fmt.Errorf("fiber: read run %q: %w", runID, err)
	}
	var in runtime.Input
	if err := json.Unmarshal(rec.Input, &in); err != nil {
		return author.Principal{}, fmt.Errorf("fiber: decode run %q input: %w", runID, err)
	}
	p := author.Principal{Tenant: in.Tag.Tenant}
	if in.Tag.Actor.ID != "" {
		p.Actor = in.Tag.Origin + ":" + in.Tag.Actor.ID
	}
	return p, nil
}

// Invoke accepts a tag: it registers the tag's routes with the router and
// then accepts the run durably. It returns before any inference.
//
// The order matters: routes are registered before the run is accepted,
// because accept publishes the run's first event and an event for a run the
// router has no routes for has nowhere to go. RunID is a pure function of the
// tenant and tag id, so the routes can be registered before the run exists.
//
// The tag's Tenant and Actor are trusted as given. An embedding program that
// accepts tags from callers must set both from the caller's credential,
// never from a request body.
func (a *App) Invoke(ctx context.Context, tag envelope.Tag) (runtime.Acceptance, error) {
	if err := a.deliver.Register(runtime.RunID(tag.Tenant, tag.ID), tag.Routes()); err != nil {
		return runtime.Acceptance{}, fmt.Errorf("fiber: register routes: %w", err)
	}
	return a.rt.Accept(ctx, tag)
}

// Stop asks a run to stop. See runtime.Runtime.Stop for what a nil error does
// and does not mean; errors are the runtime's (runtime.ErrInvalid,
// runtime.ErrStopUnavailable).
func (a *App) Stop(ctx context.Context, req runtime.StopRequest) error {
	return a.rt.Stop(ctx, req)
}

// Compact asks a run's running turn to compact its history. It needs
// Config.Compaction.OnRequest; see runtime.Runtime.Compact.
func (a *App) Compact(ctx context.Context, req runtime.CompactRequest) error {
	return a.rt.Compact(ctx, req)
}

// Answer answers a question a run is parked on, and returns the answer the
// question has, which is this one only when first is true. See
// runtime.Runtime.Answer for the errors.
func (a *App) Answer(ctx context.Context, req runtime.AnswerRequest) (answer payload.Answer, first bool, err error) {
	return a.rt.Answer(ctx, req)
}

// Run starts delivery and execution and blocks until ctx is done or one of
// them fails, then stops both. A cancelled ctx is not a failure.
func (a *App) Run(ctx context.Context) error {
	return a.run(ctx, true, true)
}

// RunDelivery runs only the router: it reads the bus and renders events onto
// connector sinks, accepting and executing nothing, so delivery can scale
// apart from execution. It needs a shared bus to be useful.
func (a *App) RunDelivery(ctx context.Context) error {
	return a.run(ctx, true, false)
}

// RunExecution runs only the durable worker and the triggers: it executes
// runs and accepts what connectors raise, and delivers nothing.
func (a *App) RunExecution(ctx context.Context) error {
	return a.run(ctx, false, true)
}

// runGrace bounds how long Run waits for its loops after ctx ends. A
// connector's flush can outlive the context that told it to stop.
const runGrace = 20 * time.Second

func (a *App) run(ctx context.Context, delivery, execution bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := &group{cancel: cancel}
	if delivery {
		g.run("router", func() error { return a.deliver.Run(ctx) })
	}
	if execution {
		a.startExecution(ctx, g)
	}
	<-ctx.Done()
	return g.wait(runGrace)
}

// startExecution runs the durable worker and every trigger. The triggers feed
// one channel this process drains into Invoke, so a connector never learns
// what a runtime is.
func (a *App) startExecution(ctx context.Context, g *group) {
	wopts := []worker.Option{
		worker.WithRegistry(a.rt.Registry()),
		worker.WithConcurrency(max(a.opt.Concurrency, 1)),
		worker.WithLogger(a.log),
	}
	wopts = append(wopts, a.opt.WorkerOpts...)
	g.run("worker", func() error { return worker.New(a.ledger, a.queue, wopts...).Run(ctx) })

	triggers := a.sinks.Triggers()
	if len(triggers) == 0 {
		return
	}
	tags := make(chan envelope.Tag)
	for _, t := range triggers {
		g.run("trigger "+t.Name(), func() error {
			if err := t.Ingest(ctx, tags); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("fiber: trigger %s: %w", t.Name(), err)
			}
			return nil
		})
	}
	g.run("tags", func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case tag := <-tags:
				// The deployment's scope: a trigger has no credential.
				if tag.Tenant == "" {
					tag.Tenant = a.opt.Tenant
				}
				acc, err := a.Invoke(ctx, tag)
				if err != nil {
					a.log.Error("fiber: accept tag", "tag", tag.ID, "agent", tag.Agent, "error", err)
					continue
				}
				a.log.Info("fiber: accepted", "run", acc.RunID, "agent", tag.Agent, "origin", tag.Origin, "rev", acc.Rev)
			}
		}
	})
}

// Webhooks returns the connectors that receive their own webhooks, by
// connector name. Each verifies its own signature, so no unverified body
// reaches anything else. Mount them wherever the embedding program serves
// HTTP; the path a third party POSTs to is that connector's contract.
func (a *App) Webhooks() map[string]http.Handler { return maps.Clone(a.webhooks) }

// WebhookHandler serves every webhook connector under /connectors/{name},
// for a program that wants one mount point.
func (a *App) WebhookHandler() http.Handler {
	mux := http.NewServeMux()
	for name, h := range a.webhooks {
		mux.Handle("/connectors/"+name+"/", http.StripPrefix("/connectors/"+name, h))
		mux.Handle("/connectors/"+name, h)
	}
	return mux
}

// Accessors. They return the live components, for a program that adds its
// own transport, tooling or tests on top.

// Registry is the agent store.
func (a *App) Registry() registry.Store { return a.store }

// Bus is the broker runs publish to.
func (a *App) Bus() bus.Bus { return a.broker }

// Runtime is the durable runtime.
func (a *App) Runtime() *runtime.Runtime { return a.rt }

// Router is the delivery router.
func (a *App) Router() *router.Router { return a.deliver }

// Connectors is the connector registry.
func (a *App) Connectors() *connector.Registry { return a.sinks }

// Knowledge is the indexed corpus agents are grounded in.
func (a *App) Knowledge() knowledge.Store { return a.docs }

// Threads is the conversation journal.
func (a *App) Threads() thread.Store { return a.threads }

// ToolProviders is the MCP tool provider store.
func (a *App) ToolProviders() toolprovider.Store { return a.tools }

// Blobs is the file store.
func (a *App) Blobs() blob.Store { return a.blobs }

// Providers is the model provider names specs may select: the built-ins and
// every extension's.
func (a *App) Providers() []string { return a.opt.providerNames() }

// group runs long-lived loops and collects the first failure, cancelling the
// rest when one fails. A cancelled context is not a failure.
type group struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	err    error
}

func (g *group) run(name string, fn func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := fn()
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		g.mu.Lock()
		if g.err == nil {
			g.err = fmt.Errorf("%s: %w", name, err)
		}
		g.mu.Unlock()
		g.cancel()
	}()
}

func (g *group) wait(grace time.Duration) error {
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grace):
		return errors.New("fiber: timed out waiting for background loops to stop")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.err
}
