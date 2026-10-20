package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/urmzd/anchor"
	"github.com/urmzd/anchor/pkg/ledger"
	dqueue "github.com/urmzd/anchor/pkg/queue"
	drun "github.com/urmzd/anchor/pkg/run"
	"github.com/urmzd/anchor/pkg/worker"
	"github.com/urmzd/playscape/pkg/controlplane"
	"github.com/urmzd/playscape/pkg/metrics"
	"github.com/urmzd/playscape/pkg/node/inproc"
	"github.com/urmzd/playscape/pkg/tool"
	"github.com/urmzd/playscape/pkg/workspace"

	"github.com/urmzd/fiber/internal/server"
	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/attachment/resolve"
	"github.com/urmzd/fiber/pkg/blob"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/connectors/author"
	"github.com/urmzd/fiber/pkg/connectors/cron"
	"github.com/urmzd/fiber/pkg/durable"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/knowledge"
	"github.com/urmzd/fiber/pkg/registry"
	"github.com/urmzd/fiber/pkg/router"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/thread"
	"github.com/urmzd/fiber/pkg/topic"
)

// Environment variables the composition root reads. Everything that names an
// external system is an environment variable rather than a flag, because it is
// deployment configuration rather than a per-invocation choice, and because a
// credential in a flag ends up in shell history and in ps output.
const (
	// EnvRedis points the bus at Redis Streams. Unset keeps the in-memory
	// broker, which is a complete implementation of the same interface and
	// exactly as correct — for one process.
	EnvRedis = "FIBER_REDIS_URL"

	// EnvPostgres points the agent registry at Postgres. Unset keeps the
	// in-memory registry, which enforces every rule the Postgres one does and
	// forgets all of it on exit.
	EnvPostgres = "FIBER_POSTGRES_URL"

	// EnvAuthorProvider and EnvAuthorModel choose what the built-in author
	// agent runs on. Unset keeps the offline provider, which seeds the agent
	// and its tools without calling a model.
	EnvAuthorProvider = "FIBER_AUTHOR_PROVIDER"
	EnvAuthorModel    = "FIBER_AUTHOR_MODEL"

	// EnvCompactTokens is the input size, in tokens, above which a run's
	// history is compacted. Unset or zero never compacts.
	EnvCompactTokens = "FIBER_COMPACT_TOKENS"
	// EnvCompactKeepTurns is how many recent tool-calling turns a
	// compaction keeps verbatim. Unset or zero takes
	// agentrt.DefaultKeepTurns.
	EnvCompactKeepTurns = "FIBER_COMPACT_KEEP_TURNS"
	// EnvCompactOnRequest, when true, lets a host ask a running turn to
	// compact (POST /v1/runs/{id}/compact). Unset or false ignores
	// requests.
	EnvCompactOnRequest = "FIBER_COMPACT_ON_REQUEST"

	// EnvWorkspace is the directory agent artifacts are written under. Unset
	// takes a directory under the user's cache.
	EnvWorkspace = "FIBER_WORKSPACE"
)

// coreConfig is what a running fiber needs that is not a command-line
// concern. It is filled from flags and the environment by the commands, and
// nothing in this file reads either: a composition root that reaches for
// os.Getenv cannot be constructed twice in one process, and a test is exactly
// the second time.
type coreConfig struct {
	// Tenant is the authorization scope this process serves. It is stamped
	// onto every tag a connector raises, mirroring what the transport does
	// with a credential: a trigger has no credential to derive a scope from,
	// so the deployment's own scope is the only honest answer.
	Tenant string

	// Redis is a redis:// URL. Empty selects the in-memory bus.
	Redis string

	// Postgres is a postgres:// URL. Empty selects the in-memory registry.
	Postgres string

	// Embeddings selects the model the knowledge layer embeds with. The zero
	// value is the offline hashing embedder.
	Embeddings embeddingsConfig

	// AuthorProvider and AuthorModel are what the built-in author agent runs
	// on. Empty takes the offline provider.
	AuthorProvider, AuthorModel string

	// CompactTokens is the history size that triggers compaction. Zero never
	// compacts on its own.
	CompactTokens int
	// CompactKeepTurns is how many recent turns a compaction keeps. Zero
	// takes the default.
	CompactKeepTurns int
	// CompactOnRequest lets a host ask a running turn to compact.
	CompactOnRequest bool

	// Workspace is where the sandbox writes agent artifacts.
	Workspace string

	// Schedules are the cron connector's standing requests. Empty registers no
	// cron connector at all, rather than one with nothing to do.
	Schedules []cron.Schedule

	// Concurrency is how many runs a worker executes at once. Zero means one.
	Concurrency int

	// Log receives operational output. Nil discards it.
	Log *slog.Logger

	// Metrics receives counters. Nil records nothing.
	Metrics metrics.Recorder
}

// core is fiber assembled: every long-lived component this process owns, in
// the order data flows through them.
//
//	registry  →  runtime  →  bus  →  router  →  connector sinks
//	                ↑                              ↓
//	            anchor worker              a surface somewhere
//
// Nothing here is a stub standing in for a deployment. The in-memory registry,
// bus, ledger and queue are complete implementations of the same interfaces the
// distributed backends implement; swapping one is a line in newCore, not a
// change to anything below it.
type core struct {
	cfg coreConfig
	log *slog.Logger

	store   registry.Store
	docs    knowledge.Store
	blobs   blob.Store
	broker  bus.Bus
	sinks   *connector.Registry
	deliver *router.Router
	rt      *runtime.Runtime
	ledger  runLedger
	queue   dqueue.Queue

	// webhooks are the connectors that serve their own HTTP, mounted under
	// /connectors/{name}. A trigger authenticates its own requests, so the
	// server never sees an unverified body.
	webhooks map[string]http.Handler

	closers []func() error
}

// runLedger is the ledger plus the point read Run polls with.
type runLedger interface {
	ledger.Ledger
	ledger.RunGetter
}

// newCore assembles the system. It is deliberately linear: read it top to
// bottom and you have read how fiber fits together.
func newCore(cfg coreConfig) (*core, error) {
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	rec := cfg.Metrics
	if rec == nil {
		rec = metrics.Nop()
	}
	c := &core{cfg: cfg, log: log, webhooks: map[string]http.Handler{}}

	// The broker. Runs publish here; the router, every subscriber, and every
	// SSE client read from here. Nothing else connects them.
	broker, closeBus, err := newBus(cfg.Redis, cfg.Concurrency, controlStreams(cfg))
	if err != nil {
		return nil, err
	}
	c.broker = broker
	if closeBus != nil {
		c.closers = append(c.closers, closeBus)
	}

	// The control plane: append-only agent definitions, scoped by tenant.
	// Postgres, when configured, holds everything that has to outlive the
	// process: agent definitions and the run ledger and queue. One pool serves
	// all of it.
	pool, err := newPostgres(cfg.Postgres)
	if err != nil {
		_ = c.close()
		return nil, err
	}
	if pool != nil {
		c.closers = append(c.closers, func() error { pool.Close(); return nil })
	}
	ctx, cancel := context.WithTimeout(context.Background(), postgresStartTimeout)
	defer cancel()

	// The control plane: append-only agent definitions, scoped by tenant.
	c.store, err = newRegistry(ctx, pool)
	if err != nil {
		_ = c.close()
		return nil, err
	}

	// What agents know: the corpus a revision's sources are retrieved from.
	c.docs, err = newKnowledge(ctx, pool, cfg.Embeddings)
	if err != nil {
		_ = c.close()
		return nil, err
	}

	// Files. Content-addressed and tenant-scoped, under the workspace so one
	// volume holds everything this process writes to disk.
	files, err := blob.NewFS(filepath.Join(workspaceDir(cfg), "blobs"))
	if err != nil {
		_ = c.close()
		return nil, fmt.Errorf("cli: blob store: %w", err)
	}
	c.blobs = files
	c.closers = append(c.closers, files.Close)

	// What a thread remembers: the conversation a run continues.
	threads, err := newThreads(ctx, pool)
	if err != nil {
		_ = c.close()
		return nil, err
	}

	// The mesh. Roles are derived from the interfaces each connector
	// implements, never declared, so registering is the whole wiring.
	sinks, err := buildConnectors(cfg, c.store, c.blobs)
	if err != nil {
		return nil, err
	}
	c.sinks = sinks

	// The author: the control plane offered to an agent as tools, and the
	// agent that holds them. Its actions act as whoever raised the run, which
	// is read from the ledger and never from what the model passed.
	if err := c.addAuthor(ctx); err != nil {
		_ = c.close()
		return nil, err
	}
	for _, t := range sinks.Triggers() {
		if h, ok := t.(http.Handler); ok {
			c.webhooks[t.Name()] = h
		}
	}

	// The router is just another bus subscriber. That indirection is why
	// origin and destination stay independent: nothing on the path from
	// trigger to sink holds both ends at once.
	c.deliver = router.New(broker, sinks, router.WithLogger(log))

	// The agent side. A turn runs as a playscape task under the NGAC policy
	// compiled from the pinned revision's Access grant, so what an agent may
	// touch is enforced rather than documented.
	exec, err := newExecutor(cfg, c.store, c.docs, c.blobs, sinks, log, rec)
	if err != nil {
		return nil, err
	}

	// Durable execution: the ledger is truth, the queue is disposable flow.
	c.ledger, c.queue, err = newDurable(ctx, pool)
	if err != nil {
		_ = c.close()
		return nil, err
	}

	rtOpts := []runtime.Option{
		runtime.WithLogger(log),
		runtime.WithThreads(threads),
		// Before a run's history is pinned, its thread catches up with
		// what people said on the surface without tagging the agent.
		// Failing to list a surface is logged and the run goes ahead.
		runtime.WithReconciler(listers(sinks)),
		runtime.WithMessageID(surfaceMessageID),
		// A stop asked for before a run's turn starts is recorded on the
		// run, so it does not depend on the bus still holding it.
		runtime.WithStopLedger(c.ledger),
		// An answered or stopped question wakes its run at once rather
		// than at the worker's next janitor sweep.
		runtime.WithWaker(c.queue),
	}
	if pool != nil {
		// Polls that want one record of a run (a stop, an answer) read that
		// record, not the run's transcripts.
		rtOpts = append(rtOpts, runtime.WithRecordReader(durable.NewRecords(pool)))
	}
	if cfg.CompactOnRequest {
		// The agents take requests to compact (see the sandbox below), so
		// their turns listen for them. Each listening turn holds a second
		// control subscription, which newBus sized the pool for.
		rtOpts = append(rtOpts, runtime.WithCompactRequests())
	}
	rt, err := runtime.New(anchor.New(c.ledger, c.queue), registry.NewSpecs(c.store), broker, exec, rtOpts...)
	if err != nil {
		return nil, fmt.Errorf("cli: runtime: %w", err)
	}
	c.rt = rt
	return c, nil
}

// close releases what the core owns. It is separate from the run loops because
// a Redis client outlives the context that stopped reading from it.
func (c *core) close() error {
	var err error
	for _, fn := range c.closers {
		err = errors.Join(err, fn())
	}
	return err
}

// newBus selects a broker. The in-memory one is the default because a single
// process needs nothing else and a default that requires infrastructure is a
// default nobody can run.
//
// On Redis every live subscription holds a pooled connection for most of its
// poll cycle, and a turn holds one for as long as it runs (it listens for a
// stop). A pool smaller than the number of running turns makes their
// publishes wait for a connection, and a publish that times out fails the
// turn. So unless the URL sets pool_size, the pool is go-redis's default plus
// one connection per run this process executes at once.
func newBus(redisURL string, concurrency, perRun int) (bus.Bus, func() error, error) {
	if strings.TrimSpace(redisURL) == "" {
		return bus.NewMemory(), nil, nil
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, nil, usagef("$%s is not a redis URL: %v", EnvRedis, err)
	}
	opts.PoolSize = redisPoolSize(redisURL, opts.PoolSize, concurrency, perRun)
	client := redis.NewClient(opts)
	return bus.NewRedis(client), client.Close, nil
}

// addAuthor registers the author connector and seeds the author agent for this
// deployment's tenant. Seeding is safe on every start: an unchanged definition
// is a no-op, and one an operator has edited or deleted is left alone.
func (c *core) addAuthor(ctx context.Context) error {
	a, err := author.New(author.Config{
		Store:      c.store,
		Principals: author.PrincipalFunc(c.principal),
		Catalog: func() []string {
			var names []string
			for _, act := range c.sinks.Actions() {
				names = append(names, act.Name)
			}
			return append(names, agentrt.ConnectorGroups(c.sinks)...)
		},
	})
	if err != nil {
		return fmt.Errorf("cli: author: %w", err)
	}
	if err := c.sinks.Register(a); err != nil {
		return fmt.Errorf("cli: register %s: %w", a.Name(), err)
	}
	if c.cfg.Tenant == "" {
		return nil
	}
	provider, model := c.cfg.AuthorProvider, c.cfg.AuthorModel
	if provider == "" {
		provider = agentspec.ProviderOffline
	}
	if model == "" {
		model = "claude-sonnet-5-5"
	}
	if _, err := author.Seed(ctx, c.store, c.cfg.Tenant, author.Spec(model, provider)); err != nil {
		return fmt.Errorf("cli: seed author: %w", err)
	}
	return nil
}

// principal resolves a run to the tenant and actor on its accepted tag. It is
// the only way an action learns who it acts as, and it reads the ledger: the
// tag there was stamped by the transport from a credential, which is the one
// account of identity a model cannot rewrite.
func (c *core) principal(ctx context.Context, runID string) (author.Principal, error) {
	if c.ledger == nil {
		return author.Principal{}, errors.New("cli: run ledger not ready")
	}
	rec, err := c.ledger.GetRun(ctx, runID)
	if err != nil {
		return author.Principal{}, fmt.Errorf("cli: read run %q: %w", runID, err)
	}
	var in runtime.Input
	if err := json.Unmarshal(rec.Input, &in); err != nil {
		return author.Principal{}, fmt.Errorf("cli: decode run %q input: %w", runID, err)
	}
	p := author.Principal{Tenant: in.Tag.Tenant}
	if in.Tag.Actor.ID != "" {
		p.Actor = in.Tag.Origin + ":" + in.Tag.Actor.ID
	}
	return p, nil
}

// postgresStartTimeout bounds how long startup waits for Postgres. A database
// that cannot be reached is a deployment that cannot resolve an agent or accept
// a run, so failing here, with the reason, beats serving requests that all fail
// later.
const postgresStartTimeout = 30 * time.Second

// newPostgres opens the pool the durable backends share. An empty URL returns
// nil, which every constructor below reads as "stay in memory".
func newPostgres(url string) (*pgxpool.Pool, error) {
	if strings.TrimSpace(url) == "" {
		return nil, nil
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		return nil, usagef("$%s is not a postgres URL: %v", EnvPostgres, err)
	}
	return pool, nil
}

// newRegistry selects the control plane's store. The schema is migrated here
// rather than by a separate command because a server that starts against an
// unmigrated database has nothing useful to do but report that.
func newRegistry(ctx context.Context, pool *pgxpool.Pool) (registry.Store, error) {
	if pool == nil {
		return registry.NewMemory(), nil
	}
	store := registry.NewPostgres(pool)
	if err := store.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("%w: $%s: %w", errUnavailable, EnvPostgres, err)
	}
	return store, nil
}

// newThreads selects the conversation journal: Postgres when there is a pool,
// so a thread survives a restart alongside the runs that continue it.
func newThreads(ctx context.Context, pool *pgxpool.Pool) (thread.Store, error) {
	if pool == nil {
		return thread.NewMemory(), nil
	}
	store := thread.NewPostgres(pool)
	if err := store.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("%w: $%s: %w", errUnavailable, EnvPostgres, err)
	}
	return store, nil
}

// surfaceMessageID names the surface message that raised a tag, from the
// metadata the triggers already set. It is what lets a later reconcile
// recognise that message instead of journaling it a second time.
func surfaceMessageID(tag envelope.Tag) string {
	switch tag.Origin {
	case "slack":
		return tag.Meta["slack_ts"]
	case "github":
		return tag.Meta["github_comment_id"]
	}
	return ""
}

// newDurable selects the run ledger and queue. On Postgres a run accepted
// before a restart is still there after it, and its lease expires and is
// reclaimed by whichever worker comes up next.
func newDurable(ctx context.Context, pool *pgxpool.Pool) (runLedger, dqueue.Queue, error) {
	if pool == nil {
		return ledger.NewMemory(), dqueue.NewMemory(), nil
	}
	lgr, q, err := durable.Open(ctx, pool)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: $%s: %w", errUnavailable, EnvPostgres, err)
	}
	return lgr, q, nil
}

// workspaceDir is the directory everything this process writes to disk lives
// under: agent artifacts and uploaded files, each in its own subdirectory.
func workspaceDir(cfg coreConfig) string {
	if dir := strings.TrimSpace(cfg.Workspace); dir != "" {
		return dir
	}
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "fiber")
}

// newExecutor builds the sandbox a turn runs in.
//
// The tool registry and the workspace are shared with the control plane rather
// than owned by the sandbox: playscape resolves a tool by name on the node that
// executes it, and the node is created from this factory. The policy that
// scopes what that tool can see comes from the pinned revision, one deployment
// per (agent, revision), which is what stops a revised Access grant from
// widening a run already in flight.
func newExecutor(cfg coreConfig, store registry.Store, docs knowledge.Store, files blob.Store, sinks *connector.Registry, log *slog.Logger, rec metrics.Recorder) (runtime.Executor, error) {
	dir := filepath.Join(workspaceDir(cfg), "workspace")
	ws, err := workspace.NewLocal(dir)
	if err != nil {
		return nil, fmt.Errorf("cli: workspace %q: %w", dir, err)
	}
	tools := tool.NewRegistry()
	plane := controlplane.NewMemory(inproc.NewFactory(tools, ws), rec)

	agentOpts := []agentrt.Option{agentrt.WithAttachments(resolve.New(files))}
	if cfg.CompactTokens > 0 || cfg.CompactOnRequest {
		agentOpts = append(agentOpts, agentrt.WithCompaction(agentrt.Compaction{
			MaxInputTokens: cfg.CompactTokens,
			KeepTurns:      cfg.CompactKeepTurns,
			OnRequest:      cfg.CompactOnRequest,
		}))
	}

	exec, err := runtime.NewSandbox(plane, tools, registry.NewSpecs(store),
		runtime.WithAgentOptions(agentOpts...),
		// A spec's tool names resolve against the actions every registered
		// connector advertises, so granting "github.comment" in a spec is the
		// whole of wiring an agent to GitHub's write side.
		runtime.WithCatalog(runtime.Connectors(sinks)),
		runtime.WithKnowledge(retrieverFor(docs)),
		runtime.WithSandboxLogger(log),
	)
	if err != nil {
		return nil, fmt.Errorf("cli: sandbox: %w", err)
	}
	return exec, nil
}

// Invoke accepts a tag. It satisfies server.Invoker, and it is the one place
// where accepting work and knowing where to deliver it meet.
//
// The order matters: routes are registered with the router BEFORE the run is
// accepted, because accept publishes the run's first event and an event that
// arrives for a run the router has no routes for has nowhere to go. RunID is a
// pure function of the tag id, so the routes can be registered before the run
// exists.
func (c *core) Invoke(ctx context.Context, tag envelope.Tag) (server.Accepted, error) {
	if err := c.deliver.Register(runtime.RunID(tag.Tenant, tag.ID), tag.Routes()); err != nil {
		return server.Accepted{}, fmt.Errorf("cli: register routes: %w", err)
	}
	acc, err := c.rt.Accept(ctx, tag)
	if err != nil {
		return server.Accepted{}, err
	}
	return server.Accepted{
		RunID: acc.RunID,
		Rev:   acc.Rev,
		Topic: acc.Topic,
		At:    acc.AcceptedAt,
	}, nil
}

// Run reads a durable run record. It satisfies server.RunReader.
//
// It is a projection of the ledger, so reading one never touches a worker and
// polling it is cheap. A run belonging to another tenant is reported as not
// found rather than as a permission error: saying "exists, but not yours"
// confirms existence across the boundary.
func (c *core) Run(ctx context.Context, tenant, runID string) (server.Run, error) {
	rec, err := c.ledger.GetRun(ctx, runID)
	if err != nil {
		if errors.Is(err, drun.ErrNotFound) {
			return server.Run{}, fmt.Errorf("%w: run %q", server.ErrNotFound, runID)
		}
		return server.Run{}, fmt.Errorf("cli: read run %q: %w", runID, err)
	}

	var in runtime.Input
	if err := json.Unmarshal(rec.Input, &in); err != nil {
		return server.Run{}, fmt.Errorf("cli: decode run %q input: %w", runID, err)
	}
	if in.Tag.Tenant != tenant {
		return server.Run{}, fmt.Errorf("%w: run %q", server.ErrNotFound, runID)
	}

	out := server.Run{
		ID:        rec.ID,
		Tenant:    in.Tag.Tenant,
		Agent:     in.Tag.Agent,
		Rev:       in.Rev,
		Origin:    in.Tag.Origin,
		Status:    statusOf(rec.Status),
		Tag:       in.Tag,
		Error:     rec.Error,
		CreatedAt: rec.CreatedAt,
		EndedAt:   rec.CompletedAt,
	}
	if t, err := topic.Run(in.Tag.Agent, rec.ID); err == nil {
		out.Topic = t
	}
	// LastSeq is the run's own answer, not the ledger's: it is the sequence of
	// the completed event, which only a finished run has.
	if rec.Status == drun.StatusSucceeded && len(rec.Output) > 0 {
		var result runtime.Output
		if err := json.Unmarshal(rec.Output, &result); err == nil {
			out.LastSeq = result.LastSeq
			// The ledger knows two endings. A stopped run is the third, and
			// it is told apart by what the run returned.
			if result.Stopped != nil {
				out.Status = server.StatusStopped
				out.StopReason = result.Stopped.Reason
				out.StoppedBy = result.Stopped.By
			}
			out.Usage = result.Usage
		}
	}
	// A pending run waiting on a person says so, and says what it asked.
	if rec.Status == drun.StatusPending {
		if q, ok, err := c.rt.PendingQuestion(ctx, tenant, runID); err == nil && ok {
			out.Status = server.StatusParked
			out.Question = &q
		}
	}
	// A failed run has no output. What its attempts spent is on its ledger
	// records, which is the same figure an output would have carried.
	if rec.Status == drun.StatusFailed {
		if _, records, err := c.ledger.Load(ctx, runID); err == nil {
			if u, ok := runtime.RecordedUsage(records); ok {
				out.Usage = &u
			}
		}
	}
	return out, nil
}

// Stop asks a run to stop. It satisfies server.Stopper.
//
// It is one line because the request is not this process's to carry out: the
// runtime publishes it on the bus, and whichever worker is executing the run,
// here or elsewhere, hears it. See runtime.Runtime.Stop for what the absence of
// an error does and does not mean.
func (c *core) Stop(ctx context.Context, req server.StopRequest) error {
	err := c.rt.Stop(ctx, runtime.StopRequest{Tenant: req.Tenant, RunID: req.RunID, Reason: req.Reason, By: req.By})
	switch {
	case errors.Is(err, runtime.ErrInvalid):
		return fmt.Errorf("%w: %w", server.ErrInvalid, err)
	case errors.Is(err, runtime.ErrStopUnavailable):
		return fmt.Errorf("%w: %w", server.ErrUnimplemented, err)
	}
	return err
}

// Compact asks a run's running turn to compact its history. It satisfies
// server.Compactor. See runtime.Runtime.Compact for what a nil error means.
func (c *core) Compact(ctx context.Context, req server.CompactRequest) error {
	err := c.rt.Compact(ctx, runtime.CompactRequest{Tenant: req.Tenant, RunID: req.RunID, By: req.By})
	switch {
	case errors.Is(err, runtime.ErrInvalid):
		return fmt.Errorf("%w: %w", server.ErrInvalid, err)
	case errors.Is(err, runtime.ErrCompactUnavailable):
		return fmt.Errorf("%w: %w", server.ErrUnimplemented, err)
	}
	return err
}

// Answer answers a question a run is parked on. It satisfies server.Answerer,
// translating the runtime's errors into the edge's.
func (c *core) Answer(ctx context.Context, req server.AnswerRequest) (payload.Answer, bool, error) {
	got, first, err := c.rt.Answer(ctx, runtime.AnswerRequest{
		Tenant: req.Tenant, RunID: req.RunID, QuestionID: req.QuestionID, Answer: req.Answer, By: req.By, Via: req.Via,
	})
	switch {
	case errors.Is(err, runtime.ErrNoQuestion):
		return payload.Answer{}, false, fmt.Errorf("%w: %w", server.ErrNotFound, err)
	case errors.Is(err, runtime.ErrNotAllowed):
		return payload.Answer{}, false, fmt.Errorf("%w: %w", server.ErrForbidden, err)
	case errors.Is(err, runtime.ErrQuestionClosed):
		return payload.Answer{}, false, fmt.Errorf("%w: %w", server.ErrClosed, err)
	case errors.Is(err, runtime.ErrInvalid):
		return payload.Answer{}, false, fmt.Errorf("%w: %w", server.ErrInvalid, err)
	case errors.Is(err, runtime.ErrAnswerUnavailable):
		return payload.Answer{}, false, fmt.Errorf("%w: %w", server.ErrUnimplemented, err)
	}
	return got, first, err
}

// statusOf maps a ledger status onto the edge's. The ledger has no word for
// stopped: it records such a run as succeeded, and Run tells the two apart by
// the run's recorded output, which is as durable as the status itself. anchor has no "running"
// state — assignment is flow state and lives in the queue, never in the ledger
// — so a pending run is reported as accepted, which is the only thing the
// ledger actually knows about it.
func statusOf(s drun.RunStatus) server.Status {
	switch s {
	case drun.StatusSucceeded:
		return server.StatusCompleted
	case drun.StatusFailed:
		return server.StatusFailed
	default:
		return server.StatusAccepted
	}
}

// startDelivery runs the router: the half of fiber that reads the bus and
// renders events onto surfaces. It is separable from execution on purpose —
// see work.go.
func (c *core) startDelivery(ctx context.Context, g *group) {
	g.run("router", func() error { return c.deliver.Run(ctx) })
}

// startExecution runs the durable worker and every trigger.
//
// The worker is a pull loop on a goroutine, not a service to deploy: it leases
// runs from the queue this process owns and executes them through the activity
// registry the runtime built. The triggers are the inbound half, and they feed
// one channel that this process drains into Invoke, so a connector never learns
// what a runtime is.
func (c *core) startExecution(ctx context.Context, g *group) {
	g.run("worker", func() error {
		return worker.New(c.ledger, c.queue,
			worker.WithRegistry(c.rt.Registry()),
			worker.WithConcurrency(max(c.cfg.Concurrency, 1)),
			worker.WithLogger(c.log),
		).Run(ctx)
	})

	triggers := c.sinks.Triggers()
	if len(triggers) == 0 {
		return
	}
	tags := make(chan envelope.Tag)
	for _, t := range triggers {
		g.run("trigger "+t.Name(), func() error {
			if err := t.Ingest(ctx, tags); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("cli: trigger %s: %w", t.Name(), err)
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
				// The deployment's scope, stamped exactly where the transport
				// stamps a credential's. A trigger has no credential.
				if tag.Tenant == "" {
					tag.Tenant = c.cfg.Tenant
				}
				acc, err := c.Invoke(ctx, tag)
				if err != nil {
					c.log.Error("cli: accept tag", "tag", tag.ID, "agent", tag.Agent, "error", err)
					continue
				}
				c.log.Info("cli: accepted", "run", acc.RunID, "agent", tag.Agent,
					"origin", tag.Origin, "rev", acc.Rev)
			}
		}
	})
}

// loadSchedules reads the cron connector's standing requests from a JSON file:
// an array of schedules, validated here so a typo fails at startup rather than
// at 2am.
func loadSchedules(path string) ([]cron.Schedule, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cli: read schedules: %w", err)
	}
	var out []cron.Schedule
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("cli: parse schedules %q: %w", path, err)
	}
	for i, s := range out {
		if err := s.Validate(); err != nil {
			return nil, fmt.Errorf("cli: schedule %d in %q: %w", i, path, err)
		}
	}
	return out, nil
}

// group runs the long-lived loops of a command and collects the first failure.
//
// It is fifteen lines rather than a dependency because that is all this needs:
// every loop here ends on one shared context, and a caller wants "the reason we
// stopped", not a cancellation tree. A cancelled context is not a failure and
// is never reported as one.
type group struct {
	wg  sync.WaitGroup
	mu  sync.Mutex
	err error
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
		defer g.mu.Unlock()
		if g.err == nil {
			g.err = fmt.Errorf("%s: %w", name, err)
		}
	}()
}

// wait blocks until every loop has returned, or until grace expires. The
// timeout exists because a connector's flush can outlive the context that told
// it to stop, and a CLI that never exits is worse than one that reports a
// straggler.
func (g *group) wait(grace time.Duration) error {
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grace):
		return errors.New("cli: timed out waiting for background loops to stop")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.err
}

// redisPoolSize is the connection pool newBus asks for: what the URL says when
// it says, otherwise go-redis's default of ten per CPU plus perRun for each
// run this process may execute at once.
func redisPoolSize(redisURL string, fromURL, concurrency, perRun int) int {
	if strings.Contains(redisURL, "pool_size=") && fromURL > 0 {
		return fromURL
	}
	return 10*goruntime.GOMAXPROCS(0) + max(concurrency, 1)*max(perRun, 1)
}

// controlStreams is how many control subscriptions a running turn holds for
// the whole turn: one for stop requests, and one for requests to compact when
// the deployment takes them.
func controlStreams(cfg coreConfig) int {
	if cfg.CompactOnRequest {
		return 2
	}
	return 1
}
