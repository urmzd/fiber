// Package fiber is an agent library you compose in Go. New assembles the
// agent definitions, the bus, dispatch, egress, the durable runtime, the sandbox,
// knowledge, thread journals, files, MCP tool providers, the author agent and
// the connectors into an App, and Run drives it in process. It is a library:
// there is no service, no configuration file and no environment variable,
// every choice is a value passed to an Option, and anything not passed is the
// in-memory implementation, which is complete rather than a stub. The one
// command in the module, cmd/fiber, is a program built on it like any other,
// for running a spec locally (fiber run).
//
//	pool, _ := pgxpool.New(ctx, dsn) // the program's own pool
//	pg := bus.NewPostgres(pool)
//	_ = pg.Migrate(ctx)
//	app, err := fiber.New(ctx,
//		fiber.WithTenant("acme"),
//		fiber.WithPostgres(pool), // every store family on this pool
//		fiber.WithBus(pg),        // and the bus
//		fiber.WithModelProvider("house", newHouseProvider),
//	)
//	if err != nil { ... }
//	defer app.Close()
//	go app.Run(ctx)
//	_ = app.Memberships().Join(ctx, "acme", "slack://T01/C02", "docs-bot",
//		membership.Rule{Types: []string{"message"}})
//	err = app.Publish(ctx, msg) // a message.posted.v1 naming docs-bot
//
// Code-shaped extensions are options too: a model provider, a tool source, a
// connector, an MCP tool provider. Configuration from files or the
// environment belongs to the program embedding fiber (Humagen is one), which
// turns it into these options.
//
// # Where it sits
//
// The package is the module root because it composes everything under pkg/,
// and nothing under pkg/ may depend on it. An App listens on nothing:
// connectors that receive webhooks are offered as http.Handlers for the
// embedding program to mount.
package fiber

import (
	"log/slog"
	"maps"

	"github.com/jackc/pgx/v5/pgxpool"
	dqueue "github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/worker"
	"github.com/urmzd/playscape/pkg/metrics"

	"github.com/urmzd/fiber/pkg/blob"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/connectors/cron"
	"github.com/urmzd/fiber/pkg/connectors/webhook"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/egress"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/knowledge"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/membership"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/thread"
	"github.com/urmzd/fiber/pkg/toolprovider"
	"github.com/urmzd/fiber/pkg/turn"
	"github.com/urmzd/fiber/pkg/turn/native"
	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

// Option is one choice New is given.
type Option func(*options)

func apply(opts []Option) options {
	var o options
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o
}

// Deployment.

// WithTenant sets the authorization scope this deployment serves. It is
// stamped onto every event a connector publishes (a trigger has no credential
// to derive one from), mentions on GitHub and Jira resolve agents in it, Run
// dispatches and delivers its events, the author agent is seeded for it, and
// tool providers declared without a tenant are recorded under it. There is no
// default: without it nothing is seeded, and WithSlack, WithGitHub and
// WithJira are ErrConfig, since they would publish nothing a run could be
// accepted for. A schedule carries its own tenant.
func WithTenant(tenant string) Option { return func(o *options) { o.Tenant = tenant } }

// WithWorkspace sets the directory the sandbox writes agent artifacts under.
// Without it New makes a temporary directory and Close removes it.
func WithWorkspace(dir string) Option { return func(o *options) { o.Workspace = dir } }

// WithLogger sends operational output to log. Without it it is discarded.
func WithLogger(log *slog.Logger) Option { return func(o *options) { o.Log = log } }

// WithMetrics records counters to rec. Without it nothing is recorded.
func WithMetrics(rec metrics.Recorder) Option { return func(o *options) { o.Metrics = rec } }

// WithConcurrency sets how many runs the worker executes at once (default
// one).
func WithConcurrency(n int) Option { return func(o *options) { o.Concurrency = n } }

// WithCompaction sets the history compaction every agent runs with. The zero
// value never compacts; OnRequest lets a host ask a running turn to compact
// (App.Compact).
func WithCompaction(c native.Compaction) Option { return func(o *options) { o.Compaction = c } }

// WithAuthor chooses what the built-in author agent runs on. Without it the
// agent is seeded on the offline provider.
func WithAuthor(provider, model string) Option {
	return func(o *options) { o.AuthorProvider, o.AuthorModel = provider, model }
}

// Backends. Each replaces the in-memory default; the App never closes what
// it is given.

// WithPostgres puts every store family on pool: the agent definitions, the run
// ledger and queue, knowledge, thread journals, memberships and MCP tool
// providers. New
// migrates each. A store passed by its own option wins over this one. The
// bus is chosen separately (WithBus with bus.NewPostgres or bus.NewRedis).
func WithPostgres(pool *pgxpool.Pool) Option { return func(o *options) { o.Postgres = pool } }

// WithBus sets the broker: bus.NewMemory (the default), bus.NewRedis or
// bus.NewPostgres, or any implementation that passes bustest.
func WithBus(b bus.Bus) Option { return func(o *options) { o.Bus = b } }

// WithDefinitions sets the agent store. The store decides which providers it
// accepts (definition.WithProviders or WithChecks); App.Providers lists the
// names this App can run.
func WithDefinitions(s definition.Source) Option { return func(o *options) { o.Definitions = s } }

// WithDurable sets the run ledger and queue runs execute from, for example
// the two durable.Open returns. Both are required.
func WithDurable(l Ledger, q dqueue.Queue) Option {
	return func(o *options) { o.Ledger, o.Queue = l, q }
}

// WithMemberships sets which agents are members of which channels, which is
// what decides who an event published with App.Publish wakes. Without it,
// and without WithPostgres, memberships live in memory and start empty.
func WithMemberships(s membership.Store) Option { return func(o *options) { o.Memberships = s } }

// WithThreads sets the conversation journal.
func WithThreads(s thread.Store) Option { return func(o *options) { o.Threads = s } }

// WithKnowledge sets the knowledge store agents are grounded in.
func WithKnowledge(s knowledge.Store) Option { return func(o *options) { o.Knowledge = s } }

// WithEmbedder sets the embedder of the knowledge store New builds (memory,
// or Postgres with WithPostgres). Without it the offline hashing embedder
// is used. A store passed by WithKnowledge brings its own.
func WithEmbedder(e knowledge.Embedder) Option { return func(o *options) { o.Embedder = e } }

// WithBlobs sets the file store, for example blob.NewFS(dir).
func WithBlobs(s blob.Store) Option { return func(o *options) { o.Blobs = s } }

// WithToolProviders sets the MCP tool provider store.
func WithToolProviders(s toolprovider.Store) Option { return func(o *options) { o.Tools = s } }

// WithTurnRuntime replaces the playscape sandbox a turn runs in. Model
// providers, tool sources, compaction and knowledge are wired into the
// sandbox, so a turn runtime passed here brings its own.
func WithTurnRuntime(rt turn.Runtime) Option { return func(o *options) { o.Runtime = rt } }

// WithRuntimeOptions adds options to the durable runtime, after the App's
// own, so they can override them.
func WithRuntimeOptions(opts ...runtime.Option) Option {
	return func(o *options) { o.RuntimeOpts = append(o.RuntimeOpts, opts...) }
}

// WithWorkerOptions adds options to the durable worker Run starts, after the
// App's own.
func WithWorkerOptions(opts ...worker.Option) Option {
	return func(o *options) { o.WorkerOpts = append(o.WorkerOpts, opts...) }
}

// WithEgressOptions adds options to the egress engine, after the App's own.
func WithEgressOptions(opts ...egress.Option) Option {
	return func(o *options) { o.EgressOpts = append(o.EgressOpts, opts...) }
}

// Extensions.

// WithModelProvider makes a model provider selectable by name: a spec whose
// provider is name runs on what fn builds. The registry New builds accepts
// specs naming it, and a built-in of the same name is replaced.
func WithModelProvider(name string, fn native.ProviderFunc) Option {
	return func(o *options) {
		if name == "" || fn == nil {
			return
		}
		o.Providers = maps.Clone(o.Providers)
		if o.Providers == nil {
			o.Providers = map[string]native.ProviderFunc{}
		}
		o.Providers[name] = fn
	}
}

// WithOpenAICompatible registers an OpenAI-compatible endpoint under ep.Name.
// It is WithModelProvider with ep.Provider(); the key is read from the
// environment variable ep.KeyEnv names when an agent is built.
func WithOpenAICompatible(ep native.OpenAICompatible) Option {
	return WithModelProvider(ep.Name, ep.Provider())
}

// WithToolSource adds a catalog that spec tool names resolve against, after
// the connectors' actions. It is built per turn, so it can bind tools to the
// run's tenant and surface; action reports a call as an action.taken event.
func WithToolSource(c runtime.Catalog) Option {
	return func(o *options) {
		if c != nil {
			o.Catalogs = append(o.Catalogs, c)
		}
	}
}

// WithTools adds a fixed set of tools, resolved by name for every turn. It is
// WithToolSource for tools that need nothing from the run.
func WithTools(tools ...saigetypes.Tool) Option {
	static := native.StaticTools(tools...)
	return WithToolSource(func(runtime.Request, func(event.Action) error) (native.Tools, error) {
		return static, nil
	})
}

// WithConnector registers a connector beside the built-in ones. Its roles
// (trigger, sink, actor) are derived from the interfaces it implements, as
// every connector's are.
func WithConnector(c connector.Connector) Option {
	return func(o *options) {
		if c != nil {
			o.Connectors = append(o.Connectors, c)
		}
	}
}

// WithToolProvider records an MCP tool provider at startup and accepts the
// tools in accept from it, each pinned by Name and SchemaHash and accepted
// with its Class (write when empty). Only an offered tool whose schema hash
// matches its pin is accepted, and its description may not change once it
// has been; anything the server adds or changes stays pending and is logged
// with its hash. A tool with no SchemaHash, or a provider declared twice, is
// ErrConfig. An empty p.Tenant is the deployment's tenant. Agents reach the
// provider with an "mcp:<name>" or "mcp:<name>:write" grant.
//
// A person-scoped provider is not dialled at startup, since it has no
// credential of its own: each tool in accept must then carry the Description
// and InputSchema the server offers, and is accepted in that form when its
// SchemaHash matches the schema. Its runs reach it with each person's own
// credential (WithPersonCredentials).
func WithToolProvider(p toolprovider.Provider, accept ...toolprovider.Tool) Option {
	return func(o *options) {
		o.ToolProviders = append(o.ToolProviders, toolProviderSeed{Provider: p, Accept: accept})
	}
}

// WithPersonCredentials sets how a person-scoped MCP tool provider
// (toolprovider.ScopePerson) finds the credential of the person a run acts
// for. That person is the actor on the run's trigger as it was accepted, which
// the program composing fiber sets from the caller's credential (a surface
// marked ActAs names the person it authenticated) and which no model output
// can change. A reference the resolver returns is resolved like a provider's
// own ("env:NAME"); a resolver that holds the secrets returns them. A person
// it has nothing for gets no tools from such a provider, the model is told
// they have not connected a credential, and the shared credential is never
// used instead. Without this option every person-scoped grant is unavailable.
func WithPersonCredentials(r mcptools.PersonResolver) Option {
	return func(o *options) { o.Persons = r }
}

// Built-in connectors. Each is registered only when given, and takes the
// App's tenant, agent names and file store, which is why it is an option
// rather than a connector the caller builds.

// WithSlack registers the Slack connector.
func WithSlack(c Slack) Option { return func(o *options) { o.Slack = c } }

// WithGitHub registers the GitHub connector.
func WithGitHub(c GitHub) Option { return func(o *options) { o.GitHub = c } }

// WithJira registers the Jira connector.
func WithJira(c Jira) Option { return func(o *options) { o.Jira = c } }

// WithWebhooks registers the outbound webhook sink with its endpoints, by
// the workspace a webhook:// address names. Each needs a secret.
func WithWebhooks(endpoints map[string]webhook.Endpoint) Option {
	return func(o *options) { o.Webhooks = endpoints }
}

// WithSchedules registers the cron connector with its standing requests.
func WithSchedules(s ...cron.Schedule) Option {
	return func(o *options) { o.Schedules = append(o.Schedules, s...) }
}
