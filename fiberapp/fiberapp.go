// Package fiberapp is fiber's composition root, as a library: New assembles
// the registry, bus, router, durable runtime, sandbox, knowledge, threads,
// files, tool providers, the author agent and the connectors into an App,
// and Run drives it. There is no fiber binary and no fiber service; a program
// embeds an App, and talks to it in process.
//
//	app, err := fiberapp.New(ctx, fiberapp.Config{
//		Tenant:   "acme",
//		Postgres: os.Getenv("DATABASE_URL"), // every store family on Postgres
//		Bus:      os.Getenv("DATABASE_URL"), // and the bus too
//	}, fiberapp.WithModelProvider("house", newHouseProvider))
//	if err != nil { ... }
//	defer app.Close()
//	go app.Run(ctx)
//	acc, err := app.Invoke(ctx, tag)
//
// # Choices are data or interfaces
//
// Every backend is chosen by Config: a bus URL (memory, redis://,
// postgres://) and one Postgres URL for every store family, empty meaning
// memory. Model endpoints and MCP tool providers can be declared in a JSON
// providers file (Config.ProvidersFile). Anything that is code is an Option:
// a model provider, a tool source, a connector, an MCP tool provider, or a
// replacement for a whole backend (WithBus, WithRegistry, WithExecutor).
// Options and the file combine; a name declared by both is ErrConfig rather
// than an override.
//
// # Where it sits
//
// The package is at the module root rather than under pkg/ because it
// composes everything under pkg/ and nothing under pkg/ may depend on it.
// An App listens on nothing: connectors that receive webhooks are offered as
// http.Handlers for the embedding program to mount.
package fiberapp

import (
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/anchor/pkg/worker"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/registry"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// Option extends the composition root.
type Option func(*extensions)

func apply(opts []Option) extensions {
	var e extensions
	for _, o := range opts {
		if o != nil {
			o(&e)
		}
	}
	return e
}

// WithModelProvider makes a model provider selectable by name: a spec whose
// provider is name runs on what fn builds. The registry accepts specs naming
// it, and a built-in of the same name is replaced.
func WithModelProvider(name string, fn agentrt.ProviderFunc) Option {
	return func(e *extensions) {
		if name == "" || fn == nil {
			return
		}
		if e.Providers == nil {
			e.Providers = map[string]agentrt.ProviderFunc{}
		}
		e.Providers[name] = fn
	}
}

// WithOpenAICompatible registers an OpenAI-compatible endpoint under ep.Name.
// It is WithModelProvider with ep.Provider(); the key is read from the
// environment variable ep.KeyEnv names when an agent is built.
func WithOpenAICompatible(ep agentrt.OpenAICompatible) Option {
	return WithModelProvider(ep.Name, ep.Provider())
}

// WithToolSource adds a catalog that spec tool names resolve against, after
// the connectors' actions. It is built per turn, so it can bind tools to the
// run's tenant and surface; action reports a call as an action.taken event.
func WithToolSource(c runtime.Catalog) Option {
	return func(e *extensions) {
		if c != nil {
			e.Catalogs = append(e.Catalogs, c)
		}
	}
}

// WithTools adds a fixed set of tools, resolved by name for every turn. It is
// WithToolSource for tools that need nothing from the run.
func WithTools(tools ...saigetypes.Tool) Option {
	static := agentrt.StaticTools(tools...)
	return WithToolSource(func(runtime.Request, func(payload.Action) error) (agentrt.Tools, error) {
		return static, nil
	})
}

// WithConnector registers a connector beside the built-in ones. Its roles
// (trigger, sink, actor) are derived from the interfaces it implements, as
// every connector's are.
func WithConnector(c connector.Connector) Option {
	return func(e *extensions) {
		if c != nil {
			e.Connectors = append(e.Connectors, c)
		}
	}
}

// WithToolProvider records an MCP tool provider at startup and accepts the
// tools in accept from it, each pinned by Name and SchemaHash and accepted
// with its Class (write when empty). Only an offered tool whose schema hash
// matches its pin is accepted, and its description may not change once it
// has been; anything the server adds or changes stays pending and is logged
// with its hash. A tool with no SchemaHash fails New. An empty p.Tenant is
// the deployment's tenant. Agents reach the provider with an "mcp:<name>" or
// "mcp:<name>:write" grant.
func WithToolProvider(p toolprovider.Provider, accept ...toolprovider.Tool) Option {
	return func(e *extensions) {
		e.ToolProviders = append(e.ToolProviders, toolProviderSeed{Provider: p, Accept: accept})
	}
}

// WithProvidersFile loads a providers file when the App is built, as
// Config.ProvidersFile does: OpenAI-compatible model endpoints and MCP tool
// providers with schema-hash pins, recorded for the deployment's tenant. A
// name it shares with another option or file is ErrConfig.
func WithProvidersFile(path string) Option {
	return func(e *extensions) {
		if path != "" {
			e.ProvidersFiles = append(e.ProvidersFiles, path)
		}
	}
}

// WithBus replaces the broker Config.Bus would open. The App does not close
// it.
func WithBus(b bus.Bus) Option {
	return func(e *extensions) { e.Bus = b }
}

// WithRegistry replaces the agent store Config.Postgres would select. The
// store decides which providers it accepts (registry.WithProviders); see
// App.Providers for the names this App can run.
func WithRegistry(s registry.Store) Option {
	return func(e *extensions) { e.Registry = s }
}

// WithExecutor replaces the playscape sandbox a turn runs in. Model
// providers, tool sources, compaction and knowledge are wired into the
// sandbox, so an executor passed here brings its own.
func WithExecutor(x runtime.Executor) Option {
	return func(e *extensions) { e.Executor = x }
}

// WithRuntimeOptions adds options to the durable runtime, after the App's
// own, so they can override them.
func WithRuntimeOptions(opts ...runtime.Option) Option {
	return func(e *extensions) { e.RuntimeOptions = append(e.RuntimeOptions, opts...) }
}

// WithWorkerOptions adds options to the durable worker Run starts, after the
// App's own.
func WithWorkerOptions(opts ...worker.Option) Option {
	return func(e *extensions) { e.WorkerOptions = append(e.WorkerOptions, opts...) }
}
