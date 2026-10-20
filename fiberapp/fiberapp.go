// Package fiberapp runs fiber with extensions: the same command tree, server
// and composition root as the fiber binary, plus model providers, tool
// sources, connectors and MCP tool providers a downstream binary registers in
// Go. It exists so that adding a backend is an import and a few options, not
// a fork of internal/cli.
//
//	func main() {
//		os.Exit(fiberapp.Main(fiberapp.Version{Version: "v1"},
//			fiberapp.WithModelProvider("house", newHouseProvider),
//			fiberapp.WithConnector(myConnector),
//		))
//	}
//
// The common cases need no Go at all: `fiber serve --providers FILE` declares
// OpenAI-compatible model endpoints and MCP tool providers in JSON (see
// README.md). Options and the file combine; a name declared by both is an
// error rather than an override.
//
// # Where it sits
//
// The package is at the module root rather than under pkg/ because it is the
// public face of internal/cli, and nothing under pkg/ may import internal/.
// It adds no behaviour of its own: every option is a field of the
// composition root's configuration, and Main is the fiber command's Execute.
package fiberapp

import (
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/internal/cli"
	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// Version is the build metadata `fiber version` reports.
type Version struct {
	Version string
	Commit  string
	Date    string
}

// Option extends the composition root.
type Option func(*cli.Extensions)

// WithModelProvider makes a model provider selectable by name: a spec whose
// provider is name runs on what fn builds. The registry accepts specs naming
// it, and a built-in of the same name is replaced.
func WithModelProvider(name string, fn agentrt.ProviderFunc) Option {
	return func(e *cli.Extensions) {
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
	return func(e *cli.Extensions) {
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
	return func(e *cli.Extensions) {
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
// with its hash. A tool with no SchemaHash stops startup. An empty p.Tenant
// is the deployment's tenant. Agents reach the provider with an
// "mcp:<name>" or "mcp:<name>:write" grant.
func WithToolProvider(p toolprovider.Provider, accept ...toolprovider.Tool) Option {
	return func(e *cli.Extensions) {
		e.ToolProviders = append(e.ToolProviders, cli.ToolProviderSeed{Provider: p, Accept: accept})
	}
}

// Main runs the fiber command tree with the options applied and returns the
// process exit code, for os.Exit. Arguments come from os.Args, as for the
// fiber binary.
func Main(v Version, opts ...Option) int {
	return cli.ExecuteWith(cli.Version(v), extensions(opts))
}

func extensions(opts []Option) cli.Extensions {
	var e cli.Extensions
	for _, o := range opts {
		if o != nil {
			o(&e)
		}
	}
	return e
}
