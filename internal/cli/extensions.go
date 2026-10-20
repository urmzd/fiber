package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"

	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// Extensions are what a binary built on fiber adds to the composition root
// without forking it: model providers, tool sources, connectors and MCP tool
// providers. The fiber command itself runs with none; fiberapp fills them
// from Go options, and `serve --providers FILE` from a file.
type Extensions struct {
	// Providers are model providers by the name a spec's provider field
	// selects. A name already built in is replaced.
	Providers map[string]agentrt.ProviderFunc
	// Catalogs resolve tool names a spec grants, after the connectors'
	// actions and in order: the first that resolves a name wins.
	Catalogs []runtime.Catalog
	// Connectors are registered beside the built-in ones; their roles are
	// derived the same way.
	Connectors []connector.Connector
	// ToolProviders are MCP tool providers recorded in the tool provider
	// store at startup, with the tools accepted from each.
	ToolProviders []ToolProviderSeed
}

// ToolProviderSeed is an MCP tool provider a deployment declares rather than
// an admin registering it: the record, and the tools (by name, with the class
// to accept each as) the deployment accepts from it. A tool's description and
// schema are taken from what the server offers at startup.
type ToolProviderSeed struct {
	Provider toolprovider.Provider
	Accept   []toolprovider.Tool
}

// providerNames is the provider table definitions are validated against: the
// built-in names plus every extension's, sorted.
func (e Extensions) providerNames() []string {
	names := agentspec.KnownProviders()
	for name := range e.Providers {
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// merge adds o to e, refusing a name both declare: a provider or a tool
// provider defined twice would leave which one runs to the order of startup.
func (e Extensions) merge(o Extensions) (Extensions, error) {
	out := Extensions{
		Providers:     maps.Clone(e.Providers),
		Catalogs:      slices.Concat(e.Catalogs, o.Catalogs),
		Connectors:    slices.Concat(e.Connectors, o.Connectors),
		ToolProviders: slices.Clone(e.ToolProviders),
	}
	if out.Providers == nil {
		out.Providers = map[string]agentrt.ProviderFunc{}
	}
	for name, fn := range o.Providers {
		if _, dup := out.Providers[name]; dup {
			return Extensions{}, usagef("model provider %q is declared twice", name)
		}
		out.Providers[name] = fn
	}
	for _, seed := range o.ToolProviders {
		if slices.ContainsFunc(out.ToolProviders, func(s ToolProviderSeed) bool {
			return s.Provider.Tenant == seed.Provider.Tenant && s.Provider.Name == seed.Provider.Name
		}) {
			return Extensions{}, usagef("tool provider %q is declared twice", seed.Provider.Name)
		}
		out.ToolProviders = append(out.ToolProviders, seed)
	}
	return out, nil
}

// catalog chains the connectors' actions with the extensions' catalogs. A
// name none of them resolves fails with every reason, so the operator log
// says where it was looked for.
func catalog(first runtime.Catalog, rest []runtime.Catalog) runtime.Catalog {
	if len(rest) == 0 {
		return first
	}
	all := append([]runtime.Catalog{first}, rest...)
	return func(req runtime.Request, action func(payload.Action) error) (agentrt.Tools, error) {
		resolved := make([]agentrt.Tools, 0, len(all))
		for _, c := range all {
			t, err := c(req, action)
			if err != nil {
				return nil, err
			}
			if t != nil {
				resolved = append(resolved, t)
			}
		}
		return agentrt.ToolsFunc(func(name string) (saigetypes.Tool, error) {
			var errs []error
			for _, t := range resolved {
				tool, err := t.Lookup(name)
				if err == nil {
					return tool, nil
				}
				errs = append(errs, err)
			}
			return nil, errors.Join(errs...)
		}), nil
	}
}

// providersFile is the shape of `serve --providers FILE`: model endpoints and
// MCP tool providers a deployment declares so that the common cases need no Go.
// It is JSON, like agent specs and schedules, and unknown fields are refused,
// so "api_key" where "api_key_env" belongs is an error rather than ignored.
type providersFile struct {
	// Models are OpenAI-compatible endpoints, each selectable by its name
	// as a spec's provider.
	Models []agentrt.OpenAICompatible `json:"models"`
	// ToolProviders are MCP servers, recorded in the tool provider store
	// at startup for the deployment's tenant.
	ToolProviders []toolProviderConfig `json:"tool_providers"`
}

type toolProviderConfig struct {
	Name          string   `json:"name"`
	Transport     string   `json:"transport"`
	Endpoint      string   `json:"endpoint"`
	Args          []string `json:"args,omitempty"`
	CredentialRef string   `json:"credential_ref,omitempty"`
	CredentialEnv string   `json:"credential_env,omitempty"`
	// Tools are the tools the deployment accepts, each with its class.
	// A tool left out stays pending; a class left out is write.
	Tools []struct {
		Name  string `json:"name"`
		Class string `json:"class,omitempty"`
	} `json:"tools"`
}

// loadProvidersFile reads a providers file into Extensions for tenant.
// Everything is validated here, so a typo stops the server at startup.
func loadProvidersFile(path, tenant string) (Extensions, error) {
	if strings.TrimSpace(path) == "" {
		return Extensions{}, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Extensions{}, usagef("providers: %v", err)
	}
	var f providersFile
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return Extensions{}, usagef("providers %q: %v", path, err)
	}
	ext := Extensions{Providers: map[string]agentrt.ProviderFunc{}}
	for _, m := range f.Models {
		if err := m.Validate(); err != nil {
			return Extensions{}, usagef("providers %q: %v", path, err)
		}
		if _, dup := ext.Providers[m.Name]; dup {
			return Extensions{}, usagef("providers %q: model %q is declared twice", path, m.Name)
		}
		ext.Providers[m.Name] = agentrt.OpenAICompatibleProvider(m)
	}
	for _, tp := range f.ToolProviders {
		p := toolprovider.Provider{
			Tenant: tenant, Name: tp.Name, Transport: toolprovider.Transport(tp.Transport), Endpoint: tp.Endpoint,
			Args: tp.Args, CredentialRef: tp.CredentialRef, CredentialEnv: tp.CredentialEnv,
		}
		if err := p.Validate(); err != nil {
			return Extensions{}, usagef("providers %q: %v", path, err)
		}
		seed := ToolProviderSeed{Provider: p}
		for _, t := range tp.Tools {
			seed.Accept = append(seed.Accept, toolprovider.Tool{Name: t.Name, Class: toolprovider.Class(t.Class)})
		}
		ext.ToolProviders = append(ext.ToolProviders, seed)
	}
	return Extensions{}.merge(ext)
}

// seedToolProviders records each declared tool provider and accepts the tools
// it names. A record or snapshot that already matches is left alone, so a
// restart does not mint versions. A server that cannot be reached is logged
// and skipped: its grants are then unavailable to runs, which is how an
// unreachable server is treated at run time too.
//
// Declaring a tool here is the admin's acceptance: the description and schema
// the server offers at startup are what is accepted, and a server that
// changed a declared tool has the change accepted at the next restart.
func seedToolProviders(ctx context.Context, store toolprovider.Store, tenant string, seeds []ToolProviderSeed, dial mcptools.Dial, log *slog.Logger) error {
	for _, seed := range seeds {
		p := seed.Provider
		if p.Tenant == "" {
			p.Tenant = tenant
		}
		if p.Tenant == "" {
			return usagef("tool provider %q: no tenant to record it under", p.Name)
		}
		rec, err := putProvider(ctx, store, p)
		if err != nil {
			return fmt.Errorf("cli: tool provider %q: %w", p.Name, err)
		}
		if len(seed.Accept) == 0 {
			continue
		}
		if err := acceptDeclared(ctx, store, rec, seed.Accept, dial); err != nil {
			log.Warn("cli: tool provider not seeded; its tools are unavailable until it is reachable",
				"provider", p.Name, "tenant", p.Tenant, "error", err)
		}
	}
	return nil
}

// putProvider creates or updates the record when it differs from p.
func putProvider(ctx context.Context, store toolprovider.Store, p toolprovider.Provider) (toolprovider.Provider, error) {
	cur, err := store.GetProvider(ctx, p.Tenant, p.Name)
	switch {
	case errors.Is(err, toolprovider.ErrNotFound):
		p.UpdatedBy = "config"
		return store.PutProvider(ctx, p)
	case err != nil:
		return toolprovider.Provider{}, err
	}
	if cur.Transport == p.Transport && cur.Endpoint == p.Endpoint && slices.Equal(cur.Args, p.Args) &&
		cur.CredentialRef == p.CredentialRef && cur.CredentialEnv == p.CredentialEnv {
		return cur, nil
	}
	p.Version, p.UpdatedBy = cur.Version, "config"
	return store.PutProvider(ctx, p)
}

// acceptDeclared accepts the declared tools as the server offers them now,
// unless the current snapshot already holds exactly that.
func acceptDeclared(ctx context.Context, store toolprovider.Store, p toolprovider.Provider, declared []toolprovider.Tool, dial mcptools.Dial) error {
	s, err := dial(ctx, p)
	if err != nil {
		return err
	}
	offered, err := s.Offered(ctx)
	_ = s.Close()
	if err != nil {
		return err
	}
	byName := make(map[string]toolprovider.Tool, len(offered))
	for _, o := range offered {
		byName[o.Name] = o
	}
	var accept []toolprovider.Tool
	var missing []string
	for _, d := range declared {
		o, ok := byName[d.Name]
		if !ok {
			missing = append(missing, d.Name)
			continue
		}
		o.Class = d.Class
		if o.Class == "" {
			o.Class = toolprovider.ClassWrite
		}
		accept = append(accept, o)
	}
	cur, err := store.Snapshot(ctx, p.Tenant, p.Name, 0)
	if err != nil {
		return err
	}
	if !sameTools(cur.Tools, accept) {
		if _, err := store.Accept(ctx, p.Tenant, p.Name, cur.Version, accept, "config"); err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("the server does not offer %v", missing)
	}
	return nil
}

func sameTools(a, b []toolprovider.Tool) bool {
	if len(a) != len(b) {
		return false
	}
	for _, t := range b {
		i := slices.IndexFunc(a, func(x toolprovider.Tool) bool { return x.Name == t.Name })
		if i < 0 || a[i].Description != t.Description || a[i].SchemaHash != t.SchemaHash || a[i].Class.Effective() != t.Class.Effective() {
			return false
		}
	}
	return true
}
