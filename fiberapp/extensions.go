package fiberapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"

	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/anchor/pkg/worker"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/registry"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// extensions are what a program built on fiber adds to the composition root
// without forking it: model providers, tool sources, connectors, MCP tool
// providers and replacement backends. Options fill them from Go, and a
// providers file (Config.ProvidersFile, WithProvidersFile) from JSON.
type extensions struct {
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
	ToolProviders []toolProviderSeed
	// ProvidersFiles are providers files to load when the app is built,
	// for the deployment's tenant.
	ProvidersFiles []string

	// Backends and pieces that replace what Config would build. Nil builds
	// it from Config.
	Bus      bus.Bus
	Registry registry.Store
	Executor runtime.Executor
	// RuntimeOptions and WorkerOptions are appended after the app's own.
	RuntimeOptions []runtime.Option
	WorkerOptions  []worker.Option
}

// toolProviderSeed is an MCP tool provider a deployment declares rather than
// an admin registering it: the record, and the tools the deployment accepts
// from it, each pinned by name, schema hash and class. Only an offered tool
// whose schema hash matches its pin is accepted; anything the server adds or
// changes stays pending, so a restart never accepts what was not declared.
type toolProviderSeed struct {
	Provider toolprovider.Provider
	Accept   []toolprovider.Tool
}

// providerNames is the provider table definitions are validated against: the
// built-in names plus every extension's, sorted.
func (e extensions) providerNames() []string {
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
func (e extensions) merge(o extensions) (extensions, error) {
	out := e
	out.Providers = maps.Clone(e.Providers)
	out.Catalogs = slices.Concat(e.Catalogs, o.Catalogs)
	out.Connectors = slices.Concat(e.Connectors, o.Connectors)
	out.ToolProviders = slices.Clone(e.ToolProviders)
	if out.Providers == nil {
		out.Providers = map[string]agentrt.ProviderFunc{}
	}
	for name, fn := range o.Providers {
		if _, dup := out.Providers[name]; dup {
			return extensions{}, configf("model provider %q is declared twice", name)
		}
		out.Providers[name] = fn
	}
	for _, seed := range o.ToolProviders {
		if slices.ContainsFunc(out.ToolProviders, func(s toolProviderSeed) bool {
			return s.Provider.Tenant == seed.Provider.Tenant && s.Provider.Name == seed.Provider.Name
		}) {
			return extensions{}, configf("tool provider %q is declared twice", seed.Provider.Name)
		}
		out.ToolProviders = append(out.ToolProviders, seed)
	}
	return out, nil
}

// catalog chains the connectors' actions with the extensions' catalogs. A
// name none of them resolves fails with every reason, so the operator log
// says where it was looked for. A catalog that also supplies tool groups
// (agentrt.ToolGroups) keeps doing so through the chain; "mcp:" grants of
// the tool provider store are supplied ahead of all of them by the sandbox's
// group source.
func catalog(first runtime.Catalog, rest []runtime.Catalog) runtime.Catalog {
	if len(rest) == 0 {
		return first
	}
	all := append([]runtime.Catalog{first}, rest...)
	return func(req runtime.Request, action func(payload.Action) error) (agentrt.Tools, error) {
		resolved := make(chained, 0, len(all))
		for _, c := range all {
			t, err := c(req, action)
			if err != nil {
				return nil, err
			}
			if t != nil {
				resolved = append(resolved, t)
			}
		}
		return resolved, nil
	}
}

// chained is one turn's catalogs, asked in order.
type chained []agentrt.Tools

func (c chained) Lookup(name string) (saigetypes.Tool, error) {
	var errs []error
	for _, t := range c {
		tool, err := t.Lookup(name)
		if err == nil {
			return tool, nil
		}
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...)
}

func (c chained) ToolGroup(name string) (agentrt.ToolGroup, error) {
	errs := []error{fmt.Errorf("%w: %q", agentrt.ErrNoSuchToolGroup, name)}
	for _, t := range c {
		groups, ok := t.(agentrt.ToolGroups)
		if !ok {
			continue
		}
		g, err := groups.ToolGroup(name)
		if err == nil {
			return g, nil
		}
		errs = append(errs, err)
	}
	return agentrt.ToolGroup{}, errors.Join(errs...)
}

// providersFile is the shape of a providers file: model endpoints and MCP
// tool providers a deployment declares so that the common cases need no Go.
// It is JSON, like agent specs and schedules, and unknown fields are refused,
// so "api_key" where "api_key_env" belongs is an error rather than ignored.
type providersFile struct {
	// Models are OpenAI-compatible endpoints, each selectable by its name
	// as a spec's provider.
	Models []modelConfig `json:"models"`
	// ToolProviders are MCP servers, recorded in the tool provider store
	// at startup for the deployment's tenant.
	ToolProviders []toolProviderConfig `json:"tool_providers"`
}

// modelConfig is one OpenAI-compatible endpoint: agentrt.OpenAICompatible
// as a file spells it. The key is named, never written.
type modelConfig struct {
	Name string `json:"name"`
	// BaseURL is where the API is served. Empty is OpenAI itself.
	BaseURL string `json:"base_url,omitempty"`
	// APIKeyEnv names the environment variable holding the key, read when
	// an agent is built. Empty sends no key, which a local server expects.
	APIKeyEnv string `json:"api_key_env,omitempty"`
}

var (
	// modelName is a provider name a spec can select: no spaces, nothing
	// a JSON file or a flag would mangle.
	modelName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	// envName is a portable environment variable name.
	envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// endpoint validates m and returns the endpoint it declares.
func (m modelConfig) endpoint() (agentrt.OpenAICompatible, error) {
	if !modelName.MatchString(m.Name) {
		return agentrt.OpenAICompatible{}, fmt.Errorf("model %q: name must match %s", m.Name, modelName)
	}
	if m.BaseURL != "" {
		u, err := url.Parse(m.BaseURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return agentrt.OpenAICompatible{}, fmt.Errorf("model %q: base_url must be an http or https URL", m.Name)
		}
		if u.User != nil {
			// Not echoed: the point is that it may hold a secret.
			return agentrt.OpenAICompatible{}, fmt.Errorf("model %q: base_url carries credentials; use api_key_env", m.Name)
		}
	}
	if m.APIKeyEnv != "" && !envName.MatchString(m.APIKeyEnv) {
		return agentrt.OpenAICompatible{}, fmt.Errorf("model %q: api_key_env %q is not a variable name", m.Name, m.APIKeyEnv)
	}
	if m.BaseURL == "" && m.APIKeyEnv == "" {
		return agentrt.OpenAICompatible{}, fmt.Errorf("model %q: OpenAI itself needs api_key_env", m.Name)
	}
	return agentrt.OpenAICompatible{Name: m.Name, BaseURL: m.BaseURL, KeyEnv: m.APIKeyEnv}, nil
}

type toolProviderConfig struct {
	Name          string   `json:"name"`
	Transport     string   `json:"transport"`
	Endpoint      string   `json:"endpoint"`
	Args          []string `json:"args,omitempty"`
	CredentialRef string   `json:"credential_ref,omitempty"`
	CredentialEnv string   `json:"credential_env,omitempty"`
	// Tools are the tools the deployment accepts, each pinned by the hash
	// of its schema (toolprovider.Tool.SchemaHash, which the app logs for
	// every pending tool). A tool left out stays pending; a class
	// left out is write.
	Tools []struct {
		Name       string `json:"name"`
		SchemaHash string `json:"schema_hash"`
		Class      string `json:"class,omitempty"`
	} `json:"tools"`
}

// loadProvidersFile reads a providers file into extensions for tenant.
// Everything is validated here, so a typo fails New rather than a run.
func loadProvidersFile(path, tenant string) (extensions, error) {
	if strings.TrimSpace(path) == "" {
		return extensions{}, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return extensions{}, configf("providers: %v", err)
	}
	var f providersFile
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return extensions{}, configf("providers %q: %v", path, err)
	}
	ext := extensions{Providers: map[string]agentrt.ProviderFunc{}}
	for _, m := range f.Models {
		ep, err := m.endpoint()
		if err != nil {
			return extensions{}, configf("providers %q: %v", path, err)
		}
		if _, dup := ext.Providers[ep.Name]; dup {
			return extensions{}, configf("providers %q: model %q is declared twice", path, ep.Name)
		}
		ext.Providers[ep.Name] = ep.Provider()
	}
	for _, tp := range f.ToolProviders {
		p := toolprovider.Provider{
			Tenant: tenant, Name: tp.Name, Transport: toolprovider.Transport(tp.Transport), Endpoint: tp.Endpoint,
			Args: tp.Args, CredentialRef: tp.CredentialRef, CredentialEnv: tp.CredentialEnv,
		}
		if err := p.Validate(); err != nil {
			return extensions{}, configf("providers %q: %v", path, err)
		}
		seed := toolProviderSeed{Provider: p}
		for _, t := range tp.Tools {
			seed.Accept = append(seed.Accept, toolprovider.Tool{Name: t.Name, SchemaHash: t.SchemaHash, Class: toolprovider.Class(t.Class)})
		}
		if err := checkPins(seed); err != nil {
			return extensions{}, configf("providers %q: %v", path, err)
		}
		ext.ToolProviders = append(ext.ToolProviders, seed)
	}
	return extensions{}.merge(ext)
}

// checkPins refuses a declared tool with no name or no schema hash. A tool
// accepted without a pin would be accepted in whatever form the server offers
// it, which is the drift declaring it is meant to rule out.
func checkPins(seed toolProviderSeed) error {
	for _, t := range seed.Accept {
		switch {
		case t.Name == "":
			return fmt.Errorf("tool provider %q declares a tool with no name", seed.Provider.Name)
		case t.SchemaHash == "":
			return fmt.Errorf("tool provider %q: tool %q has no schema_hash; pending tools are logged with theirs", seed.Provider.Name, t.Name)
		}
	}
	return nil
}

// seedToolProviders records each declared tool provider and accepts the tools
// it pins. A record or snapshot that already matches is left alone, so a
// restart does not mint versions. A server that cannot be reached is logged
// and skipped: its grants are then unavailable to runs, which is how an
// unreachable server is treated at run time too.
//
// Declaring a tool is the admin's acceptance of that tool as pinned, and of
// nothing else: see acceptDeclared.
func seedToolProviders(ctx context.Context, store toolprovider.Store, tenant string, seeds []toolProviderSeed, dial mcptools.Dial, log *slog.Logger) error {
	for _, seed := range seeds {
		if err := checkPins(seed); err != nil {
			return configf("%v", err)
		}
	}
	for _, seed := range seeds {
		p := seed.Provider
		if p.Tenant == "" {
			p.Tenant = tenant
		}
		if p.Tenant == "" {
			return configf("tool provider %q: no tenant to record it under", p.Name)
		}
		rec, err := putProvider(ctx, store, p)
		if err != nil {
			return fmt.Errorf("fiberapp: tool provider %q: %w", p.Name, err)
		}
		if len(seed.Accept) == 0 {
			continue
		}
		held, err := acceptDeclared(ctx, store, rec, seed.Accept, dial)
		if err != nil {
			log.Warn("fiberapp: tool provider not seeded; its tools are unavailable until it is reachable",
				"provider", p.Name, "tenant", p.Tenant, "error", err)
			continue
		}
		for _, h := range held {
			log.Warn("fiberapp: tool held back pending review; it does not match what the deployment declares",
				"provider", p.Name, "tenant", p.Tenant, "tool", h.tool.Name, "reason", h.reason,
				"schema_hash", h.tool.SchemaHash)
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

// heldBack is an offered tool the declaration does not accept, and why.
type heldBack struct {
	tool   toolprovider.Tool
	reason string
}

// acceptDeclared accepts what the deployment pinned and nothing else, and
// reports every offered tool it left pending.
//
// An offered tool is accepted only when it is declared and its schema hash
// matches the pin. Its description is taken from the server the first time,
// and after that must match the accepted one: a description is what the
// model is told a tool does, so a reworded one is a change like a new schema.
// A declared tool that drifted keeps its previously accepted form in the
// snapshot, which the toolset then holds back until the server serves that
// form again or the pin is updated. A tool the server offers that is not
// declared is never accepted. The snapshot is written only when what it
// holds would change.
func acceptDeclared(ctx context.Context, store toolprovider.Store, p toolprovider.Provider, declared []toolprovider.Tool, dial mcptools.Dial) ([]heldBack, error) {
	s, err := dial(ctx, p)
	if err != nil {
		return nil, err
	}
	offered, err := s.Offered(ctx)
	_ = s.Close()
	if err != nil {
		return nil, err
	}
	cur, err := store.Snapshot(ctx, p.Tenant, p.Name, 0)
	if err != nil {
		return nil, err
	}
	byName := make(map[string]toolprovider.Tool, len(offered))
	for _, o := range offered {
		byName[o.Name] = o
	}
	var accept []toolprovider.Tool
	var held []heldBack
	for _, d := range declared {
		class := d.Class
		if class == "" {
			class = toolprovider.ClassWrite
		}
		prev, wasAccepted := cur.Tool(d.Name)
		o, ok := byName[d.Name]
		var reason string
		switch {
		case !ok:
			reason = "declared but not offered"
		case o.SchemaHash != d.SchemaHash:
			reason = "schema does not match the declared schema_hash"
		case wasAccepted && prev.SchemaHash == o.SchemaHash && prev.Description != o.Description:
			reason = "description changed since it was accepted"
		}
		if reason == "" {
			o.Class = class
			accept = append(accept, o)
			continue
		}
		if ok {
			held = append(held, heldBack{tool: o, reason: reason})
		} else {
			held = append(held, heldBack{tool: toolprovider.Tool{Name: d.Name, SchemaHash: d.SchemaHash}, reason: reason})
		}
		if wasAccepted && prev.SchemaHash == d.SchemaHash {
			// Keep the form that was accepted; the toolset holds it back
			// while the server offers something else.
			prev.Class = class
			accept = append(accept, prev)
		}
	}
	for _, o := range offered {
		if !slices.ContainsFunc(declared, func(d toolprovider.Tool) bool { return d.Name == o.Name }) {
			held = append(held, heldBack{tool: o, reason: "offered but not declared"})
		}
	}
	if !sameTools(cur.Tools, accept) {
		if _, err := store.Accept(ctx, p.Tenant, p.Name, cur.Version, accept, "config"); err != nil {
			return nil, err
		}
	}
	return held, nil
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
