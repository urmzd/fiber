package fiber

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

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

// options are everything New is given: what a program built on fiber adds to
// the composition (model providers, tool sources, connectors, MCP tool
// providers) and the constructed backends it chooses. A backend left nil is
// the in-memory implementation.
type options struct {
	// Providers are model providers by the name a spec's provider field
	// selects. A name already built in is replaced.
	Providers map[string]native.ProviderFunc
	// Catalogs resolve tool names a spec grants, after the connectors'
	// actions and in order: the first that resolves a name wins.
	Catalogs []runtime.Catalog
	// Connectors are registered beside the built-in ones; their roles are
	// derived the same way.
	Connectors []connector.Connector
	// ToolProviders are MCP tool providers recorded in the tool provider
	// store at startup, with the tools accepted from each.
	ToolProviders []toolProviderSeed
	// Persons finds the credential of the person a run acts for, for the
	// person-scoped providers among them. Nil finds none.
	Persons mcptools.PersonResolver

	// Backends. Nil is memory, except where Postgres fills them.
	Bus         bus.Bus
	Definitions definition.Source
	Ledger      Ledger
	Queue       dqueue.Queue
	Threads     thread.Store
	Memberships membership.Store
	Knowledge   knowledge.Store
	Embedder    knowledge.Embedder
	Blobs       blob.Store
	Tools       toolprovider.Store
	Runtime     turn.Runtime
	Postgres    *pgxpool.Pool
	Workspace   string
	RuntimeOpts []runtime.Option
	WorkerOpts  []worker.Option
	EgressOpts  []egress.Option

	// Deployment.
	Tenant         string
	AuthorProvider string
	AuthorModel    string
	Compaction     native.Compaction
	Concurrency    int
	Schedules      []cron.Schedule
	Slack          Slack
	GitHub         GitHub
	Jira           Jira
	Webhooks       map[string]webhook.Endpoint
	Log            *slog.Logger
	Metrics        metrics.Recorder
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
func (e options) providerNames() []string {
	names := definition.KnownProviders()
	for name := range e.Providers {
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// checks are the deployment facts definitions are validated against.
func (e options) checks() definition.Checks {
	return definition.Checks{Providers: e.providerNames()}
}

// catalog chains the connectors' actions with the extensions' catalogs. A
// name none of them resolves fails with every reason, so the operator log
// says where it was looked for. A catalog that also supplies tool groups
// (native.ToolGroups) keeps doing so through the chain; "mcp:" grants of
// the tool provider store are supplied ahead of all of them by the sandbox's
// group source.
func catalog(first runtime.Catalog, rest []runtime.Catalog) runtime.Catalog {
	if len(rest) == 0 {
		return first
	}
	all := append([]runtime.Catalog{first}, rest...)
	return func(req runtime.Request, action func(event.Action) error) (native.Tools, error) {
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
type chained []native.Tools

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

func (c chained) ToolGroup(name string) (native.ToolGroup, error) {
	errs := []error{fmt.Errorf("%w: %q", native.ErrNoSuchToolGroup, name)}
	for _, t := range c {
		groups, ok := t.(native.ToolGroups)
		if !ok {
			continue
		}
		g, err := groups.ToolGroup(name)
		if err == nil {
			return g, nil
		}
		errs = append(errs, err)
	}
	return native.ToolGroup{}, errors.Join(errs...)
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

// checkSeeds refuses a tool provider declared twice for one tenant: which
// declaration wins would be left to the order of options.
func checkSeeds(tenant string, seeds []toolProviderSeed) error {
	seen := map[[2]string]bool{}
	for _, seed := range seeds {
		t := seed.Provider.Tenant
		if t == "" {
			t = tenant
		}
		key := [2]string{t, seed.Provider.Name}
		if seen[key] {
			return configf("tool provider %q is declared twice", seed.Provider.Name)
		}
		seen[key] = true
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
			return fmt.Errorf("fiber: tool provider %q: %w", p.Name, err)
		}
		if len(seed.Accept) == 0 {
			continue
		}
		accept := func() ([]heldBack, error) { return acceptDeclared(ctx, store, rec, seed.Accept, dial) }
		if rec.Scope() == toolprovider.ScopePerson {
			accept = func() ([]heldBack, error) { return acceptDeclaredInFull(ctx, store, rec, seed.Accept) }
		}
		held, err := accept()
		if err != nil {
			log.Warn("fiber: tool provider not seeded; its tools are unavailable until it is reachable",
				"provider", p.Name, "tenant", p.Tenant, "error", err)
			continue
		}
		for _, h := range held {
			log.Warn("fiber: tool held back pending review; it does not match what the deployment declares",
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
		cur.Scope() == p.Scope() && cur.CredentialRef == p.CredentialRef && cur.CredentialEnv == p.CredentialEnv {
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

// acceptDeclaredInFull accepts the declared tools of a person-scoped
// provider, which is not dialled at startup: it has no credential of its
// own, and dialling it with nobody's is what its scope rules out. Each
// declared tool must therefore carry its whole form, the description and
// input schema the server offers, and its schema hash must match that
// schema; such a declaration is the admin's acceptance of exactly that form.
// A tool declared without it is left pending and logged. Each person's
// session compares what the server offers them with what was accepted, and
// holds back anything that differs, as every session does.
func acceptDeclaredInFull(ctx context.Context, store toolprovider.Store, p toolprovider.Provider, declared []toolprovider.Tool) ([]heldBack, error) {
	cur, err := store.Snapshot(ctx, p.Tenant, p.Name, 0)
	if err != nil {
		return nil, err
	}
	var accept []toolprovider.Tool
	var held []heldBack
	for _, d := range declared {
		full, err := toolprovider.NewTool(d.Name, d.Description, d.InputSchema)
		switch {
		case err != nil:
			held = append(held, heldBack{tool: d, reason: err.Error()})
			continue
		case full.SchemaHash != d.SchemaHash:
			held = append(held, heldBack{tool: d, reason: "a person-scoped provider is not dialled at startup, " +
				"so a declared tool carries its description and input schema, and this schema does not match its schema_hash"})
			continue
		}
		full.Class = d.Class
		if full.Class == "" {
			full.Class = toolprovider.ClassWrite
		}
		accept = append(accept, full)
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
