// Package author turns the agent control plane into tools an agent can call.
//
// Every other connector reaches outward, to Slack or GitHub or Jira. This one
// reaches inward: its actions wrap a definition.Source, so an agent granted
// them can list, read, compare, lint, draft and change the agents its own
// tenant runs. An agent is a saige definition (an .agent.md file) and a fiber
// Policy; the actions take and show both. The package also carries the
// built-in agent that uses those tools (see Spec and Seed), and two
// model-free skills that are useful without any agent at all: Review, a
// prompt linter, and Scaffold, which lays out a router and its specialists.
//
// # Where the tenant comes from
//
// The source is tenant-scoped and takes the tenant as an explicit argument.
// The one thing this package must never do is let a model supply it. A tool
// argument is text the model wrote, possibly while reading text an attacker
// wrote, so a tenant taken from arguments is a tenant anyone can name.
//
// connector.Action.Invoke is handed three things: a context, the address of
// the surface the tag came from, and the model's arguments. None of them is a
// tenant. The address names a workspace (a Slack team, a GitHub owner), which
// is not a tenant and must not be treated as one. What the runtime does bind,
// outside the model's reach, is the durable run id: connector.RunID(ctx).
//
// So the tenant and the acting human are resolved from the run, through the
// Principals seam. The composition root implements it against the run ledger,
// where the accepted tag (and therefore the tenant the server stamped on it and
// the actor the trigger authenticated) is recorded. No action schema in this
// package has a tenant field, arguments are decoded strictly so an extra field
// is an error rather than something to ignore, and a call whose run cannot be
// resolved is refused: the default is deny.
//
// # Reads, proposals and the one write
//
// Neither pkg/connector nor pkg/runtime has an approval or confirmation
// concept, so a change is two steps by construction:
//
//   - A propose action validates the change, renders the diff and the review
//     findings, stages it, and returns a single-use token. It changes nothing
//     in the source.
//   - author_apply takes that token and performs the change. It is the only
//     action that writes.
//
// The token alone would let a model propose and apply in one breath, which is
// not a pause. So apply refuses a token that was issued in the same run. A
// human answering "yes, apply it" raises a new tag, which is a new run, and
// that run may apply. The pause is enforced by run identity, which the model
// cannot forge, rather than by the prompt asking nicely.
//
// A version's author is the actor of the run that applied the change: the
// person who approved it, not the model and not whoever drafted it.
//
// # Versions
//
// A definition is insert only. A revise is a new, higher version, proposed
// against the latest version the model read (a compare-and-set checked again
// at apply). A delete retires the name: it stops new runs and keeps every
// version readable.
//
// Every action declares its Effect, and the three classes are also visible in
// the name: author_propose_* stages, author_apply writes, everything else
// reads.
//
// # Limitations
//
// Staged proposals live in this process's memory. They do not survive a
// restart and are not shared between replicas; a proposal that is lost is
// proposed again. Any identified actor who can tag an agent holding
// author_apply can approve a change, so who may tag that agent is the
// authorization decision, and it is the deployment's to make.
//
// A knowledge search tool belongs here and is left as a seam (Knowledge)
// rather than an import, so this package depends on the control plane and
// nothing else.
package author

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/definition"
)

// Name is the connector's name and the prefix of every action it contributes.
const Name = "author"

// Errors callers match with errors.Is.
var (
	// ErrNoRun reports a call that is not bound to a durable run. The run is
	// the only source of a tenant this package accepts, so there is nothing
	// safe to do without one.
	ErrNoRun = errors.New("author: call is not bound to a run")
	// ErrNoPrincipal reports a run whose tenant could not be resolved.
	ErrNoPrincipal = errors.New("author: run has no resolvable tenant")
	// ErrNoActor reports a write attempted by a run nobody identifiable
	// raised, such as a schedule. A version must say who made it.
	ErrNoActor = errors.New("author: run has no identified actor")
	// ErrInvalid reports arguments this package will not act on.
	ErrInvalid = errors.New("author: invalid")
	// ErrToken reports an apply whose token names no pending proposal for
	// this tenant: it was never issued, already used, expired, or belongs to
	// another tenant. The four are deliberately indistinguishable.
	ErrToken = errors.New("author: no such pending proposal")
	// ErrSameRun reports an apply attempted by the run that proposed the
	// change. Approval has to arrive as a new run.
	ErrSameRun = errors.New("author: a proposal cannot be applied by the run that made it")
	// ErrTooManyPending reports a tenant at its cap of staged proposals.
	ErrTooManyPending = errors.New("author: too many pending proposals")
)

// Effect classifies what an action does to the source.
type Effect string

// The three classes. Only EffectWrite changes the source.
const (
	// EffectRead observes and computes. It changes nothing anywhere.
	EffectRead Effect = "read"
	// EffectPropose stages a change in this connector and returns a token.
	// The source is untouched.
	EffectPropose Effect = "propose"
	// EffectWrite changes the source.
	EffectWrite Effect = "write"
)

// Principal is who a run acts as.
type Principal struct {
	// Tenant is the authorization scope the server stamped on the run's tag.
	Tenant string
	// Actor identifies the human or system that raised the run's tag, in
	// whatever stable form the deployment records authorship. Empty when
	// nobody identifiable raised it.
	Actor string
}

// Principals resolves a durable run to the principal it acts as.
//
// It exists because connector.Action.Invoke carries the run id and nothing
// else about the run. The implementation belongs to the composition root,
// which can read the accepted tag from the run ledger. An implementation must
// derive its answer from the run alone, and must return an error rather than a
// default when it cannot: a guessed tenant is the failure this seam prevents.
type Principals interface {
	Principal(ctx context.Context, runID string) (Principal, error)
}

// PrincipalFunc adapts a function into Principals.
type PrincipalFunc func(ctx context.Context, runID string) (Principal, error)

// Principal implements Principals.
func (f PrincipalFunc) Principal(ctx context.Context, runID string) (Principal, error) {
	return f(ctx, runID)
}

// Defaults.
const (
	// DefaultProposalTTL is how long a staged proposal waits for approval.
	DefaultProposalTTL = 24 * time.Hour
	// MaxPending caps staged proposals per tenant, so a looping agent cannot
	// grow this process's memory without bound.
	MaxPending = 32
)

// Config assembles a Connector.
type Config struct {
	// Source is where the tenant's definitions are kept. Required.
	Source definition.Source
	// Principals resolves a run to its tenant and actor. Required.
	Principals Principals
	// Checks are the deployment facts drafts are validated against. They
	// must be the ones the Source validates with, or a draft would pass here
	// and fail at apply.
	Checks definition.Checks
	// Catalog reports the tool names this deployment can resolve, which lets
	// Review tell a granted tool from a misspelled one. It is a function
	// because the connector registry is still being built when this
	// connector is constructed. Optional.
	Catalog func() []string
	// Knowledge, when set, contributes author_search_knowledge. Optional.
	Knowledge Knowledge
	// Clock is the time source for proposal expiry. Nil selects time.Now.
	Clock func() time.Time
	// ProposalTTL bounds how long a proposal stays applicable. Zero selects
	// DefaultProposalTTL.
	ProposalTTL time.Duration
}

// Connector is the control plane as an Actor. It raises no tags and renders no
// events, so it implements that one face only.
type Connector struct {
	src        definition.Source
	principals Principals
	checks     definition.Checks
	catalog    func() []string
	knowledge  Knowledge
	pending    *pending
}

// New builds the connector. It refuses a configuration with no store or no way
// to resolve a tenant, because the second would otherwise surface as every
// call being denied at runtime with nobody told why at startup.
func New(cfg Config) (*Connector, error) {
	if cfg.Source == nil {
		return nil, fmt.Errorf("%w: no definition source", ErrInvalid)
	}
	if cfg.Principals == nil {
		return nil, fmt.Errorf("%w: no Principals; without one no call can be scoped to a tenant", ErrInvalid)
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	ttl := cfg.ProposalTTL
	if ttl <= 0 {
		ttl = DefaultProposalTTL
	}
	return &Connector{
		src:        cfg.Source,
		principals: cfg.Principals,
		checks:     cfg.Checks,
		catalog:    cfg.Catalog,
		knowledge:  cfg.Knowledge,
		pending:    newPending(clock, ttl),
	}, nil
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return Name }

// call is the resolved context one action runs under. It is built from the run
// and never from arguments.
type call struct {
	Principal
	runID string
}

// handler is an action body. It receives the principal already resolved, so
// no handler has an opportunity to pick a tenant of its own.
type handler func(c *Connector, ctx context.Context, who call, raw json.RawMessage) (connector.Result, error)

// actionDef is the single table every listing in this package derives from:
// the catalog, the effect classification, and the built-in agent's grant.
type actionDef struct {
	name        string
	effect      Effect
	description string
	schema      string
	run         handler
}

// Actions implements connector.Actor.
//
// The target address is ignored on purpose. It names the surface the tag came
// from, and a workspace on some surface is not a tenant.
func (c *Connector) Actions() []connector.Action {
	defs := coreActions()
	if c.knowledge != nil {
		defs = append(defs, knowledgeAction())
	}
	out := make([]connector.Action, 0, len(defs))
	for _, d := range defs {
		out = append(out, connector.Action{
			Name:        d.name,
			Description: "[" + string(d.effect) + "] " + d.description,
			Schema:      json.RawMessage(d.schema),
			Invoke: func(ctx context.Context, _ address.Address, raw json.RawMessage) (connector.Result, error) {
				who, err := c.resolve(ctx)
				if err != nil {
					return connector.Result{}, err
				}
				return d.run(c, ctx, who, raw)
			},
		})
	}
	return out
}

// resolve turns the run bound to ctx into the principal the call acts as.
// Every failure is a refusal.
func (c *Connector) resolve(ctx context.Context) (call, error) {
	runID := connector.RunID(ctx)
	if runID == "" {
		return call{}, ErrNoRun
	}
	p, err := c.principals.Principal(ctx, runID)
	if err != nil {
		return call{}, fmt.Errorf("%w: run %s: %w", ErrNoPrincipal, runID, err)
	}
	if strings.TrimSpace(p.Tenant) == "" {
		return call{}, fmt.Errorf("%w: run %s", ErrNoPrincipal, runID)
	}
	return call{Principal: p, runID: runID}, nil
}

// EffectOf reports what the named action does to the source.
func EffectOf(action string) (Effect, bool) {
	for _, d := range append(coreActions(), knowledgeAction()) {
		if d.name == action {
			return d.effect, true
		}
	}
	return "", false
}

// ToolNames returns the names of the core actions with the given effects, in
// catalog order. With no effects it returns all of them. The optional
// knowledge action is not included, because whether it exists is a deployment
// decision and a spec that grants a tool the deployment lacks does not build.
func ToolNames(effects ...Effect) []string {
	var out []string
	for _, d := range coreActions() {
		if len(effects) == 0 || slices.Contains(effects, d.effect) {
			out = append(out, d.name)
		}
	}
	return out
}

// decode reads arguments strictly. An unknown field is an error rather than
// something to skip, so an argument such as "tenant" fails loudly instead of
// being quietly ignored, which keeps a prompt-injected call from looking like
// it worked.
func decode(raw json.RawMessage, into any) error {
	if len(strings.TrimSpace(string(raw))) == 0 || string(raw) == "null" {
		raw = json.RawMessage(`{}`)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("%w: arguments: %w", ErrInvalid, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: arguments: trailing data after the object", ErrInvalid)
	}
	return nil
}

// result builds a Result whose summary is what the model reads and whose data
// is the same content, structured, for the action.taken event.
func result(summary string, data any) (connector.Result, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: encode result: %w", err)
	}
	return connector.Result{Summary: summary, Data: raw}, nil
}
