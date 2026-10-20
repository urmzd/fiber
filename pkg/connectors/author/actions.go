package author

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/registry"
)

// specSchema is the JSON Schema of a draft definition, shared by every action
// that takes one. It mirrors agentspec.AgentSpec and has no tenant field,
// because a definition does not have a tenant: the run does.
const specSchema = `{
      "type": "object",
      "description": "The complete agent definition. Always send every field, not a patch.",
      "properties": {
        "name": {"type": "string", "description": "Agent name: letters, digits, dash and underscore."},
        "description": {"type": "string", "description": "One line on what the agent is for."},
        "model": {"type": "string", "description": "Model id."},
        "provider": {"type": "string", "description": "Provider name, such as anthropic, openai (also any OpenAI-compatible server), ollama or offline."},
        "system_prompt": {"type": "string", "description": "The agent's instructions."},
        "tools": {"type": "array", "items": {"type": "string"}, "description": "Tool names the agent may call. \"mcp:<provider>\" grants every accepted tool of that MCP provider."},
        "sources": {
          "type": "array",
          "description": "Corpora the agent may retrieve from.",
          "items": {
            "type": "object",
            "properties": {
              "name": {"type": "string"},
              "uri": {"type": "string"},
              "options": {"type": "object"}
            },
            "required": ["name", "uri"]
          }
        },
        "access": {
          "type": "object",
          "description": "What the agent may do beyond talking. Everything not listed is denied.",
          "properties": {
            "spawn": {"type": "array", "items": {"type": "string"}, "description": "Agents it may delegate to."},
            "workspace_areas": {"type": "array", "items": {"type": "string"}, "description": "Workspace key prefixes it may use."}
          }
        }
      },
      "required": ["name", "model", "provider", "system_prompt"]
    }`

const roleSchema = `{
      "type": "object",
      "properties": {
        "name": {"type": "string", "description": "Agent name."},
        "role": {"type": "string", "description": "One or two sentences on what it handles."},
        "tools": {"type": "array", "items": {"type": "string"}, "description": "Tool names to grant it."}
      },
      "required": ["name", "role"]
    }`

// maxWorldPages bounds how much of a tenant Review reads to check spawn
// targets, so a very large tenant costs a bounded number of store calls.
const maxWorldPages = 10

// coreActions is the catalog. Order is the order tools are shown to a model:
// reads, then proposals, then the one write.
func coreActions() []actionDef {
	return []actionDef{
		{
			name:        "author_list",
			effect:      EffectRead,
			description: "List this tenant's agents at their current revision. Start here before changing anything.",
			schema: `{
  "type": "object",
  "properties": {
    "cursor": {"type": "string", "description": "Continue a previous listing. Omit to start from the beginning."}
  },
  "additionalProperties": false
}`,
			run: (*Connector).list,
		},
		{
			name:        "author_get",
			effect:      EffectRead,
			description: "Read one agent's full definition: the current revision, or a specific one by number.",
			schema: `{
  "type": "object",
  "properties": {
    "name": {"type": "string", "description": "Agent name."},
    "rev": {"type": "integer", "description": "Revision number. Omit or 0 for the current one."}
  },
  "required": ["name"],
  "additionalProperties": false
}`,
			run: (*Connector).get,
		},
		{
			name:        "author_history",
			effect:      EffectRead,
			description: "Show every revision of an agent, oldest first, with who made each one and when.",
			schema: `{
  "type": "object",
  "properties": {
    "name": {"type": "string", "description": "Agent name."}
  },
  "required": ["name"],
  "additionalProperties": false
}`,
			run: (*Connector).history,
		},
		{
			name:        "author_diff",
			effect:      EffectRead,
			description: "Show what changed between two revisions of one agent.",
			schema: `{
  "type": "object",
  "properties": {
    "name": {"type": "string", "description": "Agent name."},
    "from": {"type": "integer", "description": "The older revision number."},
    "to": {"type": "integer", "description": "The newer revision number. Omit or 0 for the current one."}
  },
  "required": ["name", "from"],
  "additionalProperties": false
}`,
			run: (*Connector).diff,
		},
		{
			name:        "author_validate",
			effect:      EffectRead,
			description: "Check a draft definition against the rules the registry enforces, without saving it.",
			schema: `{
  "type": "object",
  "properties": {
    "spec": ` + specSchema + `
  },
  "required": ["spec"],
  "additionalProperties": false
}`,
			run: (*Connector).validate,
		},
		{
			name:   "author_review",
			effect: EffectRead,
			description: "Lint a draft definition's prompt and grants: missing goal or output format, tools referenced but not granted, " +
				"tools granted but unused, spawn targets that do not exist, delegation loops, volatile content, excessive length. Saves nothing.",
			schema: `{
  "type": "object",
  "properties": {
    "spec": ` + specSchema + `
  },
  "required": ["spec"],
  "additionalProperties": false
}`,
			run: (*Connector).review,
		},
		{
			name:   "author_scaffold",
			effect: EffectRead,
			description: "Draft a system of agents from a short description: one router plus named specialists. " +
				"Returns definitions in the order to create them. Saves nothing.",
			schema: `{
  "type": "object",
  "properties": {
    "router": ` + roleSchema + `,
    "specialists": {"type": "array", "description": "The agents the router delegates to.", "items": ` + roleSchema + `},
    "model": {"type": "string", "description": "Model id for every agent in the system."},
    "provider": {"type": "string", "description": "Provider name for every agent in the system."}
  },
  "required": ["router", "specialists", "model", "provider"],
  "additionalProperties": false
}`,
			run: (*Connector).scaffold,
		},
		{
			name:        "author_pending",
			effect:      EffectRead,
			description: "List the changes that have been proposed and are waiting for approval, with their tokens.",
			schema:      `{"type": "object", "properties": {}, "additionalProperties": false}`,
			run:         (*Connector).listPending,
		},
		{
			name:   "author_propose_create",
			effect: EffectPropose,
			description: "Stage a new agent for approval. Returns the definition, the review findings and a token. " +
				"Nothing is saved until a person approves and the token is applied in a later turn.",
			schema: `{
  "type": "object",
  "properties": {
    "spec": ` + specSchema + `
  },
  "required": ["spec"],
  "additionalProperties": false
}`,
			run: (*Connector).proposeCreate,
		},
		{
			name:   "author_propose_revise",
			effect: EffectPropose,
			description: "Stage a new revision of an existing agent for approval. Returns the diff, the review findings and a token. " +
				"Nothing is saved until a person approves and the token is applied in a later turn.",
			schema: `{
  "type": "object",
  "properties": {
    "spec": ` + specSchema + `,
    "expected_rev": {"type": "integer", "description": "The revision number you read and edited. The change is refused if the agent has moved on since."}
  },
  "required": ["spec", "expected_rev"],
  "additionalProperties": false
}`,
			run: (*Connector).proposeRevise,
		},
		{
			name:   "author_propose_delete",
			effect: EffectPropose,
			description: "Stage the retirement of an agent for approval. Returns what depends on it and a token. " +
				"Nothing is deleted until a person approves and the token is applied in a later turn.",
			schema: `{
  "type": "object",
  "properties": {
    "name": {"type": "string", "description": "Agent name."},
    "expected_rev": {"type": "integer", "description": "The current revision number you read."}
  },
  "required": ["name", "expected_rev"],
  "additionalProperties": false
}`,
			run: (*Connector).proposeDelete,
		},
		{
			name:   "author_apply",
			effect: EffectWrite,
			description: "Carry out one approved proposal by its token. This is the only action that changes the registry. " +
				"It is refused in the turn that made the proposal: stop, show the person the proposal, and apply only after they approve.",
			schema: `{
  "type": "object",
  "properties": {
    "token": {"type": "string", "description": "The token a propose action returned."}
  },
  "required": ["token"],
  "additionalProperties": false
}`,
			run: (*Connector).apply,
		},
	}
}

// revisionView is a revision as actions report it.
type revisionView struct {
	Spec      agentspec.AgentSpec `json:"spec"`
	Rev       int                 `json:"rev"`
	Hash      string              `json:"hash"`
	CreatedAt time.Time           `json:"created_at"`
	CreatedBy string              `json:"created_by"`
}

func view(r agentspec.Revision) revisionView {
	return revisionView{Spec: r.Spec, Rev: r.Rev, Hash: r.Hash, CreatedAt: r.CreatedAt, CreatedBy: r.CreatedBy}
}

func specJSON(s agentspec.AgentSpec) string {
	raw, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Sprintf("%+v", s)
	}
	return string(raw)
}

func (c *Connector) list(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Cursor string `json:"cursor"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	revs, next, err := c.store.List(ctx, who.Tenant, registry.Page{Token: args.Cursor})
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: list: %w", err)
	}
	type row struct {
		Name        string `json:"name"`
		Rev         int    `json:"rev"`
		Description string `json:"description,omitempty"`
	}
	rows := make([]row, 0, len(revs))
	var b strings.Builder
	fmt.Fprintf(&b, "%d agent(s)", len(revs))
	for _, r := range revs {
		rows = append(rows, row{r.Spec.Name, r.Rev, r.Spec.Description})
		fmt.Fprintf(&b, "\n- %s (rev %d): %s", r.Spec.Name, r.Rev, r.Spec.Description)
	}
	if next != "" {
		fmt.Fprintf(&b, "\nmore agents follow; continue with cursor %s", next)
	}
	return result(b.String(), struct {
		Agents []row  `json:"agents"`
		Cursor string `json:"cursor,omitempty"`
	}{rows, next})
}

func (c *Connector) get(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Name string `json:"name"`
		Rev  int    `json:"rev"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	rev, err := c.store.Get(ctx, who.Tenant, args.Name, args.Rev)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: get: %w", err)
	}
	summary := fmt.Sprintf("%s rev %d, by %s at %s\n%s",
		rev.Spec.Name, rev.Rev, rev.CreatedBy, rev.CreatedAt.Format(time.RFC3339), specJSON(rev.Spec))
	return result(summary, view(rev))
}

func (c *Connector) history(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Name string `json:"name"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	revs, err := c.store.History(ctx, who.Tenant, args.Name)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: history: %w", err)
	}
	type row struct {
		Rev       int       `json:"rev"`
		Hash      string    `json:"hash"`
		CreatedAt time.Time `json:"created_at"`
		CreatedBy string    `json:"created_by"`
	}
	rows := make([]row, 0, len(revs))
	var b strings.Builder
	fmt.Fprintf(&b, "%d revision(s), oldest first", len(revs))
	for _, r := range revs {
		rows = append(rows, row{r.Rev, r.Hash, r.CreatedAt, r.CreatedBy})
		fmt.Fprintf(&b, "\n- rev %d by %s at %s", r.Rev, r.CreatedBy, r.CreatedAt.Format(time.RFC3339))
	}
	return result(b.String(), rows)
}

func (c *Connector) diff(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Name string `json:"name"`
		From int    `json:"from"`
		To   int    `json:"to"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	if args.From < agentspec.FirstRev {
		return connector.Result{}, fmt.Errorf("%w: from must name a revision, counting from %d", ErrInvalid, agentspec.FirstRev)
	}
	from, err := c.store.Get(ctx, who.Tenant, args.Name, args.From)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: diff: %w", err)
	}
	to, err := c.store.Get(ctx, who.Tenant, args.Name, args.To)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: diff: %w", err)
	}
	d := Diff(from.Spec, to.Spec)
	summary := fmt.Sprintf("%s rev %d to rev %d\n%s", to.Spec.Name, from.Rev, to.Rev, d)
	if d == "" {
		summary = fmt.Sprintf("%s rev %d and rev %d are the same definition", to.Spec.Name, from.Rev, to.Rev)
	}
	return result(summary, struct {
		From int    `json:"from"`
		To   int    `json:"to"`
		Diff string `json:"diff"`
	}{from.Rev, to.Rev, d})
}

// draft canonicalizes and validates a definition exactly as the registry does
// on write: Normalize, then ValidateWith the deployment's provider table.
func (c *Connector) draft(spec agentspec.AgentSpec) (agentspec.AgentSpec, error) {
	canonical := spec.Normalize()
	if err := canonical.ValidateWith(c.providers); err != nil {
		return agentspec.AgentSpec{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return canonical, nil
}

type specArgs struct {
	Spec        *agentspec.AgentSpec `json:"spec"`
	ExpectedRev int                  `json:"expected_rev"`
}

func (a specArgs) spec() (agentspec.AgentSpec, error) {
	if a.Spec == nil {
		return agentspec.AgentSpec{}, fmt.Errorf("%w: no spec", ErrInvalid)
	}
	return *a.Spec, nil
}

func (c *Connector) validate(_ context.Context, _ call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Spec *agentspec.AgentSpec `json:"spec"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	spec, err := specArgs{Spec: args.Spec}.spec()
	if err != nil {
		return connector.Result{}, err
	}
	type verdict struct {
		Valid bool   `json:"valid"`
		Hash  string `json:"hash,omitempty"`
		Error string `json:"error,omitempty"`
	}
	canonical, err := c.draft(spec)
	if err != nil {
		// An invalid draft is the answer to the question, not a failed call.
		return result("invalid: "+err.Error(), verdict{Error: err.Error()})
	}
	return result("valid; nothing was saved", verdict{Valid: true, Hash: canonical.Hash()})
}

// world gathers what Review needs to know about the tenant. It reads only the
// tenant the run resolved to.
func (c *Connector) world(ctx context.Context, tenant string) (World, error) {
	w := World{}
	if c.catalog != nil {
		w.Tools = c.catalog()
	}
	page := registry.Page{Size: registry.MaxPageSize}
	for range maxWorldPages {
		revs, next, err := c.store.List(ctx, tenant, page)
		if err != nil {
			return World{}, fmt.Errorf("author: read tenant agents: %w", err)
		}
		for _, r := range revs {
			w.Agents = append(w.Agents, r.Spec)
		}
		if next == "" {
			break
		}
		page.Token = next
	}
	return w, nil
}

func (c *Connector) review(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Spec *agentspec.AgentSpec `json:"spec"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	spec, err := specArgs{Spec: args.Spec}.spec()
	if err != nil {
		return connector.Result{}, err
	}
	w, err := c.world(ctx, who.Tenant)
	if err != nil {
		return connector.Result{}, err
	}
	findings := Review(spec, w)
	if findings == nil {
		findings = []Finding{}
	}
	return result(renderFindings(findings), findings)
}

func (c *Connector) scaffold(_ context.Context, _ call, raw json.RawMessage) (connector.Result, error) {
	var sys System
	if err := decode(raw, &sys); err != nil {
		return connector.Result{}, err
	}
	specs, err := ScaffoldWith(sys, c.providers)
	if err != nil {
		return connector.Result{}, err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d draft definition(s), in the order to create them; nothing was saved", len(specs))
	for _, s := range specs {
		b.WriteString("\n")
		b.WriteString(specJSON(s))
	}
	return result(b.String(), specs)
}

func (c *Connector) listPending(_ context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	if err := decode(raw, &struct{}{}); err != nil {
		return connector.Result{}, err
	}
	props := c.pending.list(who.Tenant)
	var b strings.Builder
	fmt.Fprintf(&b, "%d pending proposal(s)", len(props))
	for _, p := range props {
		fmt.Fprintf(&b, "\n- %s %s", p.Op, p.Agent)
		if p.ExpectedRev > 0 {
			fmt.Fprintf(&b, " (from rev %d)", p.ExpectedRev)
		}
		fmt.Fprintf(&b, ", token %s, expires %s", p.Token, p.ExpiresAt.Format(time.RFC3339))
		if p.runID == who.runID {
			b.WriteString(", proposed in this turn so it cannot be applied yet")
		}
	}
	return result(b.String(), props)
}

// staged is what a propose action reports.
type staged struct {
	proposal
	Diff     string    `json:"diff,omitempty"`
	Findings []Finding `json:"findings"`
}

// stage records a proposal and renders it for a human to approve: what will
// change, what the review found, and the token that carries it out.
func (c *Connector) stage(who call, p proposal, headline, diff string, findings []Finding) (connector.Result, error) {
	p.runID = who.runID
	p.ProposedBy = who.Actor
	p, err := c.pending.stage(who.Tenant, p)
	if err != nil {
		return connector.Result{}, err
	}
	if findings == nil {
		findings = []Finding{}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "PROPOSED, NOT SAVED: %s\n", headline)
	if diff != "" {
		b.WriteString(diff)
		b.WriteString("\n")
	}
	b.WriteString(renderFindings(findings))
	fmt.Fprintf(&b, "\ntoken: %s (expires %s)\n", p.Token, p.ExpiresAt.Format(time.RFC3339))
	b.WriteString("Show the person the change and the findings above and stop. Apply the token only in a later turn, after they approve.")
	return result(b.String(), staged{proposal: p, Diff: diff, Findings: findings})
}

func (c *Connector) proposeCreate(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Spec *agentspec.AgentSpec `json:"spec"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	spec, err := specArgs{Spec: args.Spec}.spec()
	if err != nil {
		return connector.Result{}, err
	}
	canonical, err := c.draft(spec)
	if err != nil {
		return connector.Result{}, err
	}
	switch _, err := c.store.Get(ctx, who.Tenant, canonical.Name, 0); {
	case err == nil:
		return connector.Result{}, fmt.Errorf("%w: agent %q; revise it instead", registry.ErrExists, canonical.Name)
	case !errors.Is(err, registry.ErrNotFound):
		return connector.Result{}, fmt.Errorf("author: propose create: %w", err)
	}
	w, err := c.world(ctx, who.Tenant)
	if err != nil {
		return connector.Result{}, err
	}
	return c.stage(who,
		proposal{Op: opCreate, Agent: canonical.Name, Hash: canonical.Hash(), spec: canonical},
		"create agent "+canonical.Name,
		Diff(agentspec.AgentSpec{}, canonical),
		Review(canonical, w))
}

func (c *Connector) proposeRevise(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args specArgs
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	spec, err := args.spec()
	if err != nil {
		return connector.Result{}, err
	}
	if args.ExpectedRev < agentspec.FirstRev {
		return connector.Result{}, fmt.Errorf("%w: expected_rev must be the revision you read, counting from %d; read the agent first", ErrInvalid, agentspec.FirstRev)
	}
	canonical, err := c.draft(spec)
	if err != nil {
		return connector.Result{}, err
	}
	cur, err := c.store.Get(ctx, who.Tenant, canonical.Name, 0)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: propose revise: %w", err)
	}
	if cur.Rev != args.ExpectedRev {
		return connector.Result{}, fmt.Errorf("%w: agent %q is at rev %d, not rev %d; read it again and redo the edit on top of that", registry.ErrConflict, canonical.Name, cur.Rev, args.ExpectedRev)
	}
	if cur.Hash == canonical.Hash() {
		return result(fmt.Sprintf("no change: this is already %s rev %d; nothing was proposed", cur.Spec.Name, cur.Rev), view(cur))
	}
	w, err := c.world(ctx, who.Tenant)
	if err != nil {
		return connector.Result{}, err
	}
	return c.stage(who,
		proposal{Op: opRevise, Agent: canonical.Name, ExpectedRev: cur.Rev, Hash: canonical.Hash(), spec: canonical},
		fmt.Sprintf("revise agent %s from rev %d", canonical.Name, cur.Rev),
		Diff(cur.Spec, canonical),
		Review(canonical, w))
}

func (c *Connector) proposeDelete(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Name        string `json:"name"`
		ExpectedRev int    `json:"expected_rev"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	cur, err := c.store.Get(ctx, who.Tenant, args.Name, 0)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: propose delete: %w", err)
	}
	if cur.Rev != args.ExpectedRev {
		return connector.Result{}, fmt.Errorf("%w: agent %q is at rev %d, not rev %d; read it again before retiring it", registry.ErrConflict, cur.Spec.Name, cur.Rev, args.ExpectedRev)
	}
	w, err := c.world(ctx, who.Tenant)
	if err != nil {
		return connector.Result{}, err
	}
	// Retiring an agent strands every spawn grant that names it, which is the
	// one consequence a person approving a delete needs to see.
	var findings []Finding
	for _, a := range w.Agents {
		for _, target := range a.Access.Spawn {
			if target == cur.Spec.Name && a.Name != cur.Spec.Name {
				findings = append(findings, Finding{
					Code: CodeSpawnMissing, Severity: SeverityWarning, Subject: a.Name,
					Message: fmt.Sprintf("agent %q delegates to %q and will be left with a grant that resolves to nothing", a.Name, cur.Spec.Name),
				})
			}
		}
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].Subject < findings[j].Subject })
	return c.stage(who,
		proposal{Op: opDelete, Agent: cur.Spec.Name, ExpectedRev: cur.Rev},
		fmt.Sprintf("retire agent %s at rev %d (history is kept, the name stops accepting tags)", cur.Spec.Name, cur.Rev),
		"", findings)
}

// apply carries out a staged proposal. It is the only place this package
// writes to the registry.
func (c *Connector) apply(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Token string `json:"token"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	// The author is checked before the token is consumed, so a run with no
	// identified actor cannot burn somebody's approval by trying.
	by := strings.TrimSpace(who.Actor)
	if by == "" {
		return connector.Result{}, fmt.Errorf("%w: a change must record who approved it", ErrNoActor)
	}
	p, err := c.pending.take(who.Tenant, strings.TrimSpace(args.Token), who.runID)
	if err != nil {
		return connector.Result{}, err
	}

	var (
		rev     agentspec.Revision
		summary string
	)
	switch p.Op {
	case opCreate:
		rev, err = c.store.Create(ctx, who.Tenant, p.spec, by)
		summary = "created"
	case opRevise:
		rev, err = c.store.Revise(ctx, who.Tenant, p.spec, p.ExpectedRev, by)
		summary = "revised"
	case opDelete:
		// The store checks the revision and retires the name in one step, so
		// a revise that lands after the approval cannot be retired unseen.
		// What was approved is exactly what is read back: an explicit
		// revision resolves after the name is gone.
		err = c.store.Delete(ctx, who.Tenant, p.Agent, p.ExpectedRev)
		if err == nil {
			rev, err = c.store.Get(ctx, who.Tenant, p.Agent, p.ExpectedRev)
		}
		summary = "retired"
	default:
		err = fmt.Errorf("%w: proposal has unknown operation %q", ErrInvalid, p.Op)
	}
	if err != nil {
		if !terminal(err) {
			c.pending.restore(who.Tenant, p)
		}
		return connector.Result{}, fmt.Errorf("author: apply %s %s: %w", p.Op, p.Agent, err)
	}
	return result(
		fmt.Sprintf("%s %s, now rev %d, recorded as approved by %s", summary, p.Agent, rev.Rev, by),
		struct {
			Op         string `json:"op"`
			Agent      string `json:"agent"`
			Rev        int    `json:"rev"`
			By         string `json:"by"`
			ProposedBy string `json:"proposed_by,omitempty"`
		}{p.Op, p.Agent, rev.Rev, by, p.ProposedBy})
}

// terminal reports an apply failure that will not go away on retry, so the
// proposal that caused it is spent.
func terminal(err error) bool {
	for _, target := range []error{
		registry.ErrConflict, registry.ErrExists, registry.ErrNotFound, registry.ErrInvalid, agentspec.ErrInvalid, ErrInvalid,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
