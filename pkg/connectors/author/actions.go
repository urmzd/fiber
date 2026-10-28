package author

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/turn"
)

// draftSchema is the JSON Schema of a draft, shared by every action that takes
// one. It has no tenant field, because a definition does not have a tenant:
// the run does.
const draftSchema = `{
      "type": "object",
      "description": "The complete agent: the definition file and its policy. Always send both whole, not a patch.",
      "properties": {
        "definition": {"type": "string", "description": "The .agent.md file: a --- line, YAML frontmatter (apiVersion: saige/v1, name, version, description, model {use: provider/model, fallback: [provider/model]}, tools {registry: [tool names], mcp: [{server}]}, subagents [{ref: name}], approval {ask: [tool names]}, limits), a --- line, then the system prompt. name is lowercase letters, digits and hyphens. version is semantic, such as 1.0.0."},
        "policy": {
          "type": "object",
          "description": "What fiber decides that the definition does not. Everything not listed is denied.",
          "properties": {
            "workspace_areas": {"type": "array", "items": {"type": "string"}, "description": "Workspace key prefixes it may use."},
            "mcp_write": {"type": "array", "items": {"type": "string"}, "description": "tools.mcp servers whose write tools it may call. Others are read only."},
            "knowledge": {"type": "array", "description": "Corpora it may retrieve from.", "items": {"type": "object", "properties": {"name": {"type": "string"}, "uri": {"type": "string"}, "options": {"type": "object"}}, "required": ["name", "uri"]}},
            "fallback_on": {"type": "array", "items": {"type": "string", "enum": ["any", "auth", "content_filter", "context_length", "rate_limit", "transient", "unavailable"]}, "description": "When to move to the next fallback model. Omit for any. \"any\" cannot be combined with others. Requires model.fallback."},
            "approvers": {"type": "array", "items": {"type": "string"}, "description": "Who may approve its approval.ask calls. role:<name> names a role."},
            "self_approval": {"type": "boolean"},
            "answerers": {"type": "array", "items": {"type": "string"}},
            "tagged_only": {"type": "boolean", "description": "Run only when a message names it."}
          }
        }
      },
      "required": ["definition"]
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
			description: "List this tenant's live agents at their latest version. Start here before changing anything.",
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
			description: "Read one agent's definition file and policy: the latest version, or a specific one.",
			schema: `{
  "type": "object",
  "properties": {
    "name": {"type": "string", "description": "Agent name."},
    "version": {"type": "string", "description": "Exact version, such as 1.2.0. Omit for the latest."}
  },
  "required": ["name"],
  "additionalProperties": false
}`,
			run: (*Connector).get,
		},
		{
			name:        "author_history",
			effect:      EffectRead,
			description: "Show every version of an agent, lowest first, with who wrote each one and when.",
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
			description: "Show what changed between two versions of one agent.",
			schema: `{
  "type": "object",
  "properties": {
    "name": {"type": "string", "description": "Agent name."},
    "from": {"type": "string", "description": "The older version."},
    "to": {"type": "string", "description": "The newer version. Omit for the latest."}
  },
  "required": ["name", "from"],
  "additionalProperties": false
}`,
			run: (*Connector).diff,
		},
		{
			name:        "author_validate",
			effect:      EffectRead,
			description: "Check a draft against the rules the store enforces, without saving it.",
			schema: `{
  "type": "object",
  "properties": {
    "draft": ` + draftSchema + `
  },
  "required": ["draft"],
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
    "draft": ` + draftSchema + `
  },
  "required": ["draft"],
  "additionalProperties": false
}`,
			run: (*Connector).review,
		},
		{
			name:   "author_scaffold",
			effect: EffectRead,
			description: "Draft a system of agents from a short description: one router plus named specialists. " +
				"Returns drafts in the order to create them. Saves nothing.",
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
    "draft": ` + draftSchema + `
  },
  "required": ["draft"],
  "additionalProperties": false
}`,
			run: (*Connector).proposeCreate,
		},
		{
			name:   "author_propose_revise",
			effect: EffectPropose,
			description: "Stage a new, higher version of an existing agent for approval. Returns the diff, the review findings and a token. " +
				"Nothing is saved until a person approves and the token is applied in a later turn.",
			schema: `{
  "type": "object",
  "properties": {
    "draft": ` + draftSchema + `,
    "expected_version": {"type": "string", "description": "The latest version you read and edited. The change is refused if the agent has moved on since."}
  },
  "required": ["draft", "expected_version"],
  "additionalProperties": false
}`,
			run: (*Connector).proposeRevise,
		},
		{
			name:   "author_propose_delete",
			effect: EffectPropose,
			description: "Stage the retirement of an agent for approval. Returns what depends on it and a token. " +
				"Nothing is retired until a person approves and the token is applied in a later turn.",
			schema: `{
  "type": "object",
  "properties": {
    "name": {"type": "string", "description": "Agent name."},
    "expected_version": {"type": "string", "description": "The latest version you read."}
  },
  "required": ["name", "expected_version"],
  "additionalProperties": false
}`,
			run: (*Connector).proposeDelete,
		},
		{
			name:   "author_apply",
			effect: EffectWrite,
			description: "Carry out one approved proposal by its token. This is the only action that changes the agents. " +
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

// versionView is a version as actions report it.
type versionView struct {
	Name       string            `json:"name"`
	Version    string            `json:"version"`
	Digest     string            `json:"digest"`
	PolicyHash string            `json:"policy_hash"`
	Definition string            `json:"definition"`
	Policy     definition.Policy `json:"policy,omitzero"`
	CreatedAt  time.Time         `json:"created_at"`
	CreatedBy  string            `json:"created_by"`
}

func view(r definition.Record) versionView {
	return versionView{
		Name: r.Name, Version: r.Version, Digest: r.Digest, PolicyHash: r.PolicyHash,
		Definition: r.Content, Policy: r.Policy, CreatedAt: r.CreatedAt, CreatedBy: r.CreatedBy,
	}
}

func draftText(d Draft) string {
	out := strings.TrimRight(d.Definition, "\n")
	if raw, err := json.MarshalIndent(d.Policy.Normalize(), "", "  "); err == nil && string(raw) != "{}" {
		out += "\npolicy: " + string(raw)
	}
	return out
}

// describe is a record's description, or "" when it no longer parses.
func (c *Connector) describe(r definition.Record) string {
	a, err := definition.Parse(r.Content, r.Policy, c.checks)
	if err != nil {
		return ""
	}
	return a.Spec.Description
}

func (c *Connector) list(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Cursor string `json:"cursor"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	recs, next, err := c.src.List(ctx, who.Tenant, definition.Page{Token: args.Cursor})
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: list: %w", err)
	}
	type row struct {
		Name        string `json:"name"`
		Version     string `json:"version"`
		Description string `json:"description,omitempty"`
	}
	rows := make([]row, 0, len(recs))
	var b strings.Builder
	fmt.Fprintf(&b, "%d agent(s)", len(recs))
	for _, r := range recs {
		d := c.describe(r)
		rows = append(rows, row{r.Name, r.Version, d})
		fmt.Fprintf(&b, "\n- %s (%s): %s", r.Name, r.Version, d)
	}
	if next != "" {
		fmt.Fprintf(&b, "\nmore agents follow; continue with cursor %s", next)
	}
	return result(b.String(), struct {
		Agents []row  `json:"agents"`
		Cursor string `json:"cursor,omitempty"`
	}{rows, next})
}

// read returns one version, or the latest when version is empty.
func (c *Connector) read(ctx context.Context, tenant, name, version string) (definition.Record, error) {
	name = definition.NormalizeName(name)
	if strings.TrimSpace(version) == "" {
		return c.src.Latest(ctx, tenant, name)
	}
	return c.src.Get(ctx, tenant, name, strings.TrimSpace(version))
}

func (c *Connector) get(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	rec, err := c.read(ctx, who.Tenant, args.Name, args.Version)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: get: %w", err)
	}
	summary := fmt.Sprintf("%s %s, by %s at %s\n%s",
		rec.Name, rec.Version, rec.CreatedBy, rec.CreatedAt.Format(time.RFC3339), draftText(draftOf(rec)))
	return result(summary, view(rec))
}

func (c *Connector) history(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Name string `json:"name"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	name := definition.NormalizeName(args.Name)
	recs, err := c.src.Versions(ctx, who.Tenant, name)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: history: %w", err)
	}
	_, latestErr := c.src.Latest(ctx, who.Tenant, name)
	retired := errors.Is(latestErr, definition.ErrRetired)
	type row struct {
		Version   string    `json:"version"`
		Digest    string    `json:"digest"`
		CreatedAt time.Time `json:"created_at"`
		CreatedBy string    `json:"created_by"`
	}
	rows := make([]row, 0, len(recs))
	var b strings.Builder
	fmt.Fprintf(&b, "%d version(s), lowest first", len(recs))
	if retired {
		b.WriteString("; the agent is retired")
	}
	for _, r := range recs {
		rows = append(rows, row{r.Version, r.Digest, r.CreatedAt, r.CreatedBy})
		fmt.Fprintf(&b, "\n- %s by %s at %s", r.Version, r.CreatedBy, r.CreatedAt.Format(time.RFC3339))
	}
	return result(b.String(), struct {
		Versions []row `json:"versions"`
		Retired  bool  `json:"retired,omitempty"`
	}{rows, retired})
}

func (c *Connector) diff(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Name string `json:"name"`
		From string `json:"from"`
		To   string `json:"to"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	if strings.TrimSpace(args.From) == "" {
		return connector.Result{}, fmt.Errorf("%w: from must name a version", ErrInvalid)
	}
	from, err := c.read(ctx, who.Tenant, args.Name, args.From)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: diff: %w", err)
	}
	to, err := c.read(ctx, who.Tenant, args.Name, args.To)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: diff: %w", err)
	}
	d := Diff(draftOf(from), draftOf(to))
	summary := fmt.Sprintf("%s %s to %s\n%s", to.Name, from.Version, to.Version, d)
	if d == "" {
		summary = fmt.Sprintf("%s %s and %s are the same definition", to.Name, from.Version, to.Version)
	}
	return result(summary, struct {
		From string `json:"from"`
		To   string `json:"to"`
		Diff string `json:"diff"`
	}{from.Version, to.Version, d})
}

// parsed is a draft that compiled.
type parsed struct {
	Draft
	agent definition.Agent
}

// compile checks a draft exactly as the source does on write.
func (c *Connector) compile(d *Draft) (parsed, error) {
	if d == nil || strings.TrimSpace(d.Definition) == "" {
		return parsed{}, fmt.Errorf("%w: no draft definition", ErrInvalid)
	}
	a, err := definition.Parse(d.Definition, d.Policy, c.checks)
	if err != nil {
		return parsed{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return parsed{Draft: Draft{Definition: d.Definition, Policy: a.Policy}, agent: a}, nil
}

func (c *Connector) validate(_ context.Context, _ call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Draft *Draft `json:"draft"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	type verdict struct {
		Valid      bool   `json:"valid"`
		Digest     string `json:"digest,omitempty"`
		PolicyHash string `json:"policy_hash,omitempty"`
		Error      string `json:"error,omitempty"`
	}
	p, err := c.compile(args.Draft)
	if err != nil {
		// An invalid draft is the answer to the question, not a failed call.
		return result("invalid: "+err.Error(), verdict{Error: err.Error()})
	}
	return result("valid; nothing was saved", verdict{Valid: true, Digest: p.agent.Definition.Digest, PolicyHash: p.Policy.Hash()})
}

// world gathers what Review needs to know about the tenant. It reads only the
// tenant the run resolved to.
func (c *Connector) world(ctx context.Context, tenant string) (World, error) {
	w := World{}
	if c.catalog != nil {
		w.Tools = c.catalog()
	}
	page := definition.Page{Size: definition.MaxPageSize}
	for range maxWorldPages {
		recs, next, err := c.src.List(ctx, tenant, page)
		if err != nil {
			return World{}, fmt.Errorf("author: read tenant agents: %w", err)
		}
		for _, r := range recs {
			if a, err := definition.Parse(r.Content, r.Policy, c.checks); err == nil {
				w.Agents = append(w.Agents, a.Spec)
			}
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
		Draft *Draft `json:"draft"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	p, err := c.compile(args.Draft)
	if err != nil {
		return connector.Result{}, err
	}
	w, err := c.world(ctx, who.Tenant)
	if err != nil {
		return connector.Result{}, err
	}
	findings := Review(p.agent.Spec, w)
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
	drafts, err := ScaffoldWith(sys, c.checks)
	if err != nil {
		return connector.Result{}, err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d draft(s), in the order to create them; nothing was saved", len(drafts))
	for _, d := range drafts {
		b.WriteString("\n")
		b.WriteString(draftText(d))
	}
	return result(b.String(), drafts)
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
		if p.Version != "" {
			fmt.Fprintf(&b, " %s", p.Version)
		}
		if p.ExpectedVersion != "" {
			fmt.Fprintf(&b, " (from %s)", p.ExpectedVersion)
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

// proposalOf stages a compiled draft as a write.
func proposalOf(op string, p parsed) proposal {
	d := p.agent.Definition
	return proposal{
		Op: op, Agent: d.Name, Version: d.Version, Digest: d.Digest, PolicyHash: p.Policy.Hash(),
		record: definition.Record{Name: d.Name, Version: d.Version, Content: p.Definition, Policy: p.Policy},
	}
}

func (c *Connector) proposeCreate(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Draft *Draft `json:"draft"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	p, err := c.compile(args.Draft)
	if err != nil {
		return connector.Result{}, err
	}
	name := p.agent.Definition.Name
	switch _, err := c.src.Latest(ctx, who.Tenant, name); {
	case err == nil:
		return connector.Result{}, fmt.Errorf("%w: agent %q; revise it instead", definition.ErrExists, name)
	case errors.Is(err, definition.ErrRetired):
		return connector.Result{}, fmt.Errorf("%w: agent %q was retired and its name is not reused", definition.ErrRetired, name)
	case !errors.Is(err, definition.ErrNotFound):
		return connector.Result{}, fmt.Errorf("author: propose create: %w", err)
	}
	w, err := c.world(ctx, who.Tenant)
	if err != nil {
		return connector.Result{}, err
	}
	return c.stage(who, proposalOf(opCreate, p),
		"create agent "+name+" "+p.agent.Definition.Version,
		Diff(Draft{}, p.Draft),
		Review(p.agent.Spec, w))
}

func (c *Connector) proposeRevise(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Draft           *Draft `json:"draft"`
		ExpectedVersion string `json:"expected_version"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	if strings.TrimSpace(args.ExpectedVersion) == "" {
		return connector.Result{}, fmt.Errorf("%w: expected_version must be the latest version you read; read the agent first", ErrInvalid)
	}
	p, err := c.compile(args.Draft)
	if err != nil {
		return connector.Result{}, err
	}
	d := p.agent.Definition
	cur, err := c.src.Latest(ctx, who.Tenant, d.Name)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: propose revise: %w", err)
	}
	if cur.Version != strings.TrimSpace(args.ExpectedVersion) {
		return connector.Result{}, fmt.Errorf("%w: agent %q is at %s, not %s; read it again and redo the edit on top of that", definition.ErrConflict, d.Name, cur.Version, args.ExpectedVersion)
	}
	if cur.Digest == d.Digest && cur.PolicyHash == p.Policy.Hash() {
		return result(fmt.Sprintf("no change: this is already %s %s; nothing was proposed", cur.Name, cur.Version), view(cur))
	}
	if !newer(d.Version, cur.Version) {
		return connector.Result{}, fmt.Errorf("%w: a revise is a new version above %s; %s is not", ErrInvalid, cur.Version, d.Version)
	}
	w, err := c.world(ctx, who.Tenant)
	if err != nil {
		return connector.Result{}, err
	}
	prop := proposalOf(opRevise, p)
	prop.ExpectedVersion = cur.Version
	return c.stage(who, prop,
		fmt.Sprintf("revise agent %s from %s to %s", d.Name, cur.Version, d.Version),
		Diff(draftOf(cur), p.Draft),
		Review(p.agent.Spec, w))
}

// newer reports whether version a is above b.
func newer(a, b string) bool {
	c, err := definition.CompareVersions(a, b)
	return err == nil && c > 0
}

func (c *Connector) proposeDelete(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Name            string `json:"name"`
		ExpectedVersion string `json:"expected_version"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	cur, err := c.src.Latest(ctx, who.Tenant, definition.NormalizeName(args.Name))
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: propose delete: %w", err)
	}
	if cur.Version != strings.TrimSpace(args.ExpectedVersion) {
		return connector.Result{}, fmt.Errorf("%w: agent %q is at %s, not %s; read it again before retiring it", definition.ErrConflict, cur.Name, cur.Version, args.ExpectedVersion)
	}
	w, err := c.world(ctx, who.Tenant)
	if err != nil {
		return connector.Result{}, err
	}
	// Retiring an agent strands every delegation that names it, which is the
	// one consequence a person approving a delete needs to see.
	var findings []Finding
	for _, a := range w.Agents {
		if a.Name != cur.Name && delegatesTo(a, cur.Name) {
			findings = append(findings, Finding{
				Code: CodeSpawnMissing, Severity: SeverityWarning, Subject: a.Name,
				Message: fmt.Sprintf("agent %q delegates to %q and will no longer resolve", a.Name, cur.Name),
			})
		}
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].Subject < findings[j].Subject })
	return c.stage(who,
		proposal{Op: opDelete, Agent: cur.Name, ExpectedVersion: cur.Version},
		fmt.Sprintf("retire agent %s at %s (every version is kept, the name stops accepting tags)", cur.Name, cur.Version),
		"", findings)
}

func delegatesTo(a turn.Spec, name string) bool {
	for _, t := range a.Access.Spawn {
		if t == name {
			return true
		}
	}
	return false
}

// apply carries out a staged proposal. It is the only place this package
// writes to the source.
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

	// cas checks the agent is still at the version the change was drafted
	// against, so a version that landed after the approval is never
	// overwritten or retired unseen.
	cas := func() error {
		cur, err := c.src.Latest(ctx, who.Tenant, p.Agent)
		if err != nil {
			return err
		}
		if cur.Version != p.ExpectedVersion {
			return fmt.Errorf("%w: agent %q moved to %s after %s was approved", definition.ErrConflict, p.Agent, cur.Version, p.ExpectedVersion)
		}
		return nil
	}
	var (
		version = p.Version
		summary string
	)
	switch p.Op {
	case opCreate:
		rec := p.record
		rec.Tenant, rec.CreatedBy = who.Tenant, by
		_, err = c.src.Put(ctx, rec)
		summary = "created"
	case opRevise:
		if err = cas(); err == nil {
			rec := p.record
			rec.Tenant, rec.CreatedBy = who.Tenant, by
			_, err = c.src.Put(ctx, rec)
		}
		summary = "revised"
	case opDelete:
		if err = cas(); err == nil {
			err = c.src.Retire(ctx, who.Tenant, p.Agent, by)
		}
		version = p.ExpectedVersion
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
		fmt.Sprintf("%s %s, now %s, recorded as approved by %s", summary, p.Agent, version, by),
		struct {
			Op         string `json:"op"`
			Agent      string `json:"agent"`
			Version    string `json:"version"`
			By         string `json:"by"`
			ProposedBy string `json:"proposed_by,omitempty"`
		}{p.Op, p.Agent, version, by, p.ProposedBy})
}

// terminal reports an apply failure that will not go away on retry, so the
// proposal that caused it is spent.
func terminal(err error) bool {
	for _, target := range []error{
		definition.ErrConflict, definition.ErrExists, definition.ErrNotFound, definition.ErrInvalid, definition.ErrRetired, ErrInvalid,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
