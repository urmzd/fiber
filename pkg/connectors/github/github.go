// Package github is the GitHub peer in the mesh: trigger, sink and actor.
//
// # Trigger
//
// GitHub delivers events over a webhook signed with HMAC-SHA256 over the body,
// so the connector is an http.Handler and a pkg/connector.Trigger at once:
// Ingest holds the channel and parks, ServeHTTP authenticates and translates.
// Tag.ID is the X-GitHub-Delivery GUID, which GitHub reuses across retries and
// manual redeliveries — the property that makes it an idempotency key rather than
// merely a unique string.
//
// Six deliveries raise tags. Five are a human naming an agent in text: a comment
// on an issue or pull request, a review, a review comment, a new issue, a new
// pull request. The sixth is GitHub naming the agent structurally, by requesting
// a review from it — the case with no message at all, where the pull request
// itself is the request.
//
// Configured repository labels also raise tags when applied.
// Everything else is acknowledged and dropped, and an event whose sender is a Bot
// is dropped except for a labeled PR's synchronize event: this connector's own comment arrives back as an
// issue_comment, and acting on it would have the mesh answer itself in a loop.
//
// # Sink
//
// One comment per run, edited as the run streams. GitHub renders Markdown, which
// is what the model already writes, so the answer is passed through rather than
// escaped; only the fragments this package interpolates into its own markup are
// neutralized.
//
// # Actor
//
// github_comment, github_request_review and github_add_labels. Each returns the
// address of what it changed, so the action.taken event can cite it.
package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/connectors/internal/inbound"
	"github.com/urmzd/mandatum/pkg/connectors/internal/render"
	"github.com/urmzd/mandatum/pkg/connectors/internal/sink"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// Name is the address scheme this connector owns.
const Name = "github"

// MaxBodyChars is GitHub's limit on a comment body.
const MaxBodyChars = 65536

// Verifier authenticates a raw request body against its headers.
//
// *signature.GitHub satisfies it and is what a deployment should pass. The
// interface is declared here so the connector depends on the check rather than on
// one implementation of it.
type Verifier interface {
	Verify(header http.Header, body []byte) error
}

// Config is everything the GitHub connector needs.
type Config struct {
	// API talks to GitHub. Required.
	API API

	// Verifier authenticates inbound deliveries. Required for the trigger.
	Verifier Verifier

	// Owners restricts which accounts this connector serves. Empty accepts any,
	// which is right for a single-tenant deployment and wrong for a shared one:
	// with owners listed, a route naming another organization's repository is
	// refused with ErrUndeliverable instead of being attempted with our token.
	Owners []string

	// ReadRepositories grants cross-source read access to exact owner/repo
	// names. It never grants mutation or delivery access.
	ReadRepositories []string

	// Bots maps a GitHub login to the agent it stands for, and is how a review
	// request is resolved: GitHub names the reviewer, not the agent. A GitHub App
	// appears as "docs-bot[bot]" in some payloads and as its slug in a mention,
	// so both forms map here.
	Bots map[string]string

	// Agents is the set of agent names a mention may name. A connector with
	// neither Bots nor Agents raises no tags at all: on GitHub "@urmzd" and
	// "@docs-bot" are the same syntax, so an open name set would start a run
	// every time one human tagged another.
	Agents mention.Set

	// AutoLabels maps owner/repo to label-to-agent mappings. Only a labeled
	// event activates automatic runs; PR synchronize events keep reviewing while
	// the label remains. Empty is off.
	AutoLabels map[string]map[string]string

	// Checks publishes run progress on a PR's pinned head. Requires Checks write
	// permission and an API implementing CheckAPI.
	Checks bool

	// Guides maintains one navigational guide per PR and agent. Bots must map
	// the authenticated bot login to each agent whose PR guide is enabled.
	Guides bool

	// Mutations enables explicit /fix requests from repository writers.
	// MutationSecret signs run-bound grants; keep it stable across restarts.
	Mutations      bool
	MutationSecret []byte

	// Deliver overrides where a tag's events go. Empty answers on the issue or
	// pull request the tag came from.
	Deliver []envelope.Route

	// Interval coalesces comment edits. Zero means sink.DefaultInterval. GitHub's
	// secondary rate limits are stricter than Slack's about writes to one
	// resource, so a longer interval is reasonable here.
	Interval time.Duration

	// Clock is the time source for coalescing. Nil means time.Now.
	Clock func() time.Time

	// MaxBody bounds an inbound request body. Zero means inbound.DefaultMaxBody.
	MaxBody int64
}

// Connector is the GitHub peer.
type Connector struct {
	cfg       Config
	bots      map[string]string
	owners    map[string]bool
	gate      inbound.Gate
	pipe      inbound.Pipe
	engine    *sink.Engine
	checks    *sink.Engine
	guideMu   sync.Mutex
	readRepos map[string]bool
}

var (
	_ connector.Trigger = (*Connector)(nil)
	_ connector.Sink    = (*Connector)(nil)
	_ connector.Actor   = (*Connector)(nil)
	_ http.Handler      = (*Connector)(nil)
)

// New returns a GitHub connector.
func New(cfg Config) (*Connector, error) {
	if cfg.API == nil {
		return nil, fmt.Errorf("github: config needs an API")
	}
	c := &Connector{cfg: cfg}
	c.readRepos = make(map[string]bool, len(cfg.ReadRepositories))
	for _, repo := range cfg.ReadRepositories {
		parts := strings.Split(repo, "/")
		if len(parts) != 2 || address.ValidWorkspace(parts[0]) != nil || !validRepository(parts[1]) {
			return nil, fmt.Errorf("github: invalid read repository %q", repo)
		}
		c.readRepos[strings.ToLower(repo)] = true
	}
	c.cfg.MutationSecret = append([]byte(nil), cfg.MutationSecret...)
	if cfg.Mutations {
		if len(cfg.MutationSecret) < 16 {
			return nil, fmt.Errorf("github: mutations need a signing secret of at least 16 bytes")
		}
		if _, ok := cfg.API.(MutationAPI); !ok {
			return nil, fmt.Errorf("github: mutations need MutationAPI")
		}
		if _, ok := cfg.API.(ContextAPI); !ok {
			return nil, fmt.Errorf("github: mutations need ContextAPI")
		}
	}
	if cfg.Guides {
		if _, ok := cfg.API.(GuideAPI); !ok {
			return nil, fmt.Errorf("github: guides need GuideAPI")
		}
		if _, ok := cfg.API.(ContextAPI); !ok {
			return nil, fmt.Errorf("github: guides need ContextAPI")
		}
		if len(cfg.Bots) == 0 {
			return nil, fmt.Errorf("github: guides need the authenticated bot login in Bots")
		}
	}
	c.cfg.AutoLabels = make(map[string]map[string]string, len(cfg.AutoLabels))
	for repo, labels := range cfg.AutoLabels {
		parts := strings.Split(repo, "/")
		if len(parts) != 2 || address.ValidWorkspace(parts[0]) != nil || !validRepository(parts[1]) {
			return nil, fmt.Errorf("github: auto labels repository %q must be owner/repo", repo)
		}
		key := strings.ToLower(repo)
		if _, exists := c.cfg.AutoLabels[key]; exists {
			return nil, fmt.Errorf("github: duplicate auto labels repository %q", repo)
		}
		c.cfg.AutoLabels[key] = make(map[string]string, len(labels))
		for label, agent := range labels {
			if strings.TrimSpace(label) == "" || !mention.Valid(agent) {
				return nil, fmt.Errorf("github: invalid auto label %q or agent %q", label, agent)
			}
			c.cfg.AutoLabels[key][label] = agent
		}
	}

	c.bots = make(map[string]string, len(cfg.Bots))
	for login, agent := range cfg.Bots {
		if !mention.Valid(agent) {
			return nil, fmt.Errorf("github: bots[%q] = %q is not a usable agent name", login, agent)
		}
		c.bots[strings.ToLower(login)] = agent
		// A GitHub App's login carries a "[bot]" suffix in payloads but not in a
		// mention, so both spellings resolve without the operator listing each.
		c.bots[strings.ToLower(strings.TrimSuffix(login, "[bot]"))] = agent
	}
	if len(cfg.Owners) > 0 {
		c.owners = make(map[string]bool, len(cfg.Owners))
		for _, owner := range cfg.Owners {
			if err := address.ValidWorkspace(owner); err != nil {
				return nil, fmt.Errorf("github: owner %q cannot be an address workspace: %w", owner, err)
			}
			c.owners[strings.ToLower(owner)] = true
		}
	}

	c.gate = inbound.Gate{Verifier: cfg.Verifier, MaxBody: cfg.MaxBody}
	interval := cfg.Interval
	if interval == 0 {
		interval = sink.DefaultInterval
	}
	c.engine = sink.New(surface{c: c}, c.body, sink.WithInterval(interval), sink.WithClock(cfg.Clock))
	if cfg.Checks {
		api, ok := cfg.API.(CheckAPI)
		if !ok {
			return nil, fmt.Errorf("github: checks need CheckAPI")
		}
		c.checks = sink.New(checkSurface{c: c, api: api}, checkBody, sink.WithInterval(interval), sink.WithClock(cfg.Clock))
	}
	return c, nil
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return Name }

// Ingest implements connector.Trigger.
func (c *Connector) Ingest(ctx context.Context, out chan<- envelope.Tag) error {
	return c.pipe.Serve(ctx, out)
}

// Ingesting reports whether an Ingest is running, so a server can answer a
// readiness probe honestly.
func (c *Connector) Ingesting() bool { return c.pipe.Ingesting() }

// ServeHTTP handles one webhook delivery.
func (c *Connector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, ok := c.gate.Body(w, r)
	if !ok {
		return
	}

	// The ping GitHub sends when a webhook is created carries no event to
	// translate, and answering it is how the UI shows the hook as healthy.
	event := r.Header.Get(HeaderEvent)
	if event == "ping" {
		w.WriteHeader(http.StatusOK)
		return
	}

	var h hook
	if err := json.Unmarshal(body, &h); err != nil {
		http.Error(w, "malformed payload", http.StatusBadRequest)
		return
	}

	tag, ok := c.tagFrom(event, r.Header.Get(HeaderDelivery), h)
	if !ok {
		w.WriteHeader(http.StatusOK)
		return
	}
	// Issue-comment payloads identify a PR but omit its head. Resolve it before
	// accepting the tag so replay and check delivery never choose a later head.
	if (c.cfg.Checks || c.cfg.Mutations) && tag.Source.Path[1] == "pull" && tag.Source.Params["head_sha"] == "" && !c.cfg.Guides {
		api, supported := c.cfg.API.(ContextAPI)
		if !supported {
			http.Error(w, "github: checks need context reads for PR comments", http.StatusBadGateway)
			return
		}
		ref, err := c.ref(tag.Source)
		if err != nil {
			http.Error(w, "invalid PR reference", http.StatusBadRequest)
			return
		}
		ctx, err := api.ReadContext(r.Context(), ref, 1)
		if err != nil || !validSHA(ctx.Subject.Head.SHA) {
			http.Error(w, "could not resolve PR head", http.StatusBadGateway)
			return
		}
		tag.Source = tag.Source.WithParam("head_sha", ctx.Subject.Head.SHA)
		tag.Meta["github_head_sha"] = ctx.Subject.Head.SHA
	}
	if c.cfg.Guides && tag.Source.Path[1] == "pull" {
		if err := c.prepareGuide(r.Context(), &tag); err != nil {
			http.Error(w, "could not prepare current PR guide", http.StatusBadGateway)
			return
		}
	}
	if err := c.grantMutation(r.Context(), &tag); err != nil {
		http.Error(w, "could not authorize requested PR mutation", http.StatusBadGateway)
		return
	}
	if err := c.pipe.Send(r.Context(), tag); err != nil {
		http.Error(w, "not ready", inbound.SendStatus(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}

// agents is the set a mention or a reviewer login is resolved against: the
// configured bot logins first, then the agent name set.
func (c *Connector) agents() mention.Set { return agentSet{bots: c.bots, agents: c.cfg.Agents} }

type agentSet struct {
	bots   map[string]string
	agents mention.Set
}

// Lookup resolves a login or a mention token to an agent.
//
// The "[bot]" suffix is trimmed before every lookup because GitHub is
// inconsistent about it: requested_reviewer.login for a GitHub App is
// "docs-bot[bot]", while the same app is mentioned in a comment as "@docs-bot".
// Trimming here means an operator configures the login once, in either spelling.
func (s agentSet) Lookup(token string) (string, bool) {
	for _, candidate := range []string{token, strings.TrimSuffix(token, "[bot]")} {
		if agent, ok := s.bots[strings.ToLower(candidate)]; ok {
			return agent, true
		}
		if s.agents != nil {
			if agent, ok := s.agents.Lookup(candidate); ok {
				return agent, true
			}
		}
	}
	return "", false
}

// Deliver implements connector.Sink.
func (c *Connector) Deliver(ctx context.Context, target address.Address, e envelope.Event) error {
	if _, err := c.ref(target); err != nil {
		return err
	}
	if _, err := reviewRoot(target); err != nil {
		return err
	}
	commentErr := c.engine.Deliver(ctx, target, e)
	if c.checks != nil && target.Path[1] == "pull" && validSHA(target.Params["head_sha"]) {
		return errors.Join(commentErr, c.checks.Deliver(ctx, target, e))
	}
	return commentErr
}

// Flush writes every run whose comment edits are still being coalesced.
func (c *Connector) Flush(ctx context.Context) error {
	err := c.engine.Flush(ctx)
	if c.checks != nil {
		return errors.Join(err, c.checks.Flush(ctx))
	}
	return err
}

// ref reads a github address: github://<owner>/<repo>/{issues|pull}/<number>.
func (c *Connector) ref(target address.Address) (Ref, error) {
	if target.Connector != Name {
		return Ref{}, fmt.Errorf("%w: %s is not a github address", connector.ErrUndeliverable, target)
	}
	if c.owners != nil && !c.owners[strings.ToLower(target.Workspace)] {
		return Ref{}, fmt.Errorf("%w: %s is owned by %q, which this connector does not serve",
			connector.ErrUndeliverable, target, target.Workspace)
	}
	if len(target.Path) != 3 {
		return Ref{}, fmt.Errorf("%w: %s is not github://<owner>/<repo>/{issues|pull}/<number>",
			connector.ErrUndeliverable, target)
	}
	if address.ValidWorkspace(target.Workspace) != nil || !validRepository(target.Path[0]) {
		return Ref{}, fmt.Errorf("%w: invalid GitHub repository", connector.ErrUndeliverable)
	}
	switch target.Path[1] {
	case "issues", "pull":
	default:
		return Ref{}, fmt.Errorf("%w: %s names %q, want issues or pull",
			connector.ErrUndeliverable, target, target.Path[1])
	}
	number, err := strconv.Atoi(target.Path[2])
	if err != nil || number <= 0 {
		return Ref{}, fmt.Errorf("%w: %s does not end in an issue or pull request number",
			connector.ErrUndeliverable, target)
	}
	return Ref{Owner: target.Workspace, Repo: target.Path[0], Number: number}, nil
}

// surface adapts the GitHub API to the sink engine. The handle is the comment
// id, rendered as a string because that is what the engine carries.
type surface struct{ c *Connector }

func (s surface) Create(ctx context.Context, target address.Address, body string) (string, error) {
	ref, err := s.c.ref(target)
	if err != nil {
		return "", err
	}
	root, err := reviewRoot(target)
	if err != nil {
		return "", err
	}
	var id int64
	if root != 0 {
		api, ok := s.c.cfg.API.(ReplyAPI)
		if !ok {
			return "", fmt.Errorf("%w: GitHub review replies", connector.ErrUnsupported)
		}
		id, err = api.CreateReviewReply(ctx, ref, root, body)
	} else {
		id, err = s.c.cfg.API.CreateComment(ctx, ref, body)
	}
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(id, 10), nil
}

func (s surface) Update(ctx context.Context, target address.Address, handle, body string) error {
	ref, err := s.c.ref(target)
	if err != nil {
		return err
	}
	id, err := strconv.ParseInt(handle, 10, 64)
	if err != nil {
		return fmt.Errorf("github: comment handle %q is not an id: %w", handle, err)
	}
	root, err := reviewRoot(target)
	if err != nil {
		return err
	}
	if root != 0 {
		api, ok := s.c.cfg.API.(ReplyAPI)
		if !ok {
			return fmt.Errorf("%w: GitHub review replies", connector.ErrUnsupported)
		}
		return api.UpdateReviewComment(ctx, ref, id, body)
	}
	return s.c.cfg.API.UpdateComment(ctx, ref, id, body)
}

// body renders a run as a GitHub comment.
//
// Nothing is escaped: GitHub renders Markdown, the model writes Markdown, and
// escaping the answer would show a reader the asterisks instead of the emphasis.
// The fragments this package interpolates into its own markup are neutralized in
// markdown.go instead.
func (c *Connector) body(v render.View) string {
	return reviewBody(v)
}

// Actions implements connector.Actor.
func (c *Connector) Actions() []connector.Action {
	actions := []connector.Action{
		{
			Name:        "github_read_context",
			Description: "Read the issue or pull request, conversation, reviews, diff comments and changed-file patches in this run's repository. Follow next_page until absent. Missing patches are unavailable evidence, not proof that a file has no changes.",
			Schema:      json.RawMessage(`{"type":"object","properties":{"address":{"type":"string","description":"Optional GitHub issue/PR URL or github:// address. Other repositories require an operator read grant."},"number":{"type":"integer","minimum":1,"description":"Issue or PR number. Defaults to this run's subject."},"page":{"type":"integer","minimum":1,"description":"Page to read, starting at 1."}},"additionalProperties":false}`),
			Invoke:      c.readContext,
		},
		{
			Name: "github_comment",
			Description: "Post a comment on a GitHub issue or pull request. Use this " +
				"to leave a durable note for humans; the run's answer is already " +
				"delivered as its own comment and does not need reposting.",
			Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "body": {"type": "string", "description": "Comment body, GitHub Markdown."},
    "number": {"type": "integer", "description": "Issue or pull request number. Defaults to the one the run is answering on."}
  },
  "required": ["body"],
  "additionalProperties": false
}`),
			Invoke: c.comment,
		},
		{
			Name: "github_request_review",
			Description: "Request a review on a pull request from users or teams. Use " +
				"it when the change needs a human who has not been asked yet.",
			Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "reviewers": {"type": "array", "items": {"type": "string"}, "description": "GitHub logins to request a review from."},
    "teams": {"type": "array", "items": {"type": "string"}, "description": "Team slugs to request a review from."},
    "number": {"type": "integer", "description": "Pull request number. Defaults to the one the run is answering on."}
  },
  "additionalProperties": false
}`),
			Invoke: c.requestReview,
		},
		{
			Name: "github_add_labels",
			Description: "Add labels to a GitHub issue or pull request. Existing labels " +
				"are kept, so this is safe to call more than once.",
			Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "labels": {"type": "array", "items": {"type": "string"}, "description": "Label names to add."},
    "number": {"type": "integer", "description": "Issue or pull request number. Defaults to the one the run is answering on."}
  },
  "required": ["labels"],
  "additionalProperties": false
}`),
			Invoke: c.addLabels,
		},
	}
	actions = append(actions, c.fileAction(), c.workflowStatusAction())
	if c.cfg.Mutations {
		actions = append(actions, c.mutationAction(), c.runChecksAction())
	}
	return actions
}

func (c *Connector) comment(ctx context.Context, target address.Address, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Body   string `json:"body"`
		Number int    `json:"number"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return connector.Result{}, fmt.Errorf("github: github_comment args: %w", err)
	}
	if strings.TrimSpace(args.Body) == "" {
		return connector.Result{}, fmt.Errorf("github: github_comment needs a body")
	}
	ref, kind, err := c.resolve(target, args.Number)
	if err != nil {
		return connector.Result{}, err
	}
	id, err := c.cfg.API.CreateComment(ctx, ref, args.Body)
	if err != nil {
		return connector.Result{}, err
	}
	return connector.Result{
		Summary: fmt.Sprintf("commented on %s", ref),
		Address: refAddress(ref, kind).WithParam("comment", strconv.FormatInt(id, 10)),
	}, nil
}

func (c *Connector) requestReview(ctx context.Context, target address.Address, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Reviewers []string `json:"reviewers"`
		Teams     []string `json:"teams"`
		Number    int      `json:"number"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return connector.Result{}, fmt.Errorf("github: github_request_review args: %w", err)
	}
	if len(args.Reviewers) == 0 && len(args.Teams) == 0 {
		return connector.Result{}, fmt.Errorf("github: github_request_review needs a reviewer or a team")
	}
	ref, _, err := c.resolve(target, args.Number)
	if err != nil {
		return connector.Result{}, err
	}
	if err := c.cfg.API.RequestReview(ctx, ref, args.Reviewers, args.Teams); err != nil {
		return connector.Result{}, err
	}
	return connector.Result{
		Summary: fmt.Sprintf("requested review on %s from %s", ref, strings.Join(append(args.Reviewers, args.Teams...), ", ")),
		// A review request is always about a pull request, whatever the run's
		// surface was called.
		Address: refAddress(ref, "pull"),
	}, nil
}

func (c *Connector) addLabels(ctx context.Context, target address.Address, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Labels []string `json:"labels"`
		Number int      `json:"number"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return connector.Result{}, fmt.Errorf("github: github_add_labels args: %w", err)
	}
	if len(args.Labels) == 0 {
		return connector.Result{}, fmt.Errorf("github: github_add_labels needs at least one label")
	}
	ref, kind, err := c.resolve(target, args.Number)
	if err != nil {
		return connector.Result{}, err
	}
	if err := c.cfg.API.AddLabels(ctx, ref, args.Labels); err != nil {
		return connector.Result{}, err
	}
	return connector.Result{
		Summary: fmt.Sprintf("labelled %s %s", ref, strings.Join(args.Labels, ", ")),
		Address: refAddress(ref, kind),
	}, nil
}

// resolve settles which resource an action acts on.
//
// An action may name another issue or pull request in the same repository, and
// may not name another repository or owner: the route decided which repository
// this run can touch, and an argument the model produced must not be able to move
// that boundary.
func (c *Connector) resolve(target address.Address, number int) (Ref, string, error) {
	ref, err := c.ref(target)
	if err != nil {
		return Ref{}, "", err
	}
	kind := target.Path[1]
	if number > 0 && number != ref.Number {
		ref.Number = number
		// A different number may be an issue or a pull request; "issues"
		// addresses both on GitHub, and the sink only ever comments.
		kind = "issues"
	}
	return ref, kind, nil
}

func refAddress(ref Ref, kind string) address.Address {
	if kind != "pull" {
		kind = "issues"
	}
	return address.Address{
		Connector: Name,
		Workspace: ref.Owner,
		Path:      []string{ref.Repo, kind, strconv.Itoa(ref.Number)},
	}
}
