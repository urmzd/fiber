package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/urmzd/legatus/pkg/controlplane"
	dispatchsandbox "github.com/urmzd/legatus/pkg/sandbox"
	"github.com/urmzd/legatus/pkg/task"
	"github.com/urmzd/legatus/pkg/tool"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/agentrt"
	"github.com/urmzd/mandatum/pkg/agentrt/payload"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/envelope"

	"github.com/urmzd/mandatum/pkg/attachment"
)

// Sandbox defaults.
const (
	// DefaultBuffer is the per-turn chunk buffer between the tool producing
	// events and the activity publishing them.
	DefaultBuffer = 256
	// DefaultPollInterval is how often a turn checks whether its legatus
	// task has reported a result.
	DefaultPollInterval = 5 * time.Millisecond
	// AnswerArea is the workspace area a turn writes its answer artifact
	// under. A revision that does not grant it simply has no artifact: the
	// policy decides, and the denial is reported rather than swallowed.
	AnswerArea = "runs"
)

// Catalog builds the tool catalog for one turn.
//
// action is called for every connector action the agent invokes; the Sandbox
// turns each call into an envelope.KindActionTaken event on the run's stream, so
// "the work got done" lands on the bus next to the conversation that decided to
// do it.
type Catalog func(req Request, action func(payload.Action) error) (agentrt.Tools, error)

// Connectors returns a Catalog resolving tool names against the actions every
// registered connector contributes, bound to the tag's own surface.
func Connectors(reg *connector.Registry) Catalog {
	return func(req Request, action func(payload.Action) error) (agentrt.Tools, error) {
		target, err := req.SourceAddress()
		if err != nil {
			return nil, err
		}
		return agentrt.ConnectorToolsForRun(reg, target, req.RunID, action), nil
	}
}

// SourceAddress parses the request's origin surface.
func (r Request) SourceAddress() (address.Address, error) {
	if r.Source == "" {
		return address.Address{}, nil
	}
	a, err := address.Parse(r.Source)
	if err != nil {
		return address.Address{}, fmt.Errorf("runtime: run %s: source: %w", r.RunID, err)
	}
	return a, nil
}

// Sandbox executes turns as legatus tasks, under the NGAC policy compiled from
// the pinned revision's Access grant.
//
// # What the sandbox is for
//
// A turn is arbitrary model-directed code: it calls tools, writes files, and may
// delegate to other agents. Access says what the agent may touch, and saying it
// is worth nothing unless something enforces it. legatus is that something: the
// tool never receives the shared workspace, only a view scoped to its policy, and
// every spawn attempt is checked against the policy's allowlist before it
// reaches a queue. Default is deny, so an agent granted nothing can do nothing.
//
// # One deployment per pinned revision
//
// The policy lives on the deployment, and a run pins a revision, so the
// deployment is keyed by (agent, revision): revising an agent's Access cannot
// retroactively widen or narrow what a run already in flight is allowed to do.
// The TOOL, in contrast, is keyed by agent name alone, because Access.Spawn names
// agents and a spawn allowlist has to name something an agent can know without
// knowing which revision of it is current.
//
// # Streaming out of a sandbox
//
// A legatus tool is a function over bytes: it has no way to stream, and the node
// running it holds neither the caller's context nor the run's replay frame. So
// the tool sends its chunks through an in-process channel registered under the
// run id, and the activity that submitted the task drains that channel on its own
// goroutine — which is what keeps every duraturo call (Emit, and the ledger
// writes behind it) on the single goroutine duraturo requires, while the agent
// loop's own parallelism stays inside the task.
//
// That handoff is in-process by construction. A remote legatus node would have
// to publish to the bus itself; nothing here pretends otherwise.
//
// # At-most-once, on purpose
//
// legatus's queue is at-most-once in beta. A lost task is a failed attempt, and
// the attempt is what duraturo already knows how to repeat. Durability is never
// borrowed from legatus.
type Sandbox struct {
	plane   controlplane.ControlPlane
	tools   *tool.Registry
	specs   Specs
	catalog Catalog
	// delegator, when set, carries out a delegation for a turn that is a
	// durable run, in place of the in-turn sub-task.
	delegator Delegator
	callTool  string

	agentOpts []agentrt.Option
	knowledge func(tenant string, sources []string) agentrt.Retriever
	replicas  int
	buffer    int
	poll      time.Duration
	logger    *slog.Logger

	mu         sync.Mutex
	deployed   map[string]string // "<agent>@<rev>" -> deployment name
	registered map[string]bool   // legatus tool name -> registered
	turns      map[string]*turn  // run id or sub-task key -> the turn someone is waiting on

	// subs numbers delegated sub-tasks, so each has a key to be cancelled by.
	subs atomic.Uint64
}

// taskInput is what a legatus task carries: the turn's Request, plus the
// sandbox's own bookkeeping, which is not part of a Request because nothing
// outside this file should ever see it. The Request's fields are embedded, so
// the task input of a durable run is the Request's JSON as it always was.
type taskInput struct {
	Request
	// Sub is the key a delegated sub-task's turn is registered under, so the
	// turn waiting on it can cancel it. Empty for a durable run, whose run id
	// is its key.
	Sub string `json:"sandbox_sub,omitempty"`
}

// turn is what the goroutine waiting on a legatus task shares with the tool
// executing it. The two never share a context (see call), so everything that
// has to cross between them crosses here.
type turn struct {
	// sink carries the turn's chunks out. For a delegated sub-task, which has
	// no stream of its own, only its usage is relayed from it onto its
	// parent's stream; the rest is dropped.
	sink chan agentrt.Chunk
	// stop asks the turn to wrap up. Nil for a turn that can only be
	// cancelled.
	stop <-chan agentrt.Stop
	// abort is closed when nobody is waiting on the turn any more: its
	// caller returned, was cancelled, or gave up on a stop. The tool cancels
	// its own context on it, which is the only cancellation that reaches a
	// legatus task from outside.
	abort chan struct{}
	// stopped is the stop request the turn was handed, once it has been: how
	// a delegation the stop cancels learns that it was a stop.
	stopped atomic.Pointer[agentrt.Stop]
	// childUsage checks and reads a child run a Delegator reports, from the
	// runtime executing the turn. Nil when the runtime offered none.
	childUsage func(context.Context, string) (payload.UsageChild, error)
	// childQuestion checks and reads a child run a Delegator watches for
	// questions (WatchChildQuestions). Nil when the runtime offered none.
	childQuestion func(context.Context, string) (payload.ChildQuestion, bool, bool, error)
}

// SandboxOption configures a Sandbox.
type SandboxOption func(*Sandbox)

// WithCatalog sets how a spec's tool names are resolved. Without one, a spec
// that names a tool fails to build, which is the safe default: an agent silently
// missing its tools answers the wrong question.
func WithCatalog(c Catalog) SandboxOption {
	return func(s *Sandbox) {
		if c != nil {
			s.catalog = c
		}
	}
}

// Delegator carries out one delegation: parent's turn asked target to do text.
// It returns the answer the parent's model reads as the tool's result.
//
// It is how a host makes a delegated turn a run in its own right, with an id, a
// stream and a record, where the default makes it a sub-task of the parent's
// turn whose only trace is the tool result. The host decides what that run is
// and how it is linked to parent; the runtime only stops answering the
// delegation itself.
//
// report puts an action on the parent's stream, so the host can say which run
// it started before that run has an answer.
//
// The spawn allowlist has already let the tool exist: a Delegator is called
// only for a target the parent's pinned revision names in Access.Spawn. A host
// with limits of its own enforces them here and returns the refusal as an
// error, which the model sees as the tool failing.
type Delegator func(ctx context.Context, parent Request, target, text string, report func(payload.Action) error) (string, error)

// WithDelegator sets who carries out a delegation made by a turn that is a
// durable run. A turn that is itself a sub-task has no run to hang another one
// from, so it keeps delegating by sub-task. Without a Delegator every
// delegation is a sub-task, which is what it always was.
func WithDelegator(d Delegator) SandboxOption {
	return func(s *Sandbox) { s.delegator = d }
}

// WithCallTool replaces per-agent tag tools with one host catalog tool for
// agents granted Access.Spawn. The host must enforce the pinned spawn grant.
func WithCallTool(name string) SandboxOption {
	return func(s *Sandbox) { s.callTool = name }
}

// WithAgentOptions passes options through to every agentrt.Runner the sandbox
// builds: the provider table, an offline script, a retriever, loop bounds.
func WithAgentOptions(opts ...agentrt.Option) SandboxOption {
	return func(s *Sandbox) { s.agentOpts = append(s.agentOpts, opts...) }
}

// WithKnowledge grounds turns in a knowledge store.
//
// It is a factory rather than a retriever because retrieval is scoped twice
// over and neither scope is known until a run starts: to the tenant the run
// belongs to, and to the sources its pinned revision declares. One retriever
// shared by every run would have to pick a tenant, and there is no right one to
// pick. The factory may return nil, which means this run retrieves nothing.
func WithKnowledge(f func(tenant string, sources []string) agentrt.Retriever) SandboxOption {
	return func(s *Sandbox) { s.knowledge = f }
}

// WithReplicas sets how many local execution nodes each deployment starts with.
func WithReplicas(n int) SandboxOption {
	return func(s *Sandbox) {
		if n > 0 {
			s.replicas = n
		}
	}
}

// WithBuffer sets the per-turn chunk buffer (default DefaultBuffer). It is
// backpressure, not a drop policy: a full buffer blocks the agent loop until the
// publisher catches up, because dropping an event from the middle of a stream
// would leave a sink rendering a conversation with a hole in it.
func WithBuffer(n int) SandboxOption {
	return func(s *Sandbox) {
		if n > 0 {
			s.buffer = n
		}
	}
}

// WithPollInterval sets how often a turn checks for its task's result.
func WithPollInterval(d time.Duration) SandboxOption {
	return func(s *Sandbox) {
		if d > 0 {
			s.poll = d
		}
	}
}

// WithSandboxLogger sets the logger.
func WithSandboxLogger(l *slog.Logger) SandboxOption {
	return func(s *Sandbox) {
		if l != nil {
			s.logger = l
		}
	}
}

// NewSandbox returns an executor over a legatus control plane.
//
// tools is the registry the plane's nodes resolve from; the sandbox registers one
// tool per agent into it as agents are first seen. specs is used only to resolve
// a delegation target's definition; passing nil disables delegation with an error
// the model can read.
func NewSandbox(plane controlplane.ControlPlane, tools *tool.Registry, specs Specs, opts ...SandboxOption) (*Sandbox, error) {
	if plane == nil {
		return nil, fmt.Errorf("%w: no legatus control plane", ErrInvalid)
	}
	if tools == nil {
		return nil, fmt.Errorf("%w: no legatus tool registry", ErrInvalid)
	}
	s := &Sandbox{
		plane:      plane,
		tools:      tools,
		specs:      specs,
		replicas:   1,
		buffer:     DefaultBuffer,
		poll:       DefaultPollInterval,
		logger:     slog.Default(),
		deployed:   make(map[string]string),
		registered: make(map[string]bool),
		turns:      make(map[string]*turn),
	}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

var _ Executor = (*Sandbox)(nil)

// ToolName is the legatus tool an agent's turns execute as. It is also the
// agent's identity in the policy graph, and the object a spawn grant names.
func ToolName(agent string) string { return "agent:" + agent }

// DeploymentName is the legatus deployment a pinned revision runs in.
func DeploymentName(agent string, rev int) string {
	return fmt.Sprintf("mandatum-%s-r%d", agent, rev)
}

// DelegateToolName is the tool an agent calls to tag another agent.
func DelegateToolName(agent string) string { return "tag_" + agent }

// AnswerKey is where a run's answer artifact is written.
func AnswerKey(agent, runID string) string {
	return path.Join(AnswerArea, agent, runID, "answer")
}

// Execute submits the turn as a legatus task and publishes what it streams.
func (s *Sandbox) Execute(ctx context.Context, req Request, emit func(context.Context, agentrt.Chunk) error) (Outcome, error) {
	if req.RunID == "" {
		return Outcome{}, fmt.Errorf("%w: turn has no run id", ErrInvalid)
	}
	deployment, err := s.ensure(ctx, req.Revision)
	if err != nil {
		return Outcome{}, err
	}
	req.Service = deployment

	sink := make(chan agentrt.Chunk, s.buffer)
	// A stop request reaches the tool as a channel, never as a call into this
	// type: the channel holds the request until the turn reads it, so a
	// request that arrives before the task has been picked up is not lost to
	// timing.
	childQuestion, _ := ctx.Value(childQuestionKey{}).(func(context.Context, string) (payload.ChildQuestion, bool, bool, error))
	if err := s.attach(req.RunID, &turn{sink: sink, stop: StopSignal(ctx), abort: make(chan struct{}), childUsage: childUsageOf(ctx), childQuestion: childQuestion}); err != nil {
		return Outcome{}, err
	}
	// Detaching aborts the task if it is still running, which it is on every
	// return below that is not the task's own result.
	defer s.detach(req.RunID)

	input, err := json.Marshal(taskInput{Request: req})
	if err != nil {
		return Outcome{}, fmt.Errorf("runtime: run %s: marshal request: %w", req.RunID, err)
	}
	id, err := s.plane.SubmitAsync(ctx, deployment, task.Task{Tool: ToolName(req.Agent()), Input: input})
	if err != nil {
		return Outcome{}, fmt.Errorf("runtime: run %s: submit to %s: %w", req.RunID, deployment, err)
	}

	return s.stream(ctx, req, deployment, id, sink, emit)
}

// stream drains the turn's chunks and returns its outcome.
//
// The loop watches the sink and polls for the task's result rather than waiting
// for the tool to close anything, because the paths that produce no chunks at all
// — an unknown tool, a node that died, a task the at-most-once queue lost — must
// end the turn too. Once a result has been reported, every chunk the tool sent is
// already in the buffer (it sent them all before returning, and a full buffer
// would have blocked it), so a non-blocking drain finishes the stream in order
// with nothing lost.
func (s *Sandbox) stream(ctx context.Context, req Request, deployment, taskID string, sink <-chan agentrt.Chunk, emit func(context.Context, agentrt.Chunk) error) (Outcome, error) {
	send := func(c agentrt.Chunk) error {
		if emit == nil {
			return nil
		}
		return emit(ctx, c)
	}
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// legatus owns the task's lifecycle and has no cancel, so the
			// tool is told through the turn it shares with this goroutine
			// (see detach). The run is what gets retried.
			return Outcome{}, fmt.Errorf("runtime: run %s: %w", req.RunID, ctx.Err())

		case c := <-sink:
			if err := send(c); err != nil {
				return Outcome{}, err
			}

		case <-ticker.C:
			res, done, err := s.plane.Result(ctx, deployment, taskID)
			if err != nil {
				return Outcome{}, fmt.Errorf("runtime: run %s: result: %w", req.RunID, err)
			}
			if !done {
				continue
			}
			for {
				select {
				case c := <-sink:
					if err := send(c); err != nil {
						return Outcome{}, err
					}
					continue
				default:
				}
				break
			}
			if res.Error != "" {
				return Outcome{}, fmt.Errorf("runtime: run %s: task %s: %s", req.RunID, taskID, res.Error)
			}
			var out Outcome
			if err := json.Unmarshal(res.Output, &out); err != nil {
				return Outcome{}, fmt.Errorf("runtime: run %s: decode outcome: %w", req.RunID, err)
			}
			return out, nil
		}
	}
}

// ensure deploys the service a pinned revision runs in, registering its tool and
// its delegation targets' tools, and returns the deployment name.
func (s *Sandbox) ensure(ctx context.Context, rev agentrt.Revision) (string, error) {
	agent := rev.Spec.Name
	key := fmt.Sprintf("%s@%d", agent, rev.Rev)

	s.mu.Lock()
	name, ok := s.deployed[key]
	s.mu.Unlock()
	if ok {
		return name, nil
	}

	name = DeploymentName(agent, rev.Rev)
	if err := s.register(ToolName(agent)); err != nil {
		return "", err
	}
	spawn := make([]string, 0, len(rev.Spec.Access.Spawn))
	for _, target := range rev.Spec.Access.Spawn {
		// A delegation target must be dispatchable before it can be spawned,
		// and it runs under its own tool name with no grants of its own: a
		// sub-task inherits nothing.
		if err := s.register(ToolName(target)); err != nil {
			return "", err
		}
		spawn = append(spawn, ToolName(target))
	}
	areas := make([]dispatchsandbox.Area, 0, len(rev.Spec.Access.WorkspaceAreas))
	for _, prefix := range rev.Spec.Access.WorkspaceAreas {
		areas = append(areas, dispatchsandbox.Area{Prefix: prefix})
	}

	err := s.plane.Deploy(ctx, controlplane.ServiceSpec{
		Name:     name,
		Replicas: s.replicas,
		Policies: []dispatchsandbox.Policy{{
			Tool:  ToolName(agent),
			Areas: areas,
			Spawn: spawn,
		}},
	})
	// Another runtime instance may have deployed the same pinned revision
	// first; the spec is a pure function of the revision, so the existing
	// deployment is the same deployment.
	if err != nil && !errors.Is(err, controlplane.ErrExists) {
		return "", fmt.Errorf("runtime: deploy %s: %w", name, err)
	}

	s.mu.Lock()
	s.deployed[key] = name
	s.mu.Unlock()
	return name, nil
}

// register adds an agent's tool to the legatus registry, once.
func (s *Sandbox) register(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.registered[name] {
		return nil
	}
	if err := s.tools.Register(tool.Func(name, s.call)); err != nil {
		// Registered by another Sandbox over the same registry: the tool body
		// is this package's either way, so adopt it rather than failing.
		s.logger.Debug("mandatum/runtime: legatus tool already registered", "tool", name, "error", err)
	}
	s.registered[name] = true
	return nil
}

// attach registers a turn someone is about to wait on, under the key its tool
// will look it up by.
func (s *Sandbox) attach(key string, t *turn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.turns[key]; exists {
		// Two attempts of one run publishing into two sinks would interleave
		// on the same topic. duraturo's fencing already stops a zombie; this
		// stops the in-process case it cannot see.
		return fmt.Errorf("%w: run %s is already executing in this process", ErrInvalid, key)
	}
	s.turns[key] = t
	return nil
}

// detach forgets a turn and aborts its task. For a task that has already
// reported its result the abort reaches nothing.
func (s *Sandbox) detach(key string) {
	s.mu.Lock()
	t := s.turns[key]
	delete(s.turns, key)
	s.mu.Unlock()
	if t != nil {
		close(t.abort)
	}
}

func (s *Sandbox) turn(key string) *turn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turns[key]
}

// call is the legatus tool: one agent turn, inside the sandbox.
//
// It runs on a node's goroutine, with a context that carries neither the run's
// replay frame nor the caller's cancellation, and with a workspace already scoped
// to the policy. Everything it needs travels in the task input.
func (s *Sandbox) call(ctx context.Context, rt tool.Runtime, input []byte) ([]byte, error) {
	var task taskInput
	if err := json.Unmarshal(input, &task); err != nil {
		return nil, fmt.Errorf("runtime: sandbox: decode request: %w", err)
	}
	req := task.Request
	rev := req.Revision
	if err := rev.Validate(); err != nil {
		return nil, err
	}

	// The turn this task was submitted for. A task whose turn is gone was
	// abandoned before a node picked it up, and running it would spend a model
	// call on an answer nobody will read. A request with no key at all was
	// submitted by something that does not wait this way, and runs unwatched.
	var waited *turn
	if key := firstNonEmpty(req.RunID, task.Sub); key != "" {
		if waited = s.turn(key); waited == nil {
			return nil, fmt.Errorf("runtime: sandbox: turn %s is no longer awaited", key)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		go func(abort <-chan struct{}) {
			select {
			case <-abort:
				cancel()
			case <-ctx.Done():
			}
		}(waited.abort)
	}

	// A delegated sub-task's sink is read by the delegation waiting on it,
	// which relays only the sub-task's usage onto the parent's stream: its
	// answer is the tool result its parent reads, and it is not a durable run
	// of its own, so it has no stream to publish onto.
	var sink chan agentrt.Chunk
	var stop <-chan agentrt.Stop
	if waited != nil {
		sink = waited.sink
		if waited.stop != nil {
			// The request passes through here on its way to the agent loop,
			// so the turn remembers it before anything it cancels can ask.
			forward := make(chan agentrt.Stop, 1)
			go func(in <-chan agentrt.Stop, t *turn) {
				select {
				case req := <-in:
					t.stopped.Store(&req)
					forward <- req
				case <-ctx.Done():
				}
			}(waited.stop, waited)
			stop = forward
		}
	}
	send := func(c agentrt.Chunk) error {
		if sink == nil {
			return nil
		}
		select {
		case sink <- c:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Every call this turn makes is keyed under one prefix, which a sub-task
	// it delegates to extends.
	req.Accounting = accountingOf(req)
	tools, granted, err := s.turnTools(rt, req, send)
	if err != nil {
		return nil, err
	}
	// The runner registers exactly the names the revision grants, so the
	// delegation tools have to be in that list. The revision here is a local
	// copy: the pinned definition itself is never edited.
	rev.Spec.Tools = granted

	opts := append(append([]agentrt.Option{}, s.agentOpts...), agentrt.WithTools(tools))
	if s.knowledge != nil {
		names := make([]string, 0, len(rev.Spec.Sources))
		for _, src := range rev.Spec.Sources {
			names = append(names, src.Name)
		}
		if r := s.knowledge(req.Tenant, names); r != nil {
			opts = append(opts, agentrt.WithRetriever(r))
		}
	}
	runner, err := agentrt.New(rev, opts...)
	if err != nil {
		return nil, err
	}

	res, err := runner.Run(ctx, agentrt.Turn{
		Text: req.Text, Meta: req.Meta, History: req.History, Instructions: req.Instructions,
		Attachments: req.Attachments, Tenant: req.Tenant,
		Stop: stop, Accounting: req.Accounting,
		Park: req.Park, QuestionTTL: req.QuestionTTL, Resume: req.Resume,
	}, func(_ context.Context, c agentrt.Chunk) error {
		return send(c)
	})
	if err != nil {
		return nil, err
	}

	out := Outcome{Text: res.Text, Prompt: res.Prompt, Chunks: res.Chunks, Citations: res.Citations, Stopped: res.Stopped, Parked: res.Parked}
	if out.Stopped != nil || out.Parked != nil {
		// Half an answer is not the run's answer artifact.
		return json.Marshal(out)
	}
	key := AnswerKey(rev.Spec.Name, req.RunID)
	if werr := rt.Workspace().Write(ctx, key, strings.NewReader(res.Text)); werr != nil {
		// The policy decided, and that is not a turn failure: an agent granted
		// no workspace area still answered. Reporting it keeps the decision
		// visible instead of pretending the artifact exists.
		out.StoreError = werr.Error()
	} else {
		out.Stored = key
	}
	return json.Marshal(out)
}

// turnTools assembles the tools one turn may call: the deployment's catalog for
// the names the spec grants, plus one delegation tool per agent in Access.Spawn.
//
// Delegation is granted by Access.Spawn alone rather than by also listing
// "tag_<agent>" in Spec.Tools. The allowlist is the grant; requiring the same
// fact in two places would let them disagree, and the failure mode of that
// disagreement is an agent that cannot delegate despite being allowed to, or the
// reverse.
// It returns the catalog and the effective list of granted tool names, in a
// stable order so that two turns of the same revision show the model the same
// tools in the same sequence.
func (s *Sandbox) turnTools(rt tool.Runtime, req Request, send func(agentrt.Chunk) error) (agentrt.Tools, []string, error) {
	report := func(a payload.Action) error {
		body, err := payload.Encode(a)
		if err != nil {
			return err
		}
		return send(agentrt.Chunk{Kind: envelope.KindActionTaken, Payload: body})
	}

	granted := make([]string, 0, len(req.Revision.Spec.Tools)+len(req.Revision.Spec.Access.Spawn))
	seen := make(map[string]bool, cap(granted))
	add := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		granted = append(granted, name)
	}
	for _, name := range req.Revision.Spec.Tools {
		add(name)
	}

	delegates := make(map[string]saigetypes.Tool, len(req.Revision.Spec.Access.Spawn))
	for _, target := range req.Revision.Spec.Access.Spawn {
		name := DelegateToolName(target)
		delegates[name] = s.delegateTool(rt, req, target, report, send)
		if s.callTool == "" {
			add(name)
		}
	}
	if s.callTool != "" && len(req.Revision.Spec.Access.Spawn) > 0 {
		add(s.callTool)
	}

	var base agentrt.Tools
	if s.catalog != nil {
		b, err := s.catalog(req, report)
		if err != nil {
			return nil, nil, err
		}
		base = b
	}

	return agentrt.ToolsFunc(func(name string) (saigetypes.Tool, error) {
		if t, ok := delegates[name]; ok {
			return t, nil
		}
		if base == nil {
			return nil, fmt.Errorf("%w: %q (no tool catalog configured)", agentrt.ErrNoSuchTool, name)
		}
		t, err := base.Lookup(name)
		if err != nil {
			return nil, err
		}
		if name == s.callTool {
			return &hostCallTool{Tool: t, sandbox: s, parent: req, send: send}, nil
		}
		return t, nil
	}), granted, nil
}

// delegateTool lets an agent tag another agent, gated by the spawn allowlist.
//
// The child is a sub-task of THIS turn, not a durable run: it is submitted into
// the same deployment, its answer comes back as this tool's result, and it
// therefore lands inside the parent activity's memoized record. That is why it
// resolves the target's current revision rather than pinning one — nothing
// replays a tool result, so there is nothing for an unpinned resolution to
// diverge from.
//
// The child is not given the parent's thread history, and that is a decision
// rather than an omission. A thread is one agent's conversation (see
// thread.KeyOf), and the child is a different agent: handing it the parent's
// journal would show it questions that were never put to it and answers it
// never gave, under its own role. What the child needs to know is what the
// parent chose to tell it, which is the text of the delegation. It has no run
// id either, so nothing it says is appended to any thread; its answer reaches
// the parent's journal only as part of whatever the parent finally answers.
//
// Stopping the parent stops the child, by cancellation: a stop cancels the
// tool calls the parent's turn has in flight, this one among them, and a
// sub-task nobody is waiting on is aborted (see the sub-task key below). The
// child is not wrapped up the way the parent is. What it had written is
// discarded with the tool call it was the result of, so there is no partial
// answer to keep.
//
// The other way to have an agent tag an agent is to raise a Tag at it, which
// creates a durable run of its own with its own pinned revision and its own
// topic. That is the right shape when the delegated work should outlive this
// turn; this one is the right shape when the parent needs the answer to continue.
//
// What the child spends is the parent's spending too. The child's model calls
// are relayed onto the parent's stream as usage.call events one level deeper,
// as they happen, so they are counted in the parent run's total (as Children)
// whether the child answers, fails or is aborted. A host Delegator reports the
// run it started with ReportChildUsage, from the context it is given, and the
// runtime reads that run's total from the ledger.
func (s *Sandbox) delegateTool(rt tool.Runtime, parent Request, target string, report func(payload.Action) error, send func(agentrt.Chunk) error) saigetypes.Tool {
	if parent.ChildRuns && parent.RunID != "" {
		return &childRunTool{s: s, parent: parent, target: target, send: send}
	}
	return &saigetypes.ToolFunc{
		Def: saigetypes.ToolDef{
			Name:        DelegateToolName(target),
			Description: fmt.Sprintf("Tag the %q agent with a request and wait for its answer.", target),
			Parameters: saigetypes.ParameterSchema{
				Type:     "object",
				Required: []string{"text"},
				Properties: map[string]saigetypes.PropertyDef{
					"text": {Type: "string", Description: "The request to send to " + target},
				},
			},
		},
		Fn: func(ctx context.Context, args map[string]any) (string, error) {
			text, _ := args["text"].(string)
			if strings.TrimSpace(text) == "" {
				return "", fmt.Errorf("%w: delegation to %q needs a text request", ErrInvalid, target)
			}
			// A host that gives delegated turns runs of their own takes it
			// from here. A sub-task has no run id, so it cannot be a parent
			// to one and falls through to delegating by sub-task.
			if s.delegator != nil && parent.RunID != "" {
				dctx, done := s.hostCallContext(ctx, parent, send)
				defer done()
				return s.delegator(dctx, parent, target, text, report)
			}
			if s.specs == nil {
				return "", fmt.Errorf("%w: this deployment cannot resolve agent %q", ErrInvalid, target)
			}
			rev, err := s.specs.Latest(ctx, parent.Tenant, target)
			if err != nil {
				return "", fmt.Errorf("runtime: delegate to %q: %w", target, err)
			}
			// The sub-task is cancelled when this call stops waiting on it,
			// for whatever reason: the parent was stopped, failed, or was
			// abandoned. legatus has no cancel of its own, so without this a
			// stopped parent would leave its helpers working on answers
			// nobody will read.
			sub := fmt.Sprintf("sub-%d", s.subs.Add(1))
			sink := make(chan agentrt.Chunk, s.buffer)
			if err := s.attach(sub, &turn{sink: sink, abort: make(chan struct{})}); err != nil {
				return "", err
			}
			defer s.detach(sub)
			// Deferred after detach, so it runs first: everything the
			// sub-task reported before it returned is relayed before the
			// delegation returns its answer.
			defer relay(sink, send)()
			child := Request{
				// No run id: a sub-task has no stream of its own, and its
				// output would be attributed to the wrong agent on the
				// parent's topic. It reaches the transcript as this tool's
				// result, which is where the model put it.
				Tenant:  parent.Tenant,
				Attempt: parent.Attempt,
				Origin:  parent.Origin,
				Source:  parent.Source,
				Text:    text,
				// No History: see above.
				Revision: rev,
				Service:  parent.Service,
				// The child's calls are keyed under the parent's, so no two
				// calls of one run share a key at any depth.
				Accounting: parent.Accounting + "/" + target + "." + sub,
				// The files the parent was asked about go with the work it
				// hands off; a specialist asked to read a log it was never
				// given can only guess.
				Attachments: attachment.Inherit(parent.Attachments, nil),
			}
			input, err := json.Marshal(taskInput{Request: child, Sub: sub})
			if err != nil {
				return "", fmt.Errorf("runtime: delegate to %q: %w", target, err)
			}
			id, err := rt.Spawn(ctx, task.Task{Tool: ToolName(target), Input: input})
			if err != nil {
				// A denial is the policy speaking. The model sees it as a
				// tool error and can say so, instead of the turn dying.
				return "", fmt.Errorf("runtime: delegate to %q: %w", target, err)
			}
			out, err := s.await(ctx, parent.Service, id)
			if err != nil {
				return "", err
			}
			return out.Text, nil
		},
	}
}

// relay forwards the usage a delegated sub-task reports on sink to its
// parent's stream, one level deeper, until the returned function is called;
// that function relays what is still buffered and waits for the relay to end.
// Everything else a sub-task streams is dropped: its answer reaches the parent
// as the delegation tool's result.
func relay(sink <-chan agentrt.Chunk, send func(agentrt.Chunk) error) func() {
	forward := func(c agentrt.Chunk) {
		if up, ok := relayUsage(c); ok {
			// A parent that has gone away cannot be told. The child's
			// calls are then lost with the parent's own unfinished ones.
			_ = send(up)
		}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case c := <-sink:
				forward(c)
			case <-stop:
				for {
					select {
					case c := <-sink:
						forward(c)
					default:
						return
					}
				}
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

// delegationContext is ctx with a cause a Delegator can read: when the
// parent's turn was stopped, the context ends with a *StoppedError, so a host
// can stop the run it started for the delegation; any other ending (a worker
// shutting down, a superseded attempt, a failed turn) keeps the cause it had.
func (s *Sandbox) delegationContext(ctx context.Context, runID string) (context.Context, func()) {
	dctx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	go func() {
		select {
		case <-ctx.Done():
			cause := context.Cause(ctx)
			if t := s.turn(runID); t != nil {
				if req := t.stopped.Load(); req != nil {
					cause = &StoppedError{Reason: req.Reason, By: req.By}
				}
			}
			cancel(cause)
		case <-dctx.Done():
		}
	}()
	return dctx, func() { cancel(nil) }
}

// await polls for a delegated sub-task's result.
func (s *Sandbox) await(ctx context.Context, deployment, taskID string) (Outcome, error) {
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return Outcome{}, ctx.Err()
		case <-ticker.C:
		}
		res, done, err := s.plane.Result(ctx, deployment, taskID)
		if err != nil {
			return Outcome{}, fmt.Errorf("runtime: await task %s: %w", taskID, err)
		}
		if !done {
			continue
		}
		if res.Error != "" {
			return Outcome{}, fmt.Errorf("runtime: task %s: %s", taskID, res.Error)
		}
		var out Outcome
		if err := json.Unmarshal(res.Output, &out); err != nil {
			return Outcome{}, fmt.Errorf("runtime: task %s: decode outcome: %w", taskID, err)
		}
		return out, nil
	}
}

// childRunTool is the delegation tool of a durable run whose delegations are
// child runs (WithChildRuns). The turn parks at a call to it (it is
// agentrt.Awaiting), the runtime starts the child, and the continuation runs
// the call with the child's ending as its answer.
type childRunTool struct {
	s      *Sandbox
	parent Request
	target string
	send   func(agentrt.Chunk) error
}

var _ agentrt.Awaiting = (*childRunTool)(nil)

func (t *childRunTool) Definition() saigetypes.ToolDef {
	return saigetypes.ToolDef{
		Name:        DelegateToolName(t.target),
		Description: fmt.Sprintf("Tag the %q agent with a request and wait for its answer.", t.target),
		Parameters: saigetypes.ParameterSchema{
			Type:     "object",
			Required: []string{"text"},
			Properties: map[string]saigetypes.PropertyDef{
				"text": {Type: "string", Description: "The request to send to " + t.target},
			},
		},
	}
}

// AwaitQuestion describes the wait a call parks on. A call with no request
// text is not a delegation: it runs, and tells the model what was wrong.
func (t *childRunTool) AwaitQuestion(call saigetypes.ToolCallPart) (payload.Question, bool) {
	text, _ := call.Arguments["text"].(string)
	if strings.TrimSpace(text) == "" {
		return payload.Question{}, false
	}
	args, _ := json.Marshal(call.Arguments)
	return payload.Question{
		Prompt: text,
		Tool:   &payload.QuestionTool{Name: call.Name, CallID: call.ID, Arguments: args},
	}, true
}

// Execute returns the child run's answer, and puts what the child spent on
// the parent's stream.
func (t *childRunTool) Execute(ctx context.Context, args map[string]any) (string, error) {
	if text, _ := args["text"].(string); strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("%w: delegation to %q needs a text request", ErrInvalid, t.target)
	}
	a, ok := agentrt.AnswerFor(ctx)
	if !ok {
		return "", fmt.Errorf("runtime: delegation to %q: the run it started has not answered", t.target)
	}
	if a.RunID != "" {
		if turn := t.s.turn(t.parent.RunID); turn != nil && turn.childUsage != nil {
			if c, err := turn.childUsage(ctx, a.RunID); err == nil {
				if body, err := payload.Encode(c); err == nil {
					_ = t.send(agentrt.Chunk{Kind: envelope.KindUsageChild, Payload: body})
				}
			}
		}
	}
	if a.Text == "" && a.Note != "" {
		return "", errors.New(a.Note)
	}
	return a.Text, nil
}

// hostCallContext gives a unified host tool the same stop propagation, child
// accounting and question forwarding as the built-in delegation tools.
func (s *Sandbox) hostCallContext(ctx context.Context, parent Request, send func(agentrt.Chunk) error) (context.Context, func()) {
	dctx, done := s.delegationContext(ctx, parent.RunID)
	dctx = context.WithValue(dctx, usageReporterKey{}, func(ctx context.Context, runID string) error {
		t := s.turn(parent.RunID)
		if t == nil || t.childUsage == nil {
			return fmt.Errorf("%w: this runtime cannot vouch for child runs", ErrNotAChild)
		}
		c, err := t.childUsage(ctx, runID)
		if err != nil {
			return err
		}
		body, err := payload.Encode(c)
		if err != nil {
			return err
		}
		return send(agentrt.Chunk{Kind: envelope.KindUsageChild, Payload: body})
	})
	dctx = context.WithValue(dctx, childQuestionKey{}, func(ctx context.Context, runID string) error {
		t := s.turn(parent.RunID)
		if t == nil || t.childQuestion == nil {
			return fmt.Errorf("%w: this runtime cannot vouch for child runs", ErrNotAChild)
		}
		return watchChild(ctx, t.childQuestion, runID, func(cq payload.ChildQuestion) error {
			body, err := payload.Encode(cq)
			if err != nil {
				return err
			}
			return send(agentrt.Chunk{Kind: envelope.KindChildParked, Payload: body})
		})
	})

	return dctx, done
}

type hostCallTool struct {
	saigetypes.Tool
	sandbox *Sandbox
	parent  Request
	send    func(agentrt.Chunk) error
}

func (t *hostCallTool) Execute(ctx context.Context, args map[string]any) (string, error) {
	ctx, done := t.sandbox.hostCallContext(ctx, t.parent, t.send)
	defer done()
	return t.Tool.Execute(ctx, args)
}
func (t *hostCallTool) AwaitQuestion(call saigetypes.ToolCallPart) (payload.Question, bool) {
	if awaiting, ok := t.Tool.(agentrt.Awaiting); ok {
		return awaiting.AwaitQuestion(call)
	}
	return payload.Question{}, false
}
