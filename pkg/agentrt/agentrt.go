// Package agentrt builds a running agent from a pinned agent revision.
//
// It is the seam between two vocabularies. saige speaks providers, messages,
// tools, and a stream of typed deltas; mandatum speaks revisions, kinds, and
// events. agentrt translates in exactly one direction — a Revision in, event
// bodies out — and knows nothing about how the revision was stored, which run
// it is executing, or where its events end up. That ignorance is the point: the
// same translation serves a durable run under pkg/runtime, a one-shot CLI
// invocation, and a test with a scripted provider, and none of them can leak
// their concerns into the others.
//
// # What it produces
//
// A turn's output is a sequence of Chunk values: a Kind and a marshalled body
// (see pkg/agentrt/payload). Chunks are not events, because an event needs run
// identity — topic, sequence, tenant, pinned revision — and this package cannot
// know any of it. pkg/runtime stamps that on.
//
// # Citations
//
// Citations are derived from retrieval (see Retriever and Citations), which
// makes them honest: they report the passages the turn was given, not the ones
// a model claims to have used. saige's CitationDelta, which reports what a
// model or tool cited, is not translated.
//
// # Context layout
//
// A provider's prompt cache reuses a request only up to the first byte that
// differs from an earlier one, so the request is laid out from what never
// changes to what always does:
//
//	tool definitions, sorted by name      identical for every run of a revision
//	system prompt, from the pinned spec   identical for every run of a revision
//	the thread's history, oldest first    identical for every run of a thread,
//	                                      and only ever appended to
//	the request: trigger metadata,        fixed for the whole turn
//	  retrieved passages, the text
//	the tool loop's history               only ever appended to
//
// Everything that varies from one run to the next (metadata, retrieval
// results) lives in the request message, never in the system prompt, so it
// cannot sit ahead of content that would otherwise be shared. Inside a turn
// each model call sends the previous call's request plus what was added since.
// The single exception is a compaction, which is off unless a budget is set;
// see WithCompaction for what it rewrites and why it is rare, and
// WithPromptCache for where the cache breakpoints go.
//
// # History across runs
//
// A turn may carry the conversation so far in Turn.History. It is replayed
// into the request in the order given, one message per entry, after the
// system prompt and before the new request, and nothing is added to it: a
// Message is a role and a text, so there is no timestamp or run id that could
// reach the model and differ between two runs.
//
// A history message with attachments is rendered as its text followed by its
// files, resolved the way the request's files are. Resolution is a function of
// the references, the stored bytes and the pinned model, and one budget is
// spent across the history's files in order and then the request's, so a
// message renders the same bytes as history that it rendered as the request:
// the files of run k's request are spent from the same remaining budget in
// run k and in every later run that replays it. A long thread can therefore
// use up the per-run file budget, after which further files are notes; that
// is the price of a prefix that never changes.
//
// That gives a thread the same property a tool loop has. If the caller keeps
// history append-only and records each exchange as it happened (Result.Prompt
// is the request message exactly as it was sent, Result.Text is the answer),
// then the first request of run k is a byte prefix of the first request of
// run k+1, and a prefix cache reads the whole earlier conversation. The
// exception is the same one as inside a turn: a compaction rewrites the
// middle of the history and keeps the tools, the system prompt, and the
// oldest message.
//
// This package does not store history, and does not know what a thread is.
// With no History a turn starts a fresh conversation, and the prefix two runs
// of the same revision share is the tool definitions and the system prompt.
//
// # Stopping a turn
//
// A turn can be asked to stop while it runs (Turn.Stop). The model call in
// flight is cancelled, running tools are cancelled through their context, and
// the turn returns what it had answered so far with Result.Stopped set and no
// error: a stopped turn is neither a completed one nor a failed one, and a
// caller that retries failures must not retry it. This package does not decide
// what a stopped turn means for a run or a thread; pkg/runtime does.
//
// # Revisions are pinned, not resolved
//
// New takes a Revision, never an agent name. Nothing in this package can look
// up "the current definition of docs-bot", because a durable run replays from
// the top and a definition that moved between attempts would make the replay
// diverge from its own ledger. Pinning is pkg/runtime's job; refusing to
// un-pin is this package's.
package agentrt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	saige "github.com/urmzd/saige/agent"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/mandatum/pkg/agentrt/payload"

	"github.com/urmzd/mandatum/pkg/attachment"
	"github.com/urmzd/mandatum/pkg/attachment/resolve"
)

// DefaultMaxIter bounds the agent loop's tool-calling iterations per turn. It
// matches saige's own default and is exposed so a deployment can lower it: an
// agent that loops is a cost incident, and the run's retry budget will not stop
// it because a looping turn never fails.
const DefaultMaxIter = 10

// Role says who a history message is from.
type Role string

const (
	// RoleUser is a message said to the agent.
	RoleUser Role = "user"
	// RoleAssistant is an answer the agent gave.
	RoleAssistant Role = "assistant"
)

// Message is one earlier message of the conversation a turn continues.
//
// It is deliberately only a role, a text and the files that came with it.
// Whatever is rendered for the model has to be identical on every run that
// replays it, and the simplest way to guarantee that a timestamp, a run id or
// a sequence number never reaches the request is for the type to have nowhere
// to put one. Attachments are content references, so they render the same
// bytes every time: see "History across runs".
type Message struct {
	Role Role   `json:"role"`
	Text string `json:"text"`
	// Attachments are the files a user message arrived with. They are
	// rendered after the message's text, exactly as they were when that
	// message was the request. An assistant message carries none.
	Attachments []attachment.Attachment `json:"attachments,omitempty"`
}

// DefaultStopReason is the reason a stop given none is reported with, on
// every path: saige's own default, so a turn that saige wraps up and a turn
// stopped before it began say the same thing.
const DefaultStopReason = saige.DefaultStopReason

// RequestText is the request message a turn of text and meta sends before any
// retrieved passages are added: the trigger metadata, in sorted key order,
// then the text. It is what a turn that was stopped before it began records as
// its request, and what a caller can record when it never learned the prompt.
func RequestText(text string, meta map[string]string) string {
	if m := renderMeta(meta); m != "" {
		return m + "\n\n" + text
	}
	return text
}

// Stop asks a turn to wrap up before it has finished.
type Stop struct {
	// Reason is why, in the words of whoever asked. Empty takes saige's
	// default.
	Reason string `json:"reason,omitempty"`
	// By is who asked. It is recorded and never sent to the model.
	By string `json:"by,omitempty"`
}

// Turn is one request to an agent.
type Turn struct {
	// Stop, when set, is how the turn is asked to wrap up early: the first
	// value received on it stops the turn (see "Stopping a turn" in the
	// package documentation). A nil channel is a turn that cannot be stopped,
	// only cancelled through its context. Nothing is read from it once the
	// turn has returned.
	Stop <-chan Stop
	// Attachments are the files that arrived with the request. They need
	// WithAttachments, and Tenant, which scopes every read of them.
	Attachments []attachment.Attachment
	// Tenant is the authorization scope the turn runs under. It is only read
	// to resolve attachments.
	Tenant string
	// History is the conversation so far, oldest first. It is replayed
	// verbatim ahead of the request. The caller owns its order and its
	// stability: see "History across runs" in the package documentation.
	History []Message
	// Text is the request, with the agent mention already stripped.
	Text string
	// Meta is trigger-specific context (a pull request number, an issue
	// title). It is rendered into the prompt in sorted key order so the same
	// tag produces the same prompt on every attempt.
	Meta map[string]string
}

// Result is what a turn produced.
type Result struct {
	// Text is the agent's final answer, the concatenation of its text
	// deltas.
	Text string
	// Prompt is the request message exactly as it was sent to the model:
	// trigger metadata, retrieved passages, and the turn's text. A caller
	// that keeps history records this, not Turn.Text, as what the user said,
	// because a later run only shares a request prefix with this one if it
	// replays the same bytes.
	Prompt string
	// Chunks counts the event bodies the turn emitted, citations included.
	Chunks int
	// Citations counts the retrieved passages the turn was grounded in.
	Citations int
	// Usage is the tokens the turn spent across all its model calls, with
	// the share read from and written to the prompt cache. It is reported
	// for a failed turn too, since the tokens were spent either way.
	Usage payload.Usage
	// Compactions lists each time the turn's history was summarized, in
	// order. It is empty unless WithCompaction set a budget and the turn
	// crossed it.
	Compactions []payload.Compaction
	// Stopped is set when the turn ended because it was asked to stop, and
	// says why and by whom. Text is then what had been answered so far, which
	// may be nothing. It is nil for a turn that finished before the request
	// took effect: that turn completed, and its Text is the whole answer.
	Stopped *Stop
}

// Runner runs turns of one pinned revision.
//
// Construction validates everything a turn needs — the provider exists and is
// configured, every tool the spec names is resolvable — so a run fails at accept
// time with a clear reason instead of halfway through a Slack thread. A Runner
// is safe for concurrent use only insofar as its provider and tools are; each
// turn gets its own conversation tree.
type Runner struct {
	rev       Revision
	provider  saigetypes.Provider
	tools     *saigetypes.ToolRegistry
	retriever Retriever
	files     *resolve.Resolver
	maxIter   int
	logger    *slog.Logger
	// compaction is the history budget; its zero value never compacts.
	compaction Compaction
}

// Option configures a Runner.
type Option func(*config)

type config struct {
	providers map[string]ProviderFunc
	script    [][]saigetypes.Delta
	tools     Tools
	retriever Retriever
	maxIter   int
	logger    *slog.Logger
	// compaction and promptCache are the context policy; see context.go.
	compaction  Compaction
	promptCache string
	files       *resolve.Resolver
}

// WithAttachments lets turns carry files. The resolver reads each attachment
// from the blob store and turns it into what the pinned model can take: the
// bytes themselves where the model has that modality, derived text where it
// does not, and a note saying a file was attached where neither is possible.
//
// Without it a turn that carries attachments is refused. Running it anyway
// would answer a question about a file as though no file had been sent, and a
// confident answer to the wrong question is worse than an error.
func WithAttachments(r *resolve.Resolver) Option {
	return func(c *config) { c.files = r }
}

// WithProvider registers or replaces a provider factory under name. A
// deployment adds its own backends this way; it also overrides a built-in, which
// is how a test pins "anthropic" to a fake without an API key.
func WithProvider(name string, fn ProviderFunc) Option {
	return func(c *config) {
		if name == "" || fn == nil {
			return
		}
		c.providers[name] = fn
	}
}

// WithScript sets the delta sequences the offline provider replays, one per
// model call. It is the zero-infrastructure path: no key, no daemon, no
// network, and the whole tag-to-event path still runs.
func WithScript(script ...[]saigetypes.Delta) Option {
	return func(c *config) { c.script = script }
}

// WithTools sets the catalog the spec's tool names resolve against. Without
// one, a spec that names any tool fails to build: silently running an agent
// without the tools it was defined with would answer the wrong question.
func WithTools(t Tools) Option {
	return func(c *config) { c.tools = t }
}

// WithRetriever grounds turns in a corpus. Without one, a spec's Sources are
// inert and the agent answers from the model alone.
func WithRetriever(r Retriever) Option {
	return func(c *config) { c.retriever = r }
}

// WithMaxIter bounds tool-calling iterations per turn (default DefaultMaxIter).
func WithMaxIter(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxIter = n
		}
	}
}

// WithLogger sets the logger. Default is slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// New builds a runner for one pinned revision.
func New(rev Revision, opts ...Option) (*Runner, error) {
	if err := rev.Validate(); err != nil {
		return nil, err
	}
	cfg := config{
		providers: defaultProviders(),
		maxIter:   DefaultMaxIter,
		logger:    slog.Default(),
		// Caching is on by default because it is free to be wrong about: a
		// prefix nobody reads again costs a quarter more to write once, and
		// a tool loop of three calls has already earned that back.
		promptCache: PromptCacheShort,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if err := validPromptCache(cfg.promptCache); err != nil {
		return nil, err
	}
	// The offline and Anthropic providers are resolved last because they
	// need what an option may have supplied: the script, the cache lifetime.
	// An explicit WithProvider for the same name wins: overriding a built-in
	// is the documented escape hatch.
	if cfg.providers[ProviderOffline] == nil {
		cfg.providers[ProviderOffline] = offlineProvider(cfg.script)
	}
	if cfg.providers[ProviderAnthropic] == nil {
		cfg.providers[ProviderAnthropic] = anthropicProvider(cfg.promptCache)
	}

	newProvider, ok := cfg.providers[rev.Spec.Provider]
	if !ok || newProvider == nil {
		return nil, fmt.Errorf("%w: agent %q revision %d names provider %q; this deployment has %v",
			ErrInvalid, rev.Spec.Name, rev.Rev, rev.Spec.Provider, providerNames(cfg.providers))
	}
	provider, err := newProvider(rev.Spec)
	if err != nil {
		return nil, err
	}

	// The registry hands tool definitions to the provider sorted by name, so
	// the order a spec lists its tools in never reaches the request. That
	// matters beyond tidiness: tool definitions are the first bytes of the
	// cached prefix, and an order that followed the spec would invalidate
	// the cache for an edit that only reordered a list.
	tools := saigetypes.NewToolRegistry()
	for _, name := range rev.Spec.Tools {
		if cfg.tools == nil {
			return nil, fmt.Errorf("%w: agent %q revision %d grants tool %q but no tool catalog is configured",
				ErrInvalid, rev.Spec.Name, rev.Rev, name)
		}
		t, err := cfg.tools.Lookup(name)
		if err != nil {
			return nil, fmt.Errorf("agentrt: agent %q revision %d: %w", rev.Spec.Name, rev.Rev, err)
		}
		tools.Register(t)
	}

	return &Runner{
		rev:        rev,
		provider:   provider,
		tools:      tools,
		retriever:  cfg.retriever,
		files:      cfg.files,
		maxIter:    cfg.maxIter,
		logger:     cfg.logger,
		compaction: cfg.compaction,
	}, nil
}

// Revision returns the pinned revision this runner executes. Callers assert on
// it: a run that pinned revision 6 must still report 6 after the agent has been
// revised to 7.
func (r *Runner) Revision() Revision { return r.rev }

// SystemPrompt returns the prompt the pinned revision runs with. It exists so a
// caller can prove which definition executed, which a run id and a revision
// number alone cannot show.
func (r *Runner) SystemPrompt() string { return r.rev.Spec.SystemPrompt }

// Run executes one turn, handing every event body to emit as it is produced.
//
// emit is called on Run's goroutine, in order, and an error from it aborts the
// turn: the caller publishing chunks is the caller that cannot keep up or has
// gone away, and continuing to burn model tokens for a stream nobody is reading
// is worse than stopping.
//
// A failed turn returns an error and whatever text was produced before it
// failed, because a partially-answered turn is what a sink has already rendered
// and pretending otherwise would make the thread inconsistent with the run.
//
// A stopped turn returns no error: see Turn.Stop and Result.Stopped.
func (r *Runner) Run(ctx context.Context, t Turn, emit func(context.Context, Chunk) error) (Result, error) {
	var res Result

	// A stop that was already waiting when the turn began ends it here,
	// before retrieval and before any model call. The request is recorded as
	// it would have been sent, without retrieved passages, since none were
	// fetched.
	if stop, ok := pendingStop(t.Stop); ok {
		res.Prompt = RequestText(t.Text, t.Meta)
		res.Stopped = stopOf(stop)
		return res, nil
	}

	send := func(c Chunk) error {
		if emit == nil {
			res.Chunks++
			return nil
		}
		if err := emit(ctx, c); err != nil {
			return err
		}
		res.Chunks++
		return nil
	}

	prompt := t.Text
	if r.retriever != nil && len(r.rev.Spec.Sources) > 0 {
		ac, err := r.retriever.Retrieve(ctx, t.Text)
		if err != nil {
			return res, fmt.Errorf("agentrt: agent %q revision %d: retrieve: %w", r.rev.Spec.Name, r.rev.Rev, err)
		}
		cites := Citations(ac)
		for _, c := range cites {
			if err := send(c); err != nil {
				return res, err
			}
		}
		res.Citations = len(cites)
		if ac != nil && ac.Prompt != "" {
			prompt = ac.Prompt + "\n\n" + prompt
		}
	}
	if meta := renderMeta(t.Meta); meta != "" {
		prompt = meta + "\n\n" + prompt
	}
	res.Prompt = prompt

	// Files ride on the message they arrived with: a history message's after
	// its text, and the request's on the last message. They are the most
	// volatile thing a turn sends, so they go after everything a later turn
	// could share a cached prefix with.
	input, err := r.conversation(ctx, t, prompt)
	if err != nil {
		return res, fmt.Errorf("agentrt: agent %q revision %d: %w", r.rev.Spec.Name, r.rev.Rev, err)
	}

	agentCfg := saige.Config{
		Name:         r.rev.Spec.Name,
		SystemPrompt: r.rev.Spec.SystemPrompt,
		Provider:     r.provider,
		Tools:        r.tools,
		MaxIter:      r.maxIter,
		Logger:       r.logger,
	}
	if cc := r.compaction.config(); cc != nil {
		agentCfg.CompactCfg = cc
		agentCfg.Hooks = []saige.Hooks{newCompactionGuard(r.compaction.keepTurns()).hooks()}
	}
	agent, err := saige.New(agentCfg)
	if err != nil {
		return res, fmt.Errorf("agentrt: agent %q revision %d: %w", r.rev.Spec.Name, r.rev.Rev, err)
	}

	stream := agent.Invoke(ctx, input)
	translator := NewTranslator()

	// The stop request is applied from its own goroutine, because this one is
	// busy draining the stream and saige's Stop is safe to call from anywhere.
	// The goroutine ends with the turn: settle runs on every return path
	// below, which is what makes "nothing is read from Turn.Stop once the
	// turn has returned" true rather than likely.
	stopping := newStopper(stream)
	finished := make(chan struct{})
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		select {
		case stop := <-t.Stop:
			stopping.apply(stop)
		case <-finished:
		}
	}()
	// settle is deferred as well as called: a panic anywhere below (an emit
	// that panics, say) must not leave the watcher waiting forever.
	var settled sync.Once
	settle := func() {
		settled.Do(func() {
			close(finished)
			<-watched
		})
	}
	defer settle()

	var answer strings.Builder
	var streamErr error
	for d := range stream.Deltas() {
		switch v := d.(type) {
		case saigetypes.PartDelta:
			answer.WriteString(v.Text)
		case saigetypes.ErrorDelta:
			// Failure is a lifecycle fact, not an event of its own (see
			// Translator.Delta). Keep the first one and let the stream
			// drain: abandoning the channel would leak the producer.
			if streamErr == nil && v.Error != nil {
				streamErr = v.Error
			}
		case saigetypes.StoppedDelta:
			// The turn wrapped up on request. Like failure it is a
			// lifecycle fact and not an event of its own; the request it
			// answers is read off the stream once it has drained.
		case saigetypes.UsageDelta:
			// Telemetry, so it goes on the Result and not on the bus (see
			// Translator.Delta).
			res.Usage = addUsage(res.Usage, v)
		case saigetypes.CompactionDelta:
			res.Compactions = append(res.Compactions, compactionRecord(v))
		case saigetypes.MarkerDelta:
			// A tool asked for human approval mid-turn. Nothing in a durable
			// turn can answer that question — the caller is a queue, not a
			// person — and leaving it unresolved would block the stream
			// forever, so it is refused explicitly. The right shape for
			// approval is to park the run on an external event; see
			// pkg/runtime.
			//
			// A refusal that cannot be delivered would leave the tool
			// waiting, so it fails the turn and stops the stream instead.
			err := stream.ResolveMarkerErr(v.ToolCallID, saige.Resolution{
				Message: "mandatum: interactive approval is not available inside a durable run",
			})
			if err != nil {
				streamErr = errors.Join(streamErr, fmt.Errorf("refuse approval for tool call %s: %w", v.ToolCallID, err))
				stream.Cancel()
			}
		}
		c, ok := translator.Delta(d)
		if !ok {
			continue
		}
		if err := send(c); err != nil {
			stream.Cancel()
			drain(stream)
			settle()
			return res, err
		}
	}

	res.Text = answer.String()
	waitErr := stream.Wait()
	settle()
	if req, ok := stream.Stopped(); ok {
		// saige wrapped the turn up: the partial text is kept and the stream
		// ended cleanly.
		res.Stopped = &Stop{Reason: req.Reason, By: req.By}
		return res, nil
	}
	if stop, ok := stopping.cancelled(); ok {
		// The stream could not wrap up on request and was cancelled instead.
		// What the caller asked for still happened, with whatever text had
		// arrived, so it is reported as a stop and not as the cancellation
		// it was implemented with.
		res.Stopped = stopOf(stop)
		return res, nil
	}
	if waitErr != nil {
		streamErr = errors.Join(streamErr, waitErr)
	}
	if streamErr != nil {
		return res, fmt.Errorf("agentrt: agent %q revision %d: %w", r.rev.Spec.Name, r.rev.Rev, streamErr)
	}
	return res, nil
}

// conversation builds the turn's input: the history, then the request, with
// every message's attachments resolved for the model this revision pins.
//
// One budget is spent across every file in the order the messages are sent,
// history first. That is what makes a message render the same bytes in
// every run that replays it (see "History across runs").
func (r *Runner) conversation(ctx context.Context, t Turn, prompt string) ([]saigetypes.Message, error) {
	input, err := conversation(t.History, prompt)
	if err != nil {
		return nil, err
	}
	var budget *resolve.Budget
	for i, m := range t.History {
		if len(m.Attachments) == 0 {
			continue
		}
		if budget == nil {
			budget = resolve.NewBudget()
		}
		msg, err := r.withFiles(ctx, t.Tenant, m.Attachments, m.Text, budget)
		if err != nil {
			return nil, fmt.Errorf("history message %d of %d: %w", i+1, len(t.History), err)
		}
		input[i] = msg
	}
	if len(t.Attachments) > 0 {
		if budget == nil {
			budget = resolve.NewBudget()
		}
		msg, err := r.withFiles(ctx, t.Tenant, t.Attachments, prompt, budget)
		if err != nil {
			return nil, err
		}
		input[len(input)-1] = msg
	}
	return input, nil
}

// withFiles builds a user message of text followed by its attachments,
// resolved for the model this revision pins. A provider that does not report
// what it accepts is treated as text only: showing a model bytes it cannot
// read fails the call, while derived text always works.
func (r *Runner) withFiles(ctx context.Context, tenant string, files []attachment.Attachment, text string, budget *resolve.Budget) (saigetypes.Message, error) {
	if r.files == nil {
		return nil, fmt.Errorf("%w: the turn carries %d attachments and this deployment has no attachment resolver", ErrInvalid, len(files))
	}
	var media saigetypes.ContentSupport
	if caps, ok := saigetypes.ProviderCapabilities(r.provider); ok {
		media = caps.Media
	}
	out, err := r.files.Resolve(ctx, resolve.Request{Tenant: tenant, Attachments: files, Model: media, Budget: budget})
	if err != nil {
		return nil, fmt.Errorf("attachments: %w", err)
	}
	return out.Message(text), nil
}

// stopOf normalizes a stop the way saige does, so every path reports the same
// reason for a request that gave none.
func stopOf(s Stop) *Stop {
	s.Reason = strings.TrimSpace(s.Reason)
	if s.Reason == "" {
		s.Reason = DefaultStopReason
	}
	return &s
}

// pendingStop reports a stop request that is already waiting, without
// blocking.
func pendingStop(ch <-chan Stop) (Stop, bool) {
	select {
	case stop := <-ch:
		return stop, true
	default:
		return Stop{}, false
	}
}

// stopper applies one stop request to a stream and remembers whether it had to
// fall back to cancelling it.
type stopper struct {
	stream *saige.EventStream

	mu       sync.Mutex
	fallback *Stop
}

func newStopper(stream *saige.EventStream) *stopper { return &stopper{stream: stream} }

// apply asks the stream to wrap up. A stream that cannot (saige refuses it for
// a replayed or remote stream and under a durable step runner) is cancelled,
// which ends it as surely and keeps less.
//
// A stream that has already ended is left alone: the turn finished before the
// request arrived and its outcome stands.
func (s *stopper) apply(stop Stop) {
	err := s.stream.Stop(saige.StopRequest{Reason: stop.Reason, By: stop.By})
	if err == nil || errors.Is(err, saige.ErrRunFinished) {
		return
	}
	s.mu.Lock()
	s.fallback = &stop
	s.mu.Unlock()
	s.stream.Cancel()
}

// cancelled reports the stop that was carried out by cancelling the stream.
func (s *stopper) cancelled() (Stop, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fallback == nil {
		return Stop{}, false
	}
	return *s.fallback, true
}

// drain empties a cancelled stream so its producing goroutine can exit.
func drain(s *saige.EventStream) {
	for range s.Deltas() {
	}
}

// conversation lays out what a turn sends after the system prompt: the
// history, then the request.
//
// Each history entry becomes exactly one message, in the order given, and
// entries are never merged, reordered or decorated. Merging two consecutive
// messages of one role would look tidier and would break the property this
// exists for: the message a run sent last would change shape once a later
// message joined it, and the next run's request would stop being an extension
// of this one.
//
// An entry this package cannot render is refused rather than skipped. A
// history with a hole in it is a different conversation, and an agent that
// answers a different conversation is worse than a turn that fails.
func conversation(history []Message, prompt string) ([]saigetypes.Message, error) {
	out := make([]saigetypes.Message, 0, len(history)+1)
	for i, m := range history {
		if strings.TrimSpace(m.Text) == "" && (m.Role != RoleUser || len(m.Attachments) == 0) {
			return nil, fmt.Errorf("%w: history message %d of %d has no text", ErrInvalid, i+1, len(history))
		}
		switch m.Role {
		case RoleUser:
			out = append(out, saigetypes.UserMsg(saigetypes.Text(m.Text)))
		case RoleAssistant:
			if len(m.Attachments) > 0 {
				return nil, fmt.Errorf("%w: history message %d of %d is the agent's answer and carries attachments", ErrInvalid, i+1, len(history))
			}
			out = append(out, saigetypes.AssistantMsg(saigetypes.Text(m.Text)))
		default:
			return nil, fmt.Errorf("%w: history message %d of %d has role %q, want %q or %q",
				ErrInvalid, i+1, len(history), m.Role, RoleUser, RoleAssistant)
		}
	}
	return append(out, saigetypes.UserMsg(saigetypes.Text(prompt))), nil
}

// renderMeta formats a tag's metadata as a context block, in sorted key order.
//
// Determinism matters more than prettiness here: the rendered prompt is part of
// the input a durable turn re-executes with, and Go's map iteration order is
// random, so an unsorted rendering would make two attempts of the same tag ask
// the model two different questions.
func renderMeta(meta map[string]string) string {
	if len(meta) == 0 {
		return ""
	}
	keys := make([]string, 0, len(meta))
	for k := range meta {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("Context from the trigger:")
	for _, k := range keys {
		fmt.Fprintf(&b, "\n- %s: %s", k, meta[k])
	}
	return b.String()
}

func providerNames(m map[string]ProviderFunc) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		if v != nil {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

var _ Tools = ToolsFunc(nil)
