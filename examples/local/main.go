// Command local is fiber, end to end, in one process.
//
//	go run ./examples/local
//
// No API key. No network. No Postgres, no Redis, no Docker. Everything a
// deployment would run against real infrastructure runs here against the
// in-memory implementations of the same interfaces:
//
//	control plane   pkg/definition.Memory      (a real one would be Postgres)
//	bus             pkg/bus.Memory             (a real bus would be Redis Streams)
//	ledger + queue  anchor's ledger.Memory   (a real ledger would be Postgres)
//	                and queue.Memory
//	model           saige's agenttest.ScriptedProvider, reached through the
//	                agent's own "offline" provider: the agent loop, the typed
//	                deltas and the translation to events are all the real ones
//	connectors      two fakes in fakes.go, implementing connector.Subscriber exactly
//
// fiber.New assembles them, in process; the options it is given say
// which pieces this file replaces. None of those are stubs standing in for
// the design. They are the design,
// with the storage swapped. The seven sections below are the seven claims
// fiber makes, each one demonstrated rather than described.
//
//  1. an agent is a definition file (agents/*.agent.md)
//  2. a version is immutable; revising writes the next one
//  3. origin and destination are independent   ← the mesh thesis
//  4. one run, two sinks, two granularities    ← N:M selectivity
//  5. a third party can listen to an agent     ← "the Kafka of agents"
//  6. citations are first-class events with resolvable provenance
//  7. a replayed run does not re-execute a recorded activity ← durability
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/urmzd/anchor/pkg/worker"

	"github.com/urmzd/fiber"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/egress"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/runtime"
	ragtypes "github.com/urmzd/fiber/pkg/turn/native/ragtypes"
)

// tenant is the authorization scope. It comes from the caller's credential in
// a real deployment and is never on the wire and never a channel segment: a
// subscription names it beside its channels, never inside them.
const tenant = "acme"

// issue is the GitHub issue the demo request arrives on.
const issue = "github://acme/handbook/issues/42"

// author is who the control plane attributes these definitions to.
const author = "ada@acme.test"

// The agent, as two versions of one saige definition file and its policy.
var (
	//go:embed agents/docs-bot.v1.agent.md
	docsBotV1 string
	//go:embed agents/docs-bot.v2.agent.md
	docsBotV2 string
	//go:embed agents/docs-bot.policy.json
	docsBotPolicy string
)

// demoStride shrinks the per-attempt sequence band from its default of 2^20 so
// that the numbers in the output stay readable. Every attempt of a run gets its
// own band (attempt 1 opens at 101, attempt 2 at 201), which is what lets a
// retry append its real output after an interrupted attempt's partial output
// instead of vanishing behind the bus's high-water mark. Section 7 shows it.
const demoStride = 100

// quiet sends every library log line to /dev/null. The story here is told by
// fmt.Println, and interleaving it with operational logging would bury it.
// Section 7 deliberately induces a failure, so this also suppresses the very
// real error egress and the worker log about it.
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fixedClock stamps events so two runs of this example print the same thing.
var fixedClock = func() time.Time { return time.Date(2026, 7, 26, 9, 30, 0, 0, time.UTC) }

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "\nexample failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(quiet)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	banner()

	// ── composition ─────────────────────────────────────────────────
	//
	// This is the whole composition root. Each of these owns exactly one
	// concern, and the wiring below is the only place they meet.

	// The control plane: insert-only agent definitions, one row per version.
	store := definition.NewMemory(definition.WithClock(fixedClock))

	// The broker. Runs publish here; everything else subscribes. flakyBus
	// wraps it for section 7 and forwards Subscribe untouched, so egress
	// and the observer below read the real broker.
	broker := bus.NewMemory()
	published := newFlakyBus(broker)

	// The connectors. Roles are DERIVED from the interfaces a connector
	// implements, never declared: both of these implement Deliver and neither
	// implements Ingest, so the registry knows them as sinks only.
	surfaces := &transcript{}
	slack := newSlackSink(surfaces)
	hook := newWebhookSink(surfaces)

	// The agent side: a corpus to ground answers in, and a turn runtime that
	// builds the agent from a pinned revision and runs it.
	docs := corpus{blocks: []ragtypes.ContextBlock{
		{
			Text:     "Merging to main runs the release workflow: build, sign, upload, then open a draft release.",
			Citation: "[1]",
			Provenance: ragtypes.Provenance{
				DocumentUUID:   "doc-release-pipeline",
				DocumentTitle:  "Release pipeline",
				SourceURI:      "https://docs.acme.test/release-pipeline#on-merge",
				SectionUUID:    "sec-on-merge",
				SectionHeading: "On merge to main",
				SectionIndex:   2,
			},
		},
		{
			Text:     "Rollback is a re-run of the previous tag's workflow; never revert the release commit by hand.",
			Citation: "[2]",
			Provenance: ragtypes.Provenance{
				DocumentUUID:   "doc-runbook",
				DocumentTitle:  "Deploy runbook",
				SourceURI:      "https://docs.acme.test/runbook#rollback",
				SectionUUID:    "sec-rollback",
				SectionHeading: "Rollback",
				SectionIndex:   7,
			},
		},
	}}

	exec := newOfflineRuntime(docs)
	exec.script("docs-bot", streamed(
		"The release workflow runs on merge to main [1]. ",
		"It builds, signs and uploads the archive, then opens a draft release ",
		"for a human to publish. ",
		"To roll back, re-run the previous tag's workflow rather than reverting by hand [2].",
	))

	// fiber.New is the composition root: it wires egress (another bus
	// subscriber, which is why origin and destination stay independent),
	// the durable runtime on anchor's in-memory ledger and queue, and the
	// worker that pulls runs off the queue. Every piece this example
	// replaces is an option; everything else is the in-memory default.
	app, err := fiber.New(ctx,
		fiber.WithTenant(tenant),
		fiber.WithLogger(quiet),
		fiber.WithDefinitions(store),
		fiber.WithBus(published),
		fiber.WithTurnRuntime(exec),
		fiber.WithConnector(slack),
		fiber.WithConnector(hook),
		fiber.WithRuntimeOptions(
			runtime.WithStride(demoStride),
			runtime.WithClock(fixedClock),
			runtime.WithLogger(quiet),
		),
		// Egress drops events older than its max age; this demo's clock is
		// fixed, so egress reads the same clock.
		fiber.WithEgressOptions(egress.WithClock(fixedClock)),
		fiber.WithWorkerOptions(
			worker.WithJanitorEvery(0),
			worker.WithLeaseTTL(2*time.Second),
			worker.WithHeartbeatEvery(200*time.Millisecond),
			worker.WithBackoff(func(int) time.Duration { return 10 * time.Millisecond }),
		),
	)
	if err != nil {
		return fmt.Errorf("compose: %w", err)
	}
	defer func() { _ = app.Close() }()
	appDone := make(chan error, 1)
	go func() { appDone <- app.Run(ctx) }()

	// ── 1 ───────────────────────────────────────────────────────────
	section(1, "AN AGENT IS A DEFINITION FILE")

	// The definition is a saige .agent.md file; what it may touch beyond its
	// tools (here, the corpus it retrieves from) is its policy, which grants
	// nothing it does not name.
	var policy definition.Policy
	if err := json.Unmarshal([]byte(docsBotPolicy), &policy); err != nil {
		return fmt.Errorf("read policy: %w", err)
	}
	v1, err := store.Put(ctx, definition.Record{Tenant: tenant, Content: docsBotV1, Policy: policy, CreatedBy: author})
	if err != nil {
		return fmt.Errorf("create agent: %w", err)
	}
	fmt.Printf("  created  %s  version %s\n", v1.Name, v1.Version)
	fmt.Printf("  digest   %s\n", v1.Digest)
	fmt.Printf("  policy   %s\n", v1.PolicyHash)
	fmt.Printf("  name     %s          ← what a channel membership names to wake it\n", v1.Name)

	// ── 2 ───────────────────────────────────────────────────────────
	section(2, "A VERSION IS IMMUTABLE; REVISING WRITES THE NEXT ONE")

	// A version is never rewritten: storing other content under 1.0.0 is
	// refused, so an edit is a new, higher version.
	v2, err := store.Put(ctx, definition.Record{Tenant: tenant, Content: docsBotV2, Policy: policy, CreatedBy: author})
	if err != nil {
		return fmt.Errorf("revise agent: %w", err)
	}
	fmt.Printf("  revised  %s  version %s\n\n", v2.Name, v2.Version)

	// Both versions still resolve, and 1.0.0 is byte-for-byte what it was
	// before the edit. This is not a nicety: a run PINS its version at accept
	// time and replays under it forever, because anchor replays a workflow
	// from the top and a definition that moved between attempts would make the
	// replay diverge from its own ledger.
	got1, err := store.Get(ctx, tenant, "docs-bot", v1.Version)
	if err != nil {
		return fmt.Errorf("resolve version %s: %w", v1.Version, err)
	}
	got2, err := store.Get(ctx, tenant, "docs-bot", v2.Version)
	if err != nil {
		return fmt.Errorf("resolve version %s: %w", v2.Version, err)
	}
	fmt.Printf("  %s  %s\n", got1.Version, got1.Digest)
	fmt.Printf("  %s  %s\n\n", got2.Version, got2.Digest)

	if got1.Digest != v1.Digest {
		return fmt.Errorf("version %s changed under a revise: %s became %s", v1.Version, v1.Digest, got1.Digest)
	}
	fmt.Println("  1.0.0 still resolves, unchanged, after the edit that wrote 2.0.0.")
	fmt.Println("  a run in flight against 1.0.0 finishes as 1.0.0.")

	// ── 3 ───────────────────────────────────────────────────────────
	section(3, "ORIGIN AND DESTINATION ARE INDEPENDENT   ← the mesh thesis")

	// An event is the canonical request every trigger publishes. This one is
	// what the GitHub connector would publish for an issue comment that
	// mentions @docs-bot; ID is GitHub's delivery GUID, which becomes the
	// run's idempotency key, so a webhook redelivery JOINS the run it
	// duplicated instead of starting a second one.
	trigger := event.Envelope{
		ID:      "gh-72d3162e-cc78",
		Source:  issue,
		Type:    event.TypeGitHubIssueCommentCreated,
		Channel: issue,
		Time:    fixedClock(),
		Tenant:  tenant,
		Data: event.GitHubIssueCommentCreated{
			Request: event.Request{
				Text:     "how does a release actually get out the door?",
				Actor:    event.Actor{ID: "U_ada", Display: "Ada"},
				Mentions: []string{"docs-bot"},
			},
			GitHubRef: event.GitHubRef{Owner: "acme", Repo: "handbook", Number: 42, Sender: "ada"},
		},
	}

	fmt.Printf("  raised in   %s\n", trigger.Channel)
	fmt.Println("  answered in")
	for _, sub := range append(slack.Subscriptions(), hook.Subscriptions()...) {
		fmt.Printf("              %-42s  %s\n", sub.Target, strings.Join(sub.Types, ", "))
	}
	fmt.Println()
	fmt.Println("  the trigger is GitHub. neither destination is GitHub.")
	fmt.Println("  no component on this path holds both ends: the trigger publishes an")
	fmt.Println("  event, the run publishes events on the same channel, and each sink")
	fmt.Println("  subscribes to the channels it renders. that indirection is the")
	fmt.Println("  difference between a mesh and a pile of integrations.")

	// The third party attaches BEFORE anything is accepted, on channel
	// prefixes rather than one run. It will pick up this run and the next
	// one without ever being told either exists. Section 5 reads its log.
	watcher := &observer{}
	watching, err := watcher.watch(ctx, broker, bus.Subscription{
		Tenant:   tenant,
		Channels: []string{"github://", "cron://"},
		Types:    []string{"run"},
	})
	if err != nil {
		return fmt.Errorf("observer subscribe: %w", err)
	}

	// ── 4 ───────────────────────────────────────────────────────────
	section(4, "ONE RUN, TWO SINKS, TWO GRANULARITIES   ← N:M selectivity")

	fmt.Println("  slack   takes every run type and edits one message as the answer arrives")
	fmt.Println("  webhook takes run.completed only and posts exactly once")
	fmt.Println()

	// Accept pins the version, publishes the accepted event, and submits
	// the run durably. From here the process could be killed and restarted
	// and the run would still finish.
	meshRun := runtime.RunKey(tenant, trigger.ID, "docs-bot")
	acc, err := app.Runtime().Accept(ctx, trigger, "docs-bot")
	if err != nil {
		return fmt.Errorf("accept: %w", err)
	}
	fmt.Printf("  accepted run %s on channel %s, pinned to version %s\n\n",
		acc.RunID, acc.Channel, acc.Version)

	out, err := app.Runtime().Result(ctx, acc.RunID)
	if err != nil {
		return fmt.Errorf("run %s: %w", acc.RunID, err)
	}
	// The run is terminal in the ledger; delivery is a separate, asynchronous
	// subscription, so wait for the surfaces to catch up before reporting.
	if err := await(ctx, "slack to render the run", func() bool {
		_, done := slack.delivered(meshRun)
		return done
	}); err != nil {
		return err
	}
	if err := await(ctx, "the webhook to be posted", func() bool {
		return hook.delivered(meshRun) > 0
	}); err != nil {
		return err
	}

	// Both surfaces are settled, so the transcript can be printed in the
	// order the events caused it. See the comment on transcript for why the
	// sinks record rather than print.
	fmt.Println("  what the two surfaces were shown, by the sequence that caused it:")
	fmt.Println()
	surfaces.flush()

	slackCount, _ := slack.delivered(meshRun)
	fmt.Println()
	fmt.Printf("  the run published %d events, sequences 1..%d.\n", out.Events, out.LastSeq)
	fmt.Printf("  slack   was delivered %d of them (every run type)\n", slackCount)
	fmt.Printf("  webhook was delivered %d of them (%s)\n", hook.delivered(meshRun), kindList(hook.seenKinds()))
	fmt.Println()
	fmt.Println("  same run, same bus, same events. selectivity is a field on the")
	fmt.Println("  subscription, not a separate channel or run, which is why adding the")
	fmt.Println("  webhook cost nothing on the agent side.")

	// ── 5 ───────────────────────────────────────────────────────────
	section(5, `A THIRD PARTY CAN LISTEN TO AN AGENT   ← "the Kafka of agents"`)

	if err := await(ctx, "the observer to see the run finish", func() bool {
		return watcher.sawKind(meshRun, event.TypeCompleted)
	}); err != nil {
		return err
	}

	fmt.Println("  this subscriber holds one subscription to github:// and cron://.")
	fmt.Println("  it did not raise the event, no sink names it, and the run cannot")
	fmt.Println("  tell it is there. it sees the whole stream anyway:")
	fmt.Println()
	fmt.Printf("    %-5s  %-22s  %s\n", "seq", "type", "data")
	fmt.Printf("    %-5s  %-22s  %s\n", "─────", "──────────────────────", "───────")
	for _, e := range watcher.events(meshRun) {
		fmt.Printf("    %-5d  %-22s  %s\n", e.Seq, e.Type, summarize(e))
	}
	fmt.Println()
	fmt.Println("  seq 1 is accepted; 101 opens attempt 1's sequence band; 200 closes it.")
	fmt.Println("  Seq is also the resume cursor: hand the last one back as")
	fmt.Println("  Subscription.From and the stream continues with no gap and no repeat.")

	// ── 6 ───────────────────────────────────────────────────────────
	section(6, "CITATIONS ARE FIRST-CLASS EVENTS WITH RESOLVABLE PROVENANCE")

	cites := 0
	for _, e := range watcher.events(meshRun) {
		c, ok := e.Data.(event.Citation)
		if !ok {
			continue
		}
		cites++
		fmt.Printf("  %s  seq %d\n", c.Label, e.Seq)
		fmt.Printf("      text     %q\n", c.Text)
		fmt.Printf("      document %s\n", c.Source.DocumentTitle)
		fmt.Printf("      section  %s (index %d)\n", c.Source.SectionHeading, c.Source.SectionIndex)
		fmt.Printf("      resolve  %s\n", c.Source.SourceURI)
	}
	if cites == 0 {
		return errors.New("no citation events were published; the retriever was not consulted")
	}
	fmt.Println()
	fmt.Println("  a citation is a run.delta.citation.v1 event, on the same stream and")
	fmt.Println("  in the same order as the text, not an attachment on the final answer.")
	fmt.Println("  it is derived from what the turn RETRIEVED, not from what the model")
	fmt.Println("  claimed, so a reader can resolve the URI and check it.")

	// ── 7 ───────────────────────────────────────────────────────────
	section(7, "A REPLAYED RUN DOES NOT RE-EXECUTE A RECORDED ACTIVITY")

	// A second event for the same agent, this time from a schedule. cron
	// has no surface to answer into and no sink subscribes to its channel,
	// so it runs and publishes and delivers nowhere, which is legal, and is
	// the reverse asymmetry from the webhook, a sink that can never trigger.
	nightly := event.Envelope{
		ID:      "nightly-2026-07-26",
		Source:  "cron:" + tenant + "/nightly",
		Type:    event.TypeCronTick,
		Channel: "cron://" + tenant + "/nightly",
		Time:    fixedClock(),
		Tenant:  tenant,
		Data: event.CronTick{
			Request:    event.Request{Text: "summarise how a release gets out the door"},
			Schedule:   "nightly",
			Expression: "0 2 * * *",
			Occurrence: fixedClock(),
		},
	}
	cronRun := runtime.RunKey(tenant, nightly.ID, "docs-bot")

	// Arm one failure: the first attempt's run.completed will not reach
	// the bus. The runtime treats a failed publish as a failed turn (a run
	// whose events nobody can see has not done its job), so anchor will
	// replay the turn. Everything the turn recorded before that point is
	// already in the ledger, including the ENTIRE agent execution.
	published.failOnce(cronRun, event.TypeCompleted)

	before := exec.modelCalls()
	fmt.Printf("  model calls so far           %d\n", before)
	fmt.Println("  arming a broker outage on this run's run.completed event...")
	fmt.Println()

	if _, err := app.Runtime().Accept(ctx, nightly, "docs-bot"); err != nil {
		return fmt.Errorf("accept nightly: %w", err)
	}
	nightlyOut, err := app.Runtime().Result(ctx, cronRun)
	if err != nil {
		return fmt.Errorf("nightly run: %w", err)
	}
	if err := await(ctx, "the observer to see the nightly run finish", func() bool {
		return watcher.sawKind(cronRun, event.TypeCompleted)
	}); err != nil {
		return err
	}

	fmt.Println("  what the third party saw, on the same subscription as before:")
	fmt.Println()
	fmt.Printf("    %-5s  %-22s  %s\n", "seq", "type", "data")
	fmt.Printf("    %-5s  %-22s  %s\n", "─────", "──────────────────────", "───────")
	for _, e := range watcher.events(cronRun) {
		fmt.Printf("    %-5d  %-22s  %s\n", e.Seq, e.Type, summarize(e))
	}

	after := exec.modelCalls()
	fmt.Println()
	fmt.Printf("  attempts on this run         %d\n", nightlyOut.Attempt)
	fmt.Printf("  model calls after the replay %d   (was %d: the difference is %d)\n",
		after, before, after-before)
	fmt.Println()
	if after-before != 1 {
		return fmt.Errorf("the agent executed %d times across %d attempts; a recorded activity was re-executed",
			after-before, nightlyOut.Attempt)
	}
	fmt.Println("  read the stream above: attempt 1 published started (101) and six deltas")
	fmt.Println("  (102..107), then its completed event was lost and the turn failed.")
	fmt.Println("  attempt 2 re-executed the workflow body from the top and published")
	fmt.Println("  resumed (201) and completed (300), and NO deltas, because the agent")
	fmt.Println("  activity was already recorded and was served from the ledger.")
	fmt.Println()
	fmt.Println("  that is the durability claim in one line: the workflow replays, the")
	fmt.Println("  model is not paid twice, and the subscriber sees a resumed event rather")
	fmt.Println("  than a run that went silent. each attempt gets its own sequence band,")
	fmt.Println("  so attempt 2's output appends after attempt 1's instead of being")
	fmt.Println("  discarded as already-seen.")

	// ── shutdown ────────────────────────────────────────────────────
	section(0, "WHAT JUST HAPPENED")

	fmt.Println("  agents defined      1 (2 versions, both still resolvable)")
	fmt.Printf("  runs executed       %d (one from GitHub, one from a schedule)\n", len(watcher.runs()))
	fmt.Printf("  events on the bus   %d (%d + %d, counted by a subscriber that invoked nothing)\n",
		watcher.total(), len(watcher.events(meshRun)), len(watcher.events(cronRun)))
	fmt.Printf("  surfaces delivered  slack %d, webhook %d\n", slackCount, hook.delivered(meshRun))
	fmt.Printf("  model calls         %d\n", after)
	fmt.Println()
	fmt.Println("  pass WithPostgres(pool) and WithBus(bus.NewRedis(client)) instead of")
	fmt.Println("  the in-memory definitions, bus and turn runtime, swap the two fakes for")
	fmt.Println("  WithSlack and WithWebhooks, and this file does not otherwise change.")
	fmt.Println()

	cancel()
	<-watching
	if err := <-appDone; err != nil {
		return fmt.Errorf("app: %w", err)
	}
	return nil
}

// ── output helpers ──────────────────────────────────────────────────

const rule = "──────────────────────────────────────────────────────────────────────────"

func banner() {
	fmt.Println()
	fmt.Println("  fiber: wake an agent from any channel, run it durably, stream it to")
	fmt.Println("            everyone, deliver it anywhere.")
	fmt.Println()
	fmt.Println("  one process, no API key, no network, no infrastructure.")
}

// section prints a numbered header. Section 0 is the epilogue.
func section(n int, title string) {
	fmt.Printf("\n%s\n", rule)
	if n == 0 {
		fmt.Printf("  %s\n", title)
	} else {
		fmt.Printf("  %d. %s\n", n, title)
	}
	fmt.Printf("%s\n\n", rule)
}

// kindList renders the kinds a sink was handed, so "the webhook never saw a
// delta" is a printed fact rather than an assertion.
func kindList(kinds map[string]int) string {
	if len(kinds) == 0 {
		return "nothing"
	}
	parts := make([]string, 0, len(kinds))
	for k, n := range kinds {
		parts = append(parts, fmt.Sprintf("%s x%d", k, n))
	}
	slices.Sort(parts)
	return strings.Join(parts, ", ")
}

// await polls cond until it holds. Delivery and observation are asynchronous
// subscriptions, not return values, so a caller that wants to print what a sink
// received has to wait for it. Polling rather than a channel per surface keeps
// the wiring in this file about the architecture rather than about
// synchronisation.
func await(ctx context.Context, what string, cond func() bool) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if cond() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
