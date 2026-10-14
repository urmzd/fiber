# Architecture

> **v0 alpha, pre-1.0.** This document describes the architecture as built and
> tested. Interfaces may change between minor versions. Where something is
> designed but not yet finished, it is called out in
> [Limitations](#limitations) rather than described as though it exists.

mandatum is an agent platform and a low-latency pub/sub bus. Create an agent from
a spec, tag it from anywhere, get its events streamed to anyone and delivered
anywhere.

The single idea the whole design turns on: **where a tag is raised and where its
answer lands are independent.** A tag raised in GitHub can answer into Jira. A
schedule nobody watched can answer into both. That independence is what makes
this a mesh rather than a collection of integrations, and almost every decision
below follows from it.

## What each layer owns

mandatum composes three libraries and adds the parts that make them a product.

| Concern | Owner |
|---------|-------|
| The agent loop, typed deltas, RAG and citations | [saige](https://github.com/urmzd/saige) |
| Durability, replay, exactly-once effects, per-run journal | [duraturo](https://github.com/urmzd/duraturo) |
| Sandboxed execution nodes, NGAC access control | [legatus](https://github.com/urmzd/legatus) |
| Agent specs, revisions, registry | mandatum |
| Topics, the bus, delivery routing, transports | mandatum |
| Connectors: triggers, sinks, and native actions | mandatum |

The division is strict. duraturo owns whether work survives a crash; legatus
owns what a tool is allowed to touch; saige owns what the model does. mandatum
owns addressing, distribution, and the control plane, and delegates the rest.

## Topics: one namespace, matched by prefix

The mental model is one line: **users listen to an agent.**

```
agent                     every agent (an audit or dashboard consumer)
agent:docs-bot            every run of one agent
agent:docs-bot:run_01J    one run
```

Events are always published to the most specific form. Subscribing to any prefix
receives them. There is no wildcard language, because prefix containment already
expresses every subscription the mesh needs and it cannot be got wrong.

Containment is segment-wise, not string-wise: `agent:docs` does not cover
`agent:docs-bot`. A naive `strings.HasPrefix` would leak one agent's events into
another agent's subscription, so the property is pinned by a test.

### What is deliberately not a topic

An earlier design put tenant, origin, agent, revision, and event kind all in the
address, with a wildcard matcher over them. It was collapsed on purpose. Each
token that came out went somewhere better:

| Was going to be a topic token | Is now | Why |
|---|---|---|
| Event kind | A field, filtered | All events stream the same way. Splitting kinds across topics forces a consumer who wants the whole conversation to fan in from several topics and re-order them. |
| Agent revision | A field, filtered | Canary observation is a filter on one topic, not a second topic that appears and disappears as agents are revised. |
| Tenant | An authorization scope | A subscriber never names its own tenant. The server sets it from the caller's credential, so a subscriber cannot ask for another tenant's traffic by spelling it. |
| Destination | A route on the run | The same agent answers into Slack for one tag and GitHub for the next. Neither destination should exist in the topic namespace. |

The result is that a consumer only ever needs to know an agent's name to listen
to it, and no topic name has to be discovered at runtime.

## Triggers, sinks, and actors

A connector is a peer, not a client. It may implement up to three faces, and the
faces are independent:

| Face | Role |
|------|------|
| `Trigger` | Raises tags into the mesh |
| `Sink` | Renders events onto a native surface |
| `Actor` | Contributes native verbs the agent can call as tools |

Which faces a connector implements is what gives the mesh its shape, and the
asymmetry is real rather than incidental:

| Connector | Trigger | Sink | Actor |
|-----------|:-------:|:----:|:-----:|
| slack | yes | yes | yes |
| github | yes | yes | yes |
| jira | yes | yes | yes |
| cron | yes | no | no |
| webhook | no | yes | no |

A schedule has no surface to deliver to, so cron only triggers. An outbound POST
raises nothing, so webhook only sinks. Roles are derived by type assertion in
`connector.RolesOf` rather than declared, so a connector cannot advertise a face
it does not have, and `Registry.Sink(cron://...)` returns `ErrUnsupported`
instead of silently dropping an event.

No connector ever calls another connector. Sources publish, sinks subscribe, and
the bus is the only thing in between. Adding a destination is adding a
subscriber.

The `Actor` face is what turns "the agent replied" into "the work got done".
Actions run under the agent's NGAC policy rather than freely, and each one that
fires emits an `action.taken` event, so the work is auditable on the same bus as
the conversation that caused it.

## N:M delivery

One tag fans out to many sinks; one sink serves many agents. Each route chooses
its own event kinds, so granularity is per destination.

The worked example, which is what `examples/local` demonstrates:

```
Tag raised in:  github://acme/handbook/issues/42        origin "github"

Route 1:  slack://T01/C02?thread=1699.001     kinds: (all)
Route 2:  webhook://acme/deploys              kinds: lifecycle.completed
```

Slack receives every delta and edits one message as the answer arrives. The
webhook receives exactly one event and learns only that the work landed. Same
run, same bus, same events. Selectivity is a field on the route, not a separate
topic and not a separate run, which is why adding the webhook costs nothing on
the agent side.

Delivery is at-least-once, so sinks must be idempotent. The shape that makes
that natural is one message per run, edited as deltas arrive, keyed by run id.
Posting a message per delta would be correct and unusable.

The router preserves ordering per `(run, target)` pair and parallelises across
targets and runs. Ordering within a pair matters concretely: a Slack message
edited out of order shows the wrong final text.

## Agent revisions are pinned per run

An `AgentSpec` is immutable. Revising appends revision N+1 and never modifies
what is already there. A run pins its revision at accept time and executes
against that revision forever.

This is a correctness requirement, not bookkeeping. duraturo replays a workflow
from the top on every attempt. If a run resolved "latest" inside its workflow
body and someone revised the agent between attempts, the replay would run
against a different prompt and diverge, which duraturo detects as
`ErrNonDeterministic`. Pinning the revision in the run's durable input is what
makes replay safe.

The runtime goes one step further and pins the revision's content hash too, so a
revision whose content moved under a fixed number fails terminally rather than
silently executing something else.

Two consequences worth knowing:

- Deleting an agent is a tombstone, not an erase. The name stops accepting new
  tags, but `Get(name, rev)` still resolves, because a run pinned to that
  revision must remain replayable and explicable after the name is retired.
- Canary and A/B become subscription concerns. Watching revision 7 is
  `--rev 7` on one subscription, not a deploy.

## Durability

duraturo owns durability; legatus does not. legatus's queue is at-most-once in
its current beta, so a node that dies mid-task drops it. That is acceptable here
only because the durable run wrapping the task is duraturo's, and duraturo
replays it.

Each event is published twice, to two systems with different jobs:

- `duraturo.Emit` writes the durable per-run journal, which is what replay and
  cursor resume are built on.
- `bus.Publish` writes the live cross-run bus, which is what observers and
  delivery read.

The journal is truth; the bus is flow.

`Tag.ID` is the run's idempotency key. Every surface that raises tags
redelivers on timeout, so a Slack retry or a GitHub redelivery joins the run
that already exists rather than starting a second one and answering twice.

## Stopping a run

A person who presses stop wants two things: the agent to stop working, and to
be able to reply to what it had said so far. A run can be asked to stop at any
point, and a stopped run is its own outcome: not a failure, not a completion.

```
POST /v1/runs/{id}/stop            a host calls runtime.Stop instead
  -> the run ledger                the request is recorded on the run (truth)
  -> control.stop                  then published to the asker's stop stream (fast path)
     -> the executing attempt      reads the ledger before its turn, listens on the bus
                                   during it, and polls the ledger as a backstop
        -> saige stream.Stop       the model call and running tools are cancelled
           -> lifecycle.stopped    terminal: the reason, who asked, the partial answer
```

### The guarantee

From the moment `Stop` returns nil for a run that exists, belongs to the
caller's tenant and has not ended:

- no attempt of the run starts its turn, so no new turn calls a model;
- a turn already running is asked to wrap up at once over the bus, or within
  the stop poll interval (`runtime.WithStopPoll`, two seconds by default) if
  the bus lost the request;
- the run ends with `lifecycle.stopped` and is never retried, unless its turn
  finished its answer before the request took effect, in which case it ends
  completed.

"Stopped" means cut short. A turn that returned its whole answer completed,
even if a request was on its way, and the agent loop is the judge: saige
reports a turn it wrapped up, and does not report one that finished first.
Exactly one outcome is ever recorded, and the journal entry, the terminal
event, the ledger and `Result` are all built from it, so they always agree. A
host that wants to know whether it asked, independent of how the run ended,
reads `Runtime.StopRequested`, which reports the recorded request for a run of
its own tenant.

### Where the request lives

**The ledger is the truth.** `Stop` writes the request on the run as a
write-once event record (`event:mandatum.stop#0`) in the duraturo ledger the
run already lives in: per run, scoped by the run's tenant, read in one round
trip when an attempt starts, and on Postgres the same tables as every other
record (`pkg/durable`), so there is no new schema. It records only for a run
that exists in that ledger, is the caller's tenant's and has not ended, so the
ledger holds at most one stop record per run, and it lives exactly as long as
the run's other records. A request for anything else writes nothing and
publishes nothing. When a request was recorded before an attempt began, that
attempt's workflow body claims it, so duraturo does not report it as a record
no call read; one recorded during an attempt is not in that attempt's replay
set and needs no claim.

**The bus is the fast path.** After recording, `Stop` publishes `control.stop`
to `agent:_control:stop-<key>`, where the key is a hash of the caller's tenant
and the run id. It does not go on the run's own topic, because that topic has
one writer: the attempt chooses its own sequence numbers and the bus keeps the
first write of a sequence. `_control` is the one name no agent may take, the
router never delivers what is published under it, and `BusService.Publish`
refuses it, so the only way to write a request is the route that stamps the
tenant and the asker from the credential. Because the tenant is part of the
key, one tenant's requests can never land in the stream another tenant's run
reads, so they cannot push that tenant's own request out of it.

| The request arrives | What happens |
|---|---|
| Before a worker has started the turn | The attempt reads it from the ledger and ends the turn without calling the executor. |
| While the turn runs | It is delivered over the bus at once, or read from the ledger at the next poll. |
| Between two attempts | The next attempt reads it from the ledger before its turn. |
| After the process restarts | The ledger survived, and the next attempt stops. |
| After the run ended | Nothing is published. Nothing is recorded either, unless the run ends between `Stop`'s check and its write, in which case the request is recorded on the finished run, where no attempt reads it and `StopRequested` reports it. |
| While the run is parked | Recorded; it takes effect when the run resumes. mandatum itself never parks a run today. |
| A second time | The first recorded request is the one the run honours. |

**Tenant isolation does not rest on secret ids.** Run ids are derived from the
tenant and the tag id by an exported function, and tag ids are often
predictable (a schedule's occurrence, a Slack event id), so a run id is not a
secret. Isolation comes from the checks: `Stop` records and publishes only for
the caller's own run, the stop stream is keyed by the caller's tenant, and an
attempt ignores any request not made under its run's tenant. `Stop` answers
the same nil (and the route the same 202) for an unknown run, a finished run
and another tenant's run, so its answer never says whether a run exists.

Its timing can. For an id the ledger holds, `Stop` reads the run and checks
its tenant before deciding to do nothing; for an unknown id it returns after
one missed read. Someone who can time the call closely can tell whether a run
with that id exists in some tenant, though not whose it is or what it holds.
Because run ids are computable, that can reveal whether a known tag (a
schedule's occurrence, say) started a run in another tenant. Closing it needs
a ledger lookup keyed by tenant, which duraturo's ledger does not have.

**The decision is a recorded result.** duraturo replays the workflow body, and
whether somebody has asked for a stop changes between attempts, so the body
never asks. The memoized agent activity reads the ledger and listens, and
returns a stopped outcome instead of an error. The ledger records the run as
ended, and a replay reads the outcome and announces the same stop.

### What the thread and the surfaces show

A stopped run journals its exchange like a completed one: the request as the
agent renders it, then an assistant entry holding the partial answer followed
by a fixed marker line, or the marker alone when the turn was stopped before it
said anything.

```
The deploy failed because

[This turn was stopped before it finished.]
```

The marker is a constant and the reason is not written there, because the
journal is replayed byte for byte to every later run in the thread. The reason
and who asked travel on `lifecycle.stopped`, on the run's output, and on
`GetRun` (`RUN_STATUS_STOPPED`, with `stop_reason` and `stopped_by`). A request
with no reason is given saige's default, "Stopped on request", once, so every
path reports the same words. Sinks render a stopped run as stopped: the shared
layout says so in the header and under the partial answer, and a GitHub check
concludes `cancelled`.

**Delegated turns are cancelled.** Stopping a parent cancels the tool calls its
turn has in flight. A delegated sub-task is one of them, and it is aborted
rather than left to finish. It is not wrapped up the way the parent is: its
partial answer is discarded with the tool call it was the result of. A host
that carries out delegations itself (`runtime.WithDelegator`) sees the context
it was called with end with a `*runtime.StoppedError` cause (matching
`runtime.ErrStopped`, with the reason and asker) when the parent was stopped,
and with the ordinary cause otherwise, so it can stop the run it started with
the same words.

**Stop requests are not agent output.** The root topic `agent` does not cover
the control namespace, so a consumer of everything (a dashboard, a host
relaying every event) never receives a `control.stop`. An audit of requests
subscribes to `agent:_control` by name.

### How long the bus keeps a request

This only decides whether the fast path delivers a request; with the ledger it
never decides whether a run stops. A host that builds a runtime without
`runtime.WithStopLedger` has only the bus, and these are its exact bounds:

- A run's stop stream keeps the newest `bus.WithRetention` requests (1024).
  Only the run's own tenant writes there, so depth cannot lose a stop; it only
  decides which request's reason survives.
- The in-memory bus keeps the `bus.WithMaxRuns` streams written most recently
  (4096), run streams and stop streams alike. A pending request is lost once
  that many other streams have been written after it, the run's own stream
  among them.
- On Redis a run-scoped subscription reads the stream key directly, so the
  discovery index TTL does not apply, and stop stream keys have no expiry. A
  request lasts until Redis evicts or loses the key.

The control state the bus holds is bounded by the runs that were asked: a
stop stream exists only for a live run of the asker's tenant that was asked to
stop, at most one per (tenant, run), each at most `WithRetention` entries. In
memory they are evicted with every other stream. On Redis they never expire,
which is the same as every run's own stream today (the bus sets no TTL on run
keys); a deployment that needs Redis bounded sets an eviction policy.

## Usage

Every run reports what it spent: tokens, the prompt cache's share, cost where
it can be known, and how full the context was. The figures are saige's;
mandatum forwards them, keys them, and adds them up without counting anything
twice.

### What is reported, and where

| Where | What |
|---|---|
| `usage.call` | One model call, published the moment saige reports it: `accounting_id`, `saige_accounting_id` when saige gave one, the `agent` whose model made it, its delegation `depth`, `model`, `requests`, `prompt_tokens` (all input), `cached_prompt_tokens`, `cache_write_tokens`, `completion_tokens`, `cost`, `context_window` |
| `usage.child` | A child run a host carried out for a delegation, once it ended: its run id, agent and whole total, read from the ledger |
| `lifecycle.completed`, `lifecycle.stopped` | The run's total as `usage` |
| `runtime.Output.Usage` | The same total, in the ledger with the rest of the output, so it survives a restart |
| `GetRun` | `Run.usage` (`RunUsage`): `own`, `children`, `total`, `context` |
| `runtime.RecordedUsage` | A failed run's total, from its ledger records |

The total is a `payload.RunUsage`. `Own` is the run's own agent, `Children` is
everything it delegated at every depth, and `Total` is the two added: the
figure to hold against a limit. `Context` is the run's own agent at its latest
model call: the model, its context window, and how many tokens that call's
prompt was.

**Usage is opt-in for delivery.** It is its own kind family, `usage`, and a
route receives it only by naming the family or one of its kinds: a route with
no kind filter, like one selecting `delta` or `lifecycle`, never does. Routes
with no filter were delivering to receivers that knew a fixed set of kinds
before usage existed, and an event per model call that such a receiver
refuses, or fails on and has retried in the run's ordered delivery lane, would
delay its `lifecycle.completed`. "Everything" does not cover accounting, as
the root topic does not cover the control namespace. Subscriptions are not
routes: a subscription with no filter (`mandatum listen`, SSE) sees usage like
any other kind.

Sinks that render a chat surface (Slack, GitHub, Jira) ignore usage even when a
route names it: token counts and cost are for whoever pays, not for everyone
in a channel or on an issue. A webhook route that selects `usage` receives it.
`mandatum listen` prints one line per call, and `mandatum tag` prints the
run's total on stderr once the run ends. `BusService.Publish` refuses the
family: only the attempt that spent the tokens writes them.

### Cost and money

Cost comes from saige's price catalog and nowhere else. saige's adapters
resolve a model's rate card from the catalog (`ModelCapabilities.Pricing`,
with its `AsOf` date and currency), and mandatum prices each call with
`Pricing.Cost` over saige's billing tiers (`UsageFromDelta`). A model with no
rate card, a modality the card does not price, or a call whose provider
reported no tokens gets no cost: its tokens are counted, the call is counted in
`unpriced_calls`, and nothing is guessed. A call saige answered from its
response cache sent nothing to a provider and is reported with no tokens and
no cost.

Money is `payload.Cost{Micros, Currency, AsOf}`: integer millionths of an ISO
currency, saige's `types.Cost`. One US dollar is 1,000,000 micros with currency
`USD`, the int64 micro-USD convention a host enforcing spend limits uses. Each
call's cost is rounded to the micro once, by saige, and a total is the integer
sum of its calls.

Money is never converted and never dropped. A total's `cost` is in the
currency of the first priced call it counted; calls priced in any other
currency are summed in `other_costs`, one entry per currency. saige's catalog
prices everything in USD, so `other_costs` is empty unless a deployment
supplies rate cards of its own. A host holding a USD limit reads `cost` when
its currency is `USD` and treats anything in `other_costs` as spend it cannot
compare. A total's `as_of` is the oldest date among its calls, and empty when
any was priced from a card with no date.

### Counted once, across retries and replays

duraturo retries a failed attempt and replays the workflow body from the top,
and a delivery can repeat any event. The rule:

- **Every model call any attempt made is counted, once.** Before the agent
  activity returns, failed or succeeded, it writes the calls it relayed to the
  run's ledger as a write-once event record,
  `event:mandatum.usage.a<attempt>#0`. A failure is retried, and so is a
  success whose result duraturo never managed to record; either way the
  attempt that finally records an outcome reads the earlier attempts' records
  inside the agent activity, adds its own calls, and the total is memoized
  with the outcome. The workflow body claims the records the outcome counted,
  which it learns from the outcome, so a replay claims the same ones.
- **A replay counts nothing.** The agent activity is memoized, so a replay
  reads the recorded total and calls no model.
- **A stopped turn is counted.** saige settles the call a stop cut short and
  reports its usage when the provider had reported any, and the stopped
  outcome carries the total like a completed one.
- **Each report has a key.** `accounting_id` is always mandatum's,
  `<run>/a<attempt>[/<agent>.<sub>]/<n>`, which no other call of any attempt or
  any delegation can share. saige's own `AccountingID` is carried as
  `saige_accounting_id` and never counted by, because saige keeps it stable
  across a durable replay of its own. A child run is keyed `run:<child run
  id>`. `payload.Tally` counts each key once, so a consumer summing the stream
  at least once gets the same total the run records.

The total is counted from the usage events the attempts relayed, never from a
figure an executor returns. It equals the sum of the run's call events
whenever every attempt that called a model returned, which is every case but
two, both in Limitations: an attempt that dies without returning, and a
superseded attempt that records after the outcome was decided.

### Roll-up

What a run delegated is the run's spending too, and each call is counted in
exactly one run's `Own`.

- **In-turn sub-tasks** (the `tag_<agent>` tool without a host Delegator) are
  not runs. Their calls are relayed onto the parent run's stream as
  `usage.call` one level deeper (`depth` 1), as they happen, and counted in the
  parent's `Children`, whether the sub-task answers, fails or is aborted. The
  relay carries any depth, but the sandbox gives a sub-task no spawn grant of
  its own, so in practice a sub-task's own delegation is refused by policy and
  in-turn delegation is one level deep.
- **Delegated runs** (a host's `runtime.WithDelegator`) are runs of their own
  with their own totals. The host rolls one up by calling
  `runtime.ReportChildUsage(ctx, childRunID)` with the context its Delegator
  was given. Only the run id comes from the host: the runtime reads the child
  from the ledger, refuses it with `runtime.ErrNotAChild` unless it is a run
  of the parent's tenant, not the parent, that has ended (another tenant's run
  is refused in the same words as one that does not exist), and takes its
  agent and total from the child's own record. The parent's stream carries a
  `usage.child` and the parent counts it in `Children`, once per child run
  however many times it is reported or however many parent attempts delegated
  to it. A child's total already includes what it delegated, so three levels
  of runs roll up with each call counted once at the origin.
- **A tag raised at another agent** is an unrelated run and does not roll up.

## Package DAG

Dependencies run one way. Leaves are stdlib-only and know nothing about
transports, storage, or agents.

```
leaves (stdlib only)
  pkg/topic        agent:name[:run], prefix containment, the control namespace
  pkg/address      github://urmzd/mandatum/issues/42, slack://T01/C02?thread=...
  pkg/envelope     Tag, Event, Kind, Route, Filter, Subscription
  pkg/signature    HMAC verification for inbound webhooks
  pkg/agentspec    immutable specs, content-addressed revisions

seams
  pkg/connector    Trigger / Sink / Actor faces, registry
  pkg/bus          Bus interface, Memory and Redis Streams, bustest suite
  pkg/registry     agent store, Memory implementation

composition
  pkg/agentrt      builds a saige agent from a pinned revision; citations
  pkg/runtime      the durable duraturo turn, legatus sandbox, stop, usage
  pkg/router       delivery fan-out to sinks
  pkg/connectors   slack, github, jira, cron, webhook
  internal/server  Connect handlers, SSE, webhook ingress
  internal/cli     command tree and the composition root
```

`internal/cli/core.go` is the composition root and the fastest way to understand
how the system fits together: it assembles the bus, registry, router, runtime,
connectors, and transport in the order data flows through them.

## Transports

Connect (connectrpc.com) serves gRPC, gRPC-Web, and HTTP/JSON from one handler
set, so gRPC clients and browsers share one contract and one codegen path.

| Surface | Purpose |
|---------|---------|
| `InvokeService.Invoke` | Tag an agent, return the run id once durable |
| `InvokeService.InvokeStream` | Invoke and tail in one RPC, with no race between accept and subscribe |
| `BusService.Subscribe` | Topic plus filter plus cursor, long-lived |
| `BusService.Publish` | Append an event to a run's topic |
| `AgentService.*` | Agent spec control plane |
| `GET /v1/sse` | The same stream, browser-native, resumable via `Last-Event-ID` |
| `POST /v1/runs/{id}/stop` | Ask a run to stop; answers 202 for every run id. `GetRun` reports `RUN_STATUS_STOPPED` |
| `InvokeService.GetRun` | A run's record, its usage included (`Run.usage`) |
| `POST /v1/webhooks/{connector}` | HMAC-verified ingress |

`InvokeStream` exists because doing invoke and subscribe separately races: events
emitted between the two are missed. Here the subscription is established before
the run is enqueued.

## Security boundaries

Two, and changes near either require tests proving confinement still holds.

**`pkg/signature`** is the only thing between the public internet and starting a
run. It verifies HMAC over the raw request body before that body is parsed.
Rules that follow: verify before parsing, hash the exact bytes received, compare
with `hmac.Equal` in constant time, and fail closed on every ambiguity including
a missing, empty, or duplicated header.

Slack signs a timestamp inside its base string, so a replay window is
enforceable and is enforced. GitHub does not, so a captured GitHub request stays
valid as long as the secret does. That defense lives downstream in `Tag.ID`
idempotency instead, and the package documents this rather than implying a
protection it cannot provide.

**The NGAC policy path** is legatus's. An agent's `Access` block compiles into a
policy graph enforced by the sandbox with default deny, covering both workspace
areas and the spawn allowlist that governs agent-tags-agent.

## Limitations

Verified against the code, and honest.

- **The bus needs Redis or memory, not Postgres.** duraturo's `pgqueue` does not
  implement the `DeltaLog` capability, stated explicitly at
  `adapters/postgres/pgqueue/pgqueue.go`. A Postgres-only deployment runs
  durably but cannot stream.
- **duraturo's Redis and Postgres adapter submodules are not depended on.** They
  currently fail checksum verification against `sum.golang.org`, so mandatum
  depends only on duraturo's root module and ships its own Redis Streams bus
  backend. duraturo's in-memory ledger and queue are complete systems and are
  what the single-binary deployment uses.
- **legatus's queue is at-most-once** in beta. Durability comes from duraturo.
- **Citations are derived from retrieval.** Citations are extracted from RAG
  `AssembledContext` blocks and emitted as mandatum's own `delta.citation`
  events. saige's `CitationDelta`, which reports what a model or tool cited, is
  not translated into events yet.
- **duraturo v1 workflow bodies are single-goroutine.** Parallelism belongs
  inside a legatus task, not in a forked workflow.
- **The bus is at-least-once with no consumer groups.** Subjects, filters, and
  cursors cover real-time consumers. If durable competing consumers with
  independent offsets become a requirement, the honest answer is NATS JetStream
  behind the `Bus` interface rather than growing a broker here.
- **Agent specs are loaded from JSON**, not YAML, so the CLI carries no parser
  dependency.
- **A stop is reliable only with the ledger.** `runtime.WithStopLedger` must
  be given the ledger the runs execute from, as the CLI does. A host without it
  falls back to the bus alone, with the bounds in "How long the bus keeps a
  request". With an in-memory ledger, only the process that accepted a run can
  stop it, which is also the only process that can read it with `GetRun`.
- **A crash between the wrap-up and its record loses the partial text from the
  thread.** If a worker dies after saige stopped the turn but before the
  stopped outcome was recorded, the next attempt finds the request in the
  ledger and stops before calling the model, with no text. The thread then
  holds only the marker, while the surface already showed what the first
  attempt streamed.
- **A turn holds one bus subscription while it runs.** On Redis each one holds
  a pooled connection for most of its poll cycle. The CLI sizes the pool at
  go-redis's default plus one connection per run a process executes at once;
  a host building its own client should do the same. One shared subscription
  per process would remove this cost; it needs a way to subscribe to a prefix
  starting from now, which the bus does not have.
- **Stopping reaches a turn through the in-process sandbox.** The stop signal
  and the abort of an abandoned task cross from the waiting goroutine to the
  legatus tool in memory. A remote legatus node would need them carried over
  its own transport; only the in-process plane is supported and tested.
- **Stop is an HTTP route, not an RPC.** `POST /v1/runs/{id}/stop`. A run's
  stopped status is on the wire (`RUN_STATUS_STOPPED`); asking for a stop is
  not yet a Connect method.
- **A stop wraps up the turn, not its side effects.** Tool calls already
  finished stay done, and a tool that ignores its context delays the stop until
  the grace period (ten seconds by default) cancels the turn. A turn that is
  cancelled this way ends stopped with the text it had published.
- **A route that selects only `lifecycle.completed` is not told about a stop.**
  Select `lifecycle`, or add `lifecycle.stopped`, to hear every ending.
- **Earlier attempts' usage needs the ledger.** An attempt's calls are
  recorded on the run through `runtime.WithStopLedger`. Without it, the total
  counts the attempt that produced the outcome, and the `usage.call` events
  still report every call. If the record cannot be written, it is logged, and
  if that attempt is executed again its calls are left out of the total.
- **A crash mid-turn loses that attempt's calls from the total.** An attempt
  that dies without returning (the process is killed, the machine goes away)
  writes no usage record. Its calls were published as they happened and are on
  the bus, but the run's total does not count them, so the total is then lower
  than its call events.
- **A superseded attempt may be counted late or not at all.** An attempt that
  lost its lease and keeps running until it notices may write its record after
  the attempt that took over has read the records and fixed the total. Its
  calls are then on the bus and in the ledger, and not in the total.
- **Rolling up a delegated run needs the ledger and the Sandbox.**
  `ReportChildUsage` reads the child from the run ledger, so a runtime without
  `WithStopLedger`, or an executor other than `Sandbox`, refuses every report.
- **A call a turn never finishes may not be counted.** saige reports a call's
  usage when the call completes, or when a stop wraps it up. A call cancelled
  outright (a worker shutting down, a stop whose grace period ran out, a
  sub-task aborted with its parent) reports what its provider had sent, which
  may be nothing.
- **Cost is only as good as saige's catalog.** Rates are recorded on a date
  (`as_of`) and do not know about contract pricing, regions or batch calls, so
  a cost is an estimate of the list price, not an invoice. There is no
  conversion between currencies.
- **The context window is saige's declaration.** It is the model's input
  limit from the catalog, 0 when the catalog does not declare one, and the
  prompt size is the provider's count for the latest call. Nothing estimates a
  prompt before it is sent.
- **Roll-up of delegated runs is the host's call.** mandatum cannot see a run
  a Delegator started until the host reports it with
  `runtime.ReportChildUsage`, and the host must report it after it ended.
- **A stop cannot be made for a run before it is accepted.** Nothing is
  recorded or published for a run the ledger does not hold yet; ask again once
  `Accept` has returned the run id.
