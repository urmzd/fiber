# fiber

Agent library and low-latency pub/sub bus. Publish a typed event on a channel,
wake the agents that are members of it, stream the run's events to anyone,
deliver them anywhere. Go module
`github.com/urmzd/fiber`. It is a library first, composed in Go: no
service, no configuration files, no environment variables. The root package
`fiber` is the composition root (`fiber.New(ctx, ...Option)`); Humagen is the
platform built on it, owns all configuration, and imports only `pkg/`. The
module's one command, `cmd/fiber` (`fiber run`), is a program on the library
for validating a definition locally, not a way to serve one.

**Status: v0 alpha, pre-1.0, not recommended for production.** Interfaces are
still moving and may change between minor versions. The in-memory composition
is complete and tested. Every Postgres store, the Postgres bus and the Redis
bus pass their conformance suites in CI's integration job (`make integration`
locally). Keep the surface small and document limitations honestly.

The design turns on one idea: where an event arrives and where its answer lands
are independent. Read `docs/architecture/events.md` (the event union, channels,
definitions, the Runtime seam) and `docs/architecture/overview.md` (how a run
behaves) before changing anything structural.

## Architecture

Composes three libraries and adds addressing, distribution and the control
plane. saige owns the agent loop, anchor owns durability, playscape owns
sandboxing and access control.

| Package | Role |
|---------|------|
| `pkg/event` | The one event shape: `Envelope` (CloudEvents-shaped: `id`, `source`, `type`, `subject`, `channel`, `time`, `hops`, plus `tenant`, `run`, `seq`, `agent`, `version`, `digest`), a sealed `Body` union with one struct per type, `Unknown` for types this build does not know, and the catalog (`Register`, `Lookup`, `Render`) holding each type's decoder, renderer and `Sensitive`/`OptIn` flags. Channel and type prefix matching, the control channel, the actor a context acts for (`WithActor`, `ActorFrom`). Leaf apart from `pkg/address` and `pkg/attachment`. |
| `pkg/event/legacy` | Reads JSON written before the union (bus rows, journal entries, old run inputs, old kind selectors) and maps each old kind to its type. Raw JSON only; imports none of the removed packages. |
| `pkg/address` | Channel URIs: `github://urmzd/fiber/issues/42`, `slack://T01/C02?thread=...`. Leaf. |
| `pkg/signature` | HMAC verification for inbound webhooks (Slack v0, GitHub sha256). Leaf. Security boundary. |
| `pkg/bus` | The channel bus: `Bus` carries `event.Envelope`; `Subscription{Tenant, Channels, Types, Run, From}` is default deny. Hop limit (`DefaultMaxHops`), run sequences, retention. `Memory`, Redis Streams and Postgres (`fiber_events`, `fiber_event_runs`); `bustest` conformance suite. |
| `pkg/connector` | `Trigger` (`Ingest(ctx, publish)`) / `Sink` / `Actor` faces, derived by `RolesOf`, `Subscriber` for a sink that names its channels and types, plus the registry of sinks. |
| `pkg/definition` | Agents as saige `.agent.md` definitions plus a fiber `Policy` sidecar. The insert-only `Source` (`Memory` and `Postgres`, `definitiontest`), the `Resolver` that pins a version tree (`Pin`: name, version, saige digest, sub-agent versions, policy hash) and `Verify`s it on replay (fail closed), fiber's validation, MCP grants, and the converter for stored agents from before the union (see the upgrade section of `docs/architecture/events.md`). |
| `pkg/membership` | Which agents are members of which channel prefixes, per tenant, with a `Rule` (type prefixes, addressed only by default). Default deny. `Memory` and `Postgres`, `membershiptest`. |
| `pkg/dispatch` | Wakes agents: listens to a tenant's ingress events and calls `Runtime.Accept` for each member whose rule matches. Drops self-wakes and events at the hop limit; maps `slack.session.stopped.v1` to stop requests. |
| `pkg/egress` | Delivery to sinks. Each sink subscribes to its own scheme's channels (or the channels it names, `connector.Subscriber`) with run types; opt-in types only when named; control channels never. Ordered per `(run, channel, target)`, skipping events older than its max age. |
| `pkg/turn` | The one Runtime seam: `Runtime.Turn(ctx, Pinned, Input, emit)` emits catalog bodies and returns a `Result`. `Func` adapts a function; stop and compact signals travel on the context. Imports no saige. |
| `pkg/turn/native` | The default `turn.Runtime`, on saige: builds an agent from a pinned definition, renders the trigger through the catalog, translates deltas into bodies, extracts citations, counts usage, parks on questions. A granted tool it cannot resolve is dropped and announced to the model by name (`WithStrictTools` refuses instead). `ResolveTools` resolves grants without building a model client. With `pkg/definition`, the only package that imports saige directly. |
| `pkg/turn/sandbox` | `Sandboxed(inner, plane)`: any `turn.Runtime` inside playscape, as a task under the pinned policy (spawn and workspace areas). |
| `pkg/turn/runtimetest` | The conformance suite every `turn.Runtime` passes. |
| `pkg/runtime` | The durable anchor turn: accept (pins the definition and the thread history), the `fiber.turn.v2` activities, dual publish, stopping a run mid-turn, compacting it on request, per-run usage, and parking a run on a person. Every turn's context carries the actor on the run's record (`event.ActorFrom`). Runs recorded under the v1 activity names fail terminally. |
| `pkg/durable` | The Postgres schema for anchor's ledger and queue adapters, and `Open`, which builds both on it. |
| `pkg/internal/pgrename` | The one migration step every Postgres store runs first: renames objects a database created before fiber's rename still carries, and refuses (naming both) when an old table was created again beside its new one. |
| `pkg/thread` | The per-thread conversation journal a run continues: append-only, pinned at accept, with a reconciliation seam for surfaces. `Memory` and `Postgres`; `threadtest`. |
| `pkg/blob` | Content-addressed, tenant-scoped file store. `Memory` and filesystem; `blobtest`. |
| `pkg/attachment` | The file reference an ingress event carries. Leaf. `intake/` stores and fetches files (SSRF-guarded); `resolve/` turns a file into model input by kind. |
| `pkg/toolprovider` | MCP tool providers as versioned records: how to reach a server, whose credential it is reached with (`CredentialScope`: `shared`, the default, or `person`), and the admin-accepted, numbered tool snapshots. A tool is shown to models as `<provider>__<tool>`. `Memory` and `Postgres`, `toolprovidertest`. |
| `pkg/mcptools` | MCP client (stdio, streamable HTTP) over `toolprovider`: lists and diffs a server's tools, exposes only accepted ones as saige tools, enforces the grant's class per call, and redacts the resolved credential everywhere. `Grants` expands a definition's `tools.mcp` server (write only with `Policy.mcp_write`) per tenant. A person-scoped provider is expanded for the actor on the turn's context with that person's own credential (`PersonResolver`); a person with none gets `ErrNoPersonCredential` and a note for the model, never the shared credential. |
| `pkg/knowledge` | Hybrid retrieval over indexed documents: dense plus lexical, fused, with citations. `Memory` and `Postgres`, `knowledgetest`. |
| `pkg/connectors` | Ingress and egress adapters: `slack`, `github`, `jira` (ingress, egress, actor), `cron` (ingress only), `webhook` (egress only, body v2), `author` (actor only: definitions as tools, prompt review, scaffolding, the seeded `author` agent), `mention` (the mention helper every ingress adapter uses), plus shared helpers under `internal/`. |
| `fiber` (module root) | The composition root. `New(ctx, ...Option)` builds an `App` (`app.go`). Every backend is a constructed value passed by an option (`WithPostgres(pool)` for every store family, `WithBus`, `WithDefinitions`, `WithMemberships`, `WithDurable`, `WithThreads`, `WithKnowledge`, `WithBlobs`, `WithToolProviders`, `WithTurnRuntime`); anything not given is memory. Extensions are options too (`fiber.go`, `extensions.go`). `App.Publish` puts an ingress event on the bus; `App` runs dispatch, delivery and execution in process and exposes webhook connectors as `http.Handler`s; it listens on nothing and reads no file or `FIBER_*` variable. |
| `cmd/fiber` | `fiber run [flags] DEFINITION MESSAGE`: stores one `.agent.md` (and `-policy FILE`) on a `fiber.New` with memory backends and a temporary workspace, publishes one `message.posted.v1`, prints the run's events by type (text, or `-json` one envelope per line) and exits 0 completed, 1 not, 2 bad input. `-offline`, `-provider`, `-model`, `-tools` (prints `App.ToolsFor`, runs nothing), `-timeout`. Nothing imports it. |

Discover layout with `tree` or ripgrep; do not trust stale listings.

## Commands

| Action | Command |
|--------|---------|
| init | `make init` |
| build | `make build` (`go build ./...`) |
| test | `go test ./...` |
| race test | `go test ./... -race` |
| lint | `golangci-lint run` |
| fmt | `gofmt -w .` |
| quality gate | `make check` |
| end-to-end demo | `go run ./examples/local`, `go run ./examples/downstream` |
| validate a definition | `go run ./cmd/fiber run -offline DEFINITION.agent.md MESSAGE`, `-tools` to list its grants |
| backends | `make up` (Postgres with pgvector, Redis), `make down` |
| suites on real backends | `make integration` (sets `FIBER_POSTGRES_TEST_URL` and `FIBER_REDIS_ADDR`) |

## Code Style

- Idiomatic Go, stdlib-first; every dependency must earn its place.
- Package doc comments explain the concern, its orthogonality boundary, and the
  why. `pkg/event/body.go` and `pkg/signature/signature.go` set the bar.
- Errors: wrap with `fmt.Errorf("pkg: context: %w", err)`; sentinel errors for
  callers to `errors.Is` on.
- In-memory implementations are complete systems, never stubs.
- Tests are table-driven and named after the property they protect, not the
  function they call.
- Conventional commits (feat/fix/chore/...); `sr` cuts releases from them.
- No em dashes in documentation.

## Rules

- **`pkg/signature` is a security boundary.** It is the only thing between the
  public internet and starting a run. Any change needs tests proving
  confinement: constant-time comparison, verify-before-parse, replay window,
  malformed input, and duplicated headers. Default is always deny.
- **The NGAC policy path is a security boundary.** An agent's `Access` block
  compiles into playscape's policy graph with default deny. Changes need tests
  proving spawn gating and workspace confinement still hold.
- **Every type has a renderer.** A new event type is registered once in the
  `pkg/event` catalog with its decoder, a byte-stable renderer and its
  `Sensitive` and `OptIn` flags. The catalog test fails on a type without a
  renderer. `type` is the only discriminator and carries the version: a
  breaking change to a body is a new `.vN` type, never an edit. A known type
  whose data does not decode is an error; only an unknown type becomes
  `event.Unknown`.
- **Subscriptions are default deny.** An empty tenant, channel list or type
  list matches nothing; there is no "everything" subscription. A control
  channel matches only a subscription that names it exactly. Opt-in families
  (`run.usage`, `run.context`) arrive only when named. Memberships are default
  deny too: no row, no run. Never add a wildcard.
- **One Runtime seam.** A turn runs through `turn.Runtime` and nothing else.
  Only `pkg/turn/native` imports saige directly, besides `pkg/definition`,
  whose agents are saige definitions (`pkg/turn/imports_test.go` guards it);
  `pkg/runtime`, `pkg/turn` and `pkg/turn/sandbox` reach saige only through
  them.
  Sandboxing is a decorator around any Runtime, never a second path.
- **Pins fail closed.** A run pins its definition at accept (name, version,
  saige digest, sub-agent versions, policy hash) and every attempt verifies
  the pin. A moved digest, a missing row or an empty digest fails the run
  terminally (`anchor.NonRetryable`). Never re-resolve "latest" inside a
  workflow body: anchor replays from the top, and an unpinned run diverges.
  Tests that assert this exist in `pkg/runtime` and `pkg/definition`; do not
  weaken them.
- **Preserve the DAG.** Leaves (`address`, `signature`, `attachment`) stay
  stdlib-only; `pkg/event` imports only those. Nothing under `pkg/` imports
  `internal/`, and `pkg/event/legacy` imports none of the removed packages.
- **History pinning is load-bearing too.** A run pins "thread history up to
  sequence N" at accept, next to its definition pin, and every attempt loads exactly
  that. The journal only appends, and what it replays is byte-identical from
  run to run, which is what keeps a long thread's prompt cache warm.
- **Volatile content goes last.** Tools and system prompt, then history, then
  the request, then its files. Nothing that changes per turn (timestamps, run
  ids, retrieved passages, attachments) may sit ahead of something that does
  not.
- **Durability comes from anchor, never from playscape.** playscape's queue is
  at-most-once in beta.
- **A run's events have one writer.** Only the attempt executing a run
  publishes its `run.*` events, with sequence numbers it chose. Anything
  addressed to a run, such as a stop request, goes to its control channel
  (`event.ControlChannel`) instead.
- **A stop is recorded before it is announced.** `Runtime.Stop` writes the
  request on the run in the ledger and only then publishes it; the bus is the
  fast path, never the only copy. Hosts pass `runtime.WithStopLedger`.
- **A stopped run is a result, not an error.** The agent activity returns a
  stopped outcome and the workflow body decides from that record alone, so a
  replay announces the same stop. Never read "has a stop been requested" in
  the body.
- **Nothing about a particular stop reaches the thread journal.** A stopped
  turn is journaled as its partial answer plus `runtime.StopMarker`, a
  constant. The reason and the asker go on `run.stopped.v1`.
- **Usage is counted from what an attempt relayed, once.** A run's total is
  built from the `run.usage.v1` and `run.usage.child.v1` events its attempts published,
  keyed by fiber's attempt-unique `accounting_id`, inside the memoized agent
  activity. Every attempt records its calls on the run before it returns,
  success or failure; the workflow body never reads usage. Cost comes only
  from saige's price catalog, as integer micro-units, never converted or
  estimated. Usage is opt-in for sinks and chat sinks do not render it.
- **A parked run decides from its records.** A question is a anchor step the
  body records once; its resolution is the write-once event record
  `event:fiber.answer.<id>#0`, written by `Runtime.Answer`, by a stop, or by
  an expiry, and whichever was first is the answer. The body never reads the
  clock or a request to decide; it reads that record. An attempt runs at most
  one turn: the attempt that asked parks, and the answer wakes the next.
- **An approval is a second pair of eyes.** `Runtime.Answer` refuses an
  approval from the requester, or from the credential that raised the run,
  unless the pinned Policy sets `self_approval`, and admits only the Policy's
  `approvers`/`answerers` when it names them. Never widen this to tenant-wide.
  A call that runs with the credential of the person the run acts for
  (`native.PersonScoped`, `QuestionTool.ActsForPerson`) is approved with
  edited arguments only by that person; anyone else approves or denies it as
  asked.
- **A run never waits for another run in its turn.** With
  `runtime.WithChildRuns` a delegation parks the parent on the child's ending;
  a blocking wait holds a worker and N of them deadlock N workers.
- **A continuation sends what an uninterrupted turn would have.** The resumed
  turn rebuilds the parked conversation from the pinned history, the recorded
  request and the recorded transcript, and answers its first model call with
  the recorded message. Nothing may be re-rendered, re-retrieved or reordered
  on the way, or the prompt cache misses and the model sees a different
  conversation.
- **Sinks must be idempotent.** Delivery is at-least-once. Render one message
  per run and edit it; never post per delta.
- **Tenant is never read from a request body.** fiber has no transport; a
  program that accepts events from callers (Humagen) sets the tenant from the
  caller's credential and overwrites whatever the client sent.
  `App.Publish`, `Runtime.Accept` and `App.ToolsFor` trust the event they are
  handed. A host that lets callers publish refuses the `control` scheme and
  the `run.*` types.
- **No self-wake, bounded hops, joined redelivery.** Dispatch never wakes the
  agent that produced an event, refuses an event at the hop limit, and keys a
  run by `(tenant, event id, agent)` so a redelivered event joins its run.
  Delivery is at-least-once; the run key is the only deduplication.
- **Sensitive bodies stay in.** A body the catalog marks `Sensitive`
  (human-authored text or private host data) is rendered to the model, but
  egress adapters and logs leave it out unless the subscriber opts in.
  `Envelope.Private` is never serialized.
- **An ingress adapter chooses no agent.** It publishes a typed event on the
  surface's channel, with the agents the text mentions. The channel's members
  decide who answers, and a run's events go back out on the same channel.
- **Neither is the actor.** An event's actor is the credential's subject unless
  the credential is marked `ActAs`, which is what a surface that authenticated
  the person itself holds. Authorship and approvals are decided on it. The
  same holds for every actor field, `user` (the host's account id) included:
  a credential without `ActAs` cannot set any of them.
- **A shared credential is never a fallback.** A person-scoped MCP provider
  is reached only with the credential of the person the run acts for, on a
  session of that person's own; it names no shared credential (the store
  refuses one), `mcptools.Connect` never reads one for it, and a person with
  none gets no tools and a note saying so. Never widen this to "try the
  shared one", to another person's session, or to a credential read from
  anything but the resolver. Changes need tests proving each person reaches
  the server with their own, a person with none reaches it with nothing, and
  every credential stays redacted.
- **Who a turn acts for comes from the run's record.** The runtime puts the
  accepted event's actor on the turn's context and the sandbox carries it to the
  task in process; it is never read from the task input, trigger context or
  anything a model produced, and a turn for nobody acts for
  nobody.
- **The command is a program on the library.** `cmd/fiber` composes
  `fiber.New` through exported API only, reads no file but the definition
  and policy it is given and no environment variable of its own (the model providers read
  their vendors' keys, as in any program), listens on nothing and keeps
  nothing. What it needs from fiber is library API that any program could
  call, as `App.ToolsFor` is; it never reaches past that, and nothing in the
  module imports it.
- This is a v0 alpha: keep the surface small and document limitations honestly
  rather than papering over them.

## Extension Guide

- **New connector**: implement `connector.Connector` plus whichever of
  `Trigger` (ingress: publish one typed event on the surface's channel, choose
  no agent), `Sink` (egress), `Actor` apply, then register it. Roles are
  derived by type assertion, so implementing a face is how you advertise it.
  A sink that is not bound to its own scheme implements `connector.Subscriber`
  and names its channels and types. Reuse `pkg/connectors/mention` and the
  shared helpers in `pkg/connectors/internal/` for idempotent sink rendering
  and HTTP plumbing rather than reimplementing them.
- **New event type**: add a body struct in `pkg/event` and register it in the
  catalog with `New`, a byte-stable `Render`, `Sensitive` (true when it carries
  human-authored text or private host data) and `OptIn`, plus a golden in
  `pkg/event/testdata`. Types are fields, never channels. A type that ends a
  run must also be taught to everything that waits for a run to end:
  `render.Status.Terminal`, and the ledger status mapping in `status.go`
  (`Status.Terminal`). An old kind that maps to it goes in
  `pkg/event/legacy`.
- **New Runtime**: implement `turn.Runtime`, pass `runtimetest.Run`, and give
  it to `fiber.WithTurnRuntime`. Wrap it in `sandbox.Sandboxed` for playscape
  isolation. Durability and pinning stay in `pkg/runtime`.
- **New bus backend**: implement `bus.Bus` and run it through
  `pkg/bus/bustest.Run`, which holds every backend to the identical contract.
- **New store backend**: implement the package's `Store` (or `Source`) and run
  it through its suite (`definitiontest`, `membershiptest`, `knowledgetest`,
  `threadtest`, `toolprovidertest`, `blobtest`), as `Memory` and `Postgres`
  do. Wire it in `app.go` behind an option, or let callers pass it to the
  existing one.
- **New model provider**: an OpenAI-compatible endpoint is
  `fiber.WithOpenAICompatible`. Anything else outside this repo is a
  `fiber.WithModelProvider` in the composing program; see
  `examples/downstream`. A built-in belongs in `pkg/turn/native`'s provider
  construction and `definition.KnownProviders`. Keep
  `agenttest.ScriptedProvider` (the `offline` provider) working, since it is
  what makes the examples and the tests run with no API key.
- **Downstream extensions**: tool sources, connectors and MCP tool providers
  are `fiber` options too. Each option fills a field of `options`
  (`extensions.go`), so a new extension point is a field there, its wiring in
  `New`, and an option. No option reads a file or the environment.
