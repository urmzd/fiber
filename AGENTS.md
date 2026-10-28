# fiber

Agent library and low-latency pub/sub bus. Tag an agent from anywhere, stream
its events to anyone, deliver them anywhere. Go module
`github.com/urmzd/fiber`. It is a library first, composed in Go: no
service, no configuration files, no environment variables. The root package
`fiber` is the composition root (`fiber.New(ctx, ...Option)`); Humagen is the
platform built on it, owns all configuration, and imports only `pkg/`. The
module's one command, `cmd/fiber` (`fiber run`), is a program on the library
for validating a spec locally, not a way to serve one.

**Status: v0 alpha, pre-1.0, not recommended for production.** Interfaces are
still moving and may change between minor versions. The in-memory composition
is complete and tested. Every Postgres store, the Postgres bus and the Redis
bus pass their conformance suites in CI's integration job (`make integration`
locally). Keep the surface small and document limitations honestly.

The design turns on one idea: where a tag is raised and where its answer lands
are independent. Read `docs/architecture/overview.md` before changing anything
structural.

## Architecture

Composes three libraries and adds addressing, distribution and the control
plane. saige owns the agent loop, anchor owns durability, playscape owns
sandboxing and access control.

| Package | Role |
|---------|------|
| `pkg/topic` | Topic addressing: `agent`, `agent:<name>`, `agent:<name>:<run>`. Prefix containment, no wildcards. The reserved control namespace `agent:_control:<verb>-<key>`. Leaf. |
| `pkg/address` | Mesh endpoint URIs: `github://urmzd/fiber/issues/42`, `slack://T01/C02?thread=...`. Leaf. |
| `pkg/envelope` | Wire contract: `Tag`, `Event`, `Kind`, `Route`, `Filter`, `Subscription`, and the actor a context acts for (`WithActor`, `ActorFrom`). Leaf. |
| `pkg/signature` | HMAC verification for inbound webhooks (Slack v0, GitHub sha256). Leaf. Security boundary. |
| `pkg/connector` | `Trigger` / `Sink` / `Actor` faces, derived by `RolesOf`, plus the registry. |
| `pkg/definition` | Agents as saige `.agent.md` definitions plus a fiber `Policy` sidecar. The insert-only `Source` (`Memory` and `Postgres`, `definitiontest` conformance suite), the `Resolver` that pins a version tree and `Verify`s it on replay (fail closed), fiber's validation, MCP grants, and `FromAgentSpec` for the old spec shape. |
| `pkg/durable` | The Postgres schema for anchor's ledger and queue adapters, and `Open`, which builds both on it. |
| `pkg/internal/pgrename` | The one migration step every Postgres store runs first: renames objects a database created before fiber's rename still carries, and refuses (naming both) when an old table was created again beside its new one. It also names the pre-rename turn, which `pkg/runtime` registers only to fail such a run with a reason. The only code that knows the old names. |
| `pkg/thread` | The per-thread conversation journal a run continues: append-only, pinned at accept, with a reconciliation seam for surfaces. `Memory` and `Postgres`; `threadtest.Run` is its conformance suite. |
| `pkg/blob` | Content-addressed, tenant-scoped file store. `Memory` and filesystem; `blobtest`. |
| `pkg/attachment` | The file reference a tag carries. Leaf. `intake/` stores and fetches files (SSRF-guarded); `resolve/` turns a file into model input by kind. |
| `pkg/toolprovider` | MCP tool providers as versioned records: how to reach a server, whose credential it is reached with (`CredentialScope`: `shared`, the default, with its credential reference, or `person`, which names none), and the admin-accepted, numbered tool snapshots. Changed tools stay pending; read/write class defaults to write. A tool is shown to models as `<provider>__<tool>` (`QualifiedName`). `Memory` and `Postgres`, `toolprovidertest`; the composition root migrates and wires it. |
| `pkg/mcptools` | MCP client (stdio, streamable HTTP) over `toolprovider`: lists and diffs a server's tools, exposes only accepted ones as saige tools, enforces the grant's class per call, and redacts the resolved credential everywhere. `Grants` expands a spec's `mcp:<provider>[:write]` group grant (read only by default) per tenant, on sessions kept per provider record version, for `runtime.WithToolGroupSource`. A person-scoped provider is expanded for the actor on the turn's context with that person's own credential (`PersonResolver`), on a session per person; a person with none gets `ErrNoPersonCredential` and a note for the model, never the shared credential. Such a provider's tools report `ActsForPerson` (`agentrt.PersonScoped`), so an approval of their calls says so. |
| `pkg/knowledge` | Hybrid retrieval over indexed documents: dense plus lexical, fused, with citations. `Memory` and `Postgres`, `knowledgetest`. |
| `pkg/connectors/author` | The registry as agent tools (read, propose, apply), prompt review, system scaffolding, and the seeded `author` agent. |
| `pkg/bus` | `Bus` interface; `Memory`, Redis Streams and Postgres backends; `bustest` conformance suite. The Postgres bus stores every event body in `fiber_bus_events` until it is trimmed or `WithIndexTTL` expires. |
| `pkg/router` | Delivery fan-out to sinks, ordered per `(run, target)`. |
| `pkg/agentrt` | Builds a saige agent from a pinned revision; delta translation; citation extraction. A granted tool it cannot resolve is dropped and announced to the model by name (`WithStrictTools` refuses instead), with the error's `ModelNote` when it has one. `ResolveTools` resolves a revision's grants exactly as `New` does and builds nothing else (no model client). `payload/` holds typed event bodies. |
| `pkg/runtime` | The durable anchor turn: revision pinning, playscape sandbox, dual publish, stopping a run mid-turn, compacting it on request, per-run usage, and parking a run on a person. Every turn's context carries the actor on the run's record (`envelope.ActorFrom`). `Sandbox.ToolsFor` assembles a turn's tools as `Execute` does and runs nothing. |
| `pkg/connectors` | `slack`, `github`, `jira`, `cron`, `webhook`, plus shared helpers under `internal/`. |
| `fiber` (module root) | The composition root. `New(ctx, ...Option)` builds an `App` (`app.go`). Every backend is a constructed value passed by an option (`WithPostgres(pool)` for every store family, `WithBus`, `WithRegistry`, `WithDurable`, `WithThreads`, `WithKnowledge`, `WithBlobs`, `WithToolProviders`, `WithExecutor`); anything not given is memory. Extensions are options too: model providers, tool sources, connectors, MCP tool providers and each person's credentials for them (`WithPersonCredentials`) (`fiber.go`, `extensions.go`). `App` runs delivery and execution in process and exposes webhook connectors as `http.Handler`s; it listens on nothing and reads no file or `FIBER_*` variable. `App.ToolsFor` reports what a run of a tag would be granted without running it. It is the module root because it composes `pkg/` and nothing under `pkg/` may depend on it. |
| `cmd/fiber` | `fiber run [flags] SPEC MESSAGE`: registers one JSON spec on a `fiber.New` with memory backends and a temporary workspace, raises one tag, prints the run's events (text, or `-json` one per line) and exits 0 completed, 1 not, 2 bad input. `-offline`, `-provider`, `-model`, `-tools` (prints `App.ToolsFor`, runs nothing), `-timeout`. Nothing imports it. |

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
| validate a spec | `go run ./cmd/fiber run -offline SPEC MESSAGE`, `-tools` to list its grants |
| backends | `make up` (Postgres with pgvector, Redis), `make down` |
| suites on real backends | `make integration` (sets `FIBER_POSTGRES_TEST_URL` and `FIBER_REDIS_ADDR`) |

## Code Style

- Idiomatic Go, stdlib-first; every dependency must earn its place.
- Package doc comments explain the concern, its orthogonality boundary, and the
  why. `pkg/topic/topic.go` and `pkg/signature/signature.go` set the bar.
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
- **Preserve the DAG.** Leaves (`topic`, `address`, `envelope`, `signature`,
  `attachment`) stay stdlib-only. Nothing under `pkg/` imports `internal/`.
- **History pinning is load-bearing too.** A run pins "thread history up to
  sequence N" at accept, next to its revision, and every attempt loads exactly
  that. The journal only appends, and what it replays is byte-identical from
  run to run, which is what keeps a long thread's prompt cache warm.
- **Volatile content goes last.** Tools and system prompt, then history, then
  the request, then its files. Nothing that changes per turn (timestamps, run
  ids, retrieved passages, attachments) may sit ahead of something that does
  not.
- **Revision pinning is load-bearing.** A run pins its revision at accept time
  and must never re-resolve "latest" inside a workflow body: anchor replays
  from the top, and an unpinned run diverges. Tests that assert this exist in
  `pkg/runtime`; do not weaken them.
- **Durability comes from anchor, never from playscape.** playscape's queue is
  at-most-once in beta.
- **A run's topic has one writer.** Only the attempt executing a run publishes
  to `agent:<name>:<run>`, with sequence numbers it chose. Anything addressed
  to a run, such as a stop request, goes to its control topic instead.
- **A stop is recorded before it is announced.** `Runtime.Stop` writes the
  request on the run in the ledger and only then publishes it; the bus is the
  fast path, never the only copy. Hosts pass `runtime.WithStopLedger`.
- **A stopped run is a result, not an error.** The agent activity returns a
  stopped outcome and the workflow body decides from that record alone, so a
  replay announces the same stop. Never read "has a stop been requested" in
  the body.
- **Nothing about a particular stop reaches the thread journal.** A stopped
  turn is journaled as its partial answer plus `runtime.StopMarker`, a
  constant. The reason and the asker go on `lifecycle.stopped`.
- **Usage is counted from what an attempt relayed, once.** A run's total is
  built from the `usage.call` and `usage.child` events its attempts published,
  keyed by fiber's attempt-unique `accounting_id`, inside the memoized agent
  activity. Every attempt records its calls on the run before it returns,
  success or failure; the workflow body never reads usage. Cost comes only
  from saige's price catalog, as integer micro-units, never converted or
  estimated. Usage is opt-in for routes and chat sinks do not render it.
- **A parked run decides from its records.** A question is a anchor step the
  body records once; its resolution is the write-once event record
  `event:fiber.answer.<id>#0`, written by `Runtime.Answer`, by a stop, or by
  an expiry, and whichever was first is the answer. The body never reads the
  clock or a request to decide; it reads that record. An attempt runs at most
  one turn: the attempt that asked parks, and the answer wakes the next.
- **An approval is a second pair of eyes.** `Runtime.Answer` refuses an
  approval from the requester, or from the credential that raised the run,
  unless the pinned spec sets `self_approval`, and admits only the spec's
  `approvers`/`answerers` when it names them. Never widen this to tenant-wide.
  A call that runs with the credential of the person the run acts for
  (`agentrt.PersonScoped`, `QuestionTool.ActsForPerson`) is approved with
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
  program that accepts tags from callers (Humagen) sets the tenant from the
  caller's credential and overwrites whatever the client sent.
  `App.Invoke` and `App.ToolsFor` trust the tag they are handed.
- **Neither is the actor.** A tag's actor is the credential's subject unless
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
  accepted tag's actor on the turn's context and the sandbox carries it to the
  task in process; it is never read from a `Request`, the task input, trigger
  metadata or anything a model produced, and a turn for nobody acts for
  nobody.
- **The command is a program on the library.** `cmd/fiber` composes
  `fiber.New` through exported API only, reads no file but the spec it is
  given and no environment variable of its own (the model providers read
  their vendors' keys, as in any program), listens on nothing and keeps
  nothing. What it needs from fiber is library API that any program could
  call, as `App.ToolsFor` is; it never reaches past that, and nothing in the
  module imports it.
- This is a v0 alpha: keep the surface small and document limitations honestly
  rather than papering over them.

## Extension Guide

- **New connector**: implement `connector.Connector` plus whichever of
  `Trigger`, `Sink`, `Actor` apply, then register it. Roles are derived by type
  assertion, so implementing a face is how you advertise it. Reuse the shared
  helpers in `pkg/connectors/internal/` for mention parsing, idempotent sink
  rendering and HTTP plumbing rather than reimplementing them.
- **New bus backend**: implement `bus.Bus` and run it through
  `pkg/bus/bustest.Run`, which holds every backend to the identical contract.
- **New store backend**: implement the package's `Store` and run it through
  its suite (`registrytest`, `knowledgetest`, `threadtest`,
  `toolprovidertest`, `blobtest`), as `Memory` and `Postgres` do. Wire it in
  `app.go` behind an option, or let callers pass it to the existing one.
- **New agent provider**: an OpenAI-compatible endpoint is
  `fiber.WithOpenAICompatible`. Anything else outside this repo is a
  `fiber.WithModelProvider` in the composing program; see
  `examples/downstream`. A built-in belongs in `pkg/agentrt`'s provider
  construction. Keep `agenttest.ScriptedProvider` working, since it is what
  makes the example and the tests run with no API key.
- **Downstream extensions**: tool sources, connectors and MCP tool providers
  are `fiber` options too. Each option fills a field of `options`
  (`extensions.go`), so a new extension point is a field there, its wiring in
  `New`, and an option. No option reads a file or the environment.
- **New event kind**: add it to `pkg/envelope`, give it a typed body in
  `pkg/agentrt/payload`, and teach the connector renderers about it. Kinds are
  fields, never topics. A kind that ends a run must also be taught to
  everything that waits for a run to end: `render.Status.Terminal`, and the
  ledger status mapping in `status.go` (`Status.Terminal`).
