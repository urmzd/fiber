# fiber

Agent platform and low-latency pub/sub bus. Tag an agent from anywhere, stream
its events to anyone, deliver them anywhere. Go module
`github.com/urmzd/fiber`, binary `fiber`.

**Status: v0 alpha, pre-1.0, not recommended for production.** Interfaces are
still moving and may change between minor versions. The single-binary deployment
is complete and tested; the Redis bus backend is conformance-tested but not
exercised in CI. The Postgres registry, run ledger (`pkg/durable`) and
knowledge store run in CI's integration job and in `make e2e`. Keep the surface small and document limitations honestly.

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
| `pkg/envelope` | Wire contract: `Tag`, `Event`, `Kind`, `Route`, `Filter`, `Subscription`. Leaf. |
| `pkg/signature` | HMAC verification for inbound webhooks (Slack v0, GitHub sha256). Leaf. Security boundary. |
| `pkg/agentspec` | Immutable agent specs, content-addressed revisions. Leaf. |
| `pkg/connector` | `Trigger` / `Sink` / `Actor` faces, derived by `RolesOf`, plus the registry. |
| `pkg/registry` | Agent store and revision history. `Memory` and `Postgres` implementations, plus the adapter to `internal/server.Store`. |
| `pkg/durable` | The Postgres schema for anchor's ledger and queue adapters, and `Open`, which builds both on it. |
| `pkg/thread` | The per-thread conversation journal a run continues: append-only, pinned at accept, with a reconciliation seam for surfaces. `Memory` and `Postgres`. |
| `pkg/blob` | Content-addressed, tenant-scoped file store. `Memory` and filesystem. |
| `pkg/attachment` | The file reference a tag carries. Leaf. `intake/` stores and fetches files (SSRF-guarded); `resolve/` turns a file into model input by kind. |
| `pkg/toolprovider` | MCP tool providers as versioned records: how to reach a server, its credential reference, and the admin-accepted, numbered tool snapshots. Changed tools stay pending; read/write class defaults to write. `Memory` and `Postgres`. |
| `pkg/mcptools` | MCP client (stdio, streamable HTTP) over `toolprovider`: lists and diffs a server's tools, exposes only accepted ones as saige tools, enforces the grant's class per call, and redacts the resolved credential everywhere. |
| `pkg/knowledge` | Hybrid retrieval over indexed documents: dense plus lexical, fused, with citations. `Memory` and `Postgres`. |
| `pkg/connectors/author` | The registry as agent tools (read, propose, apply), prompt review, system scaffolding, and the seeded `author` agent. |
| `pkg/bus` | `Bus` interface, `Memory` and Redis Streams backends, `bustest` conformance suite. |
| `pkg/router` | Delivery fan-out to sinks, ordered per `(run, target)`. |
| `pkg/agentrt` | Builds a saige agent from a pinned revision; delta translation; citation extraction. Providers: `anthropic`, `ollama`, `offline`, and `openai`, which `WithOpenAICompatible` points at any Chat Completions endpoint by base URL and key variable name. `payload/` holds typed event bodies. |
| `pkg/runtime` | The durable anchor turn: revision pinning, playscape sandbox, dual publish, stopping a run mid-turn, compacting it on request, per-run usage, and parking a run on a person. |
| `pkg/connectors` | `slack`, `github`, `jira`, `cron`, `webhook`, plus shared helpers under `internal/`. |
| `internal/server` | Connect handlers, SSE, webhook ingress, tenant interceptor. |
| `internal/cli` | Command tree. `core.go` is the composition root. |
| `gen/`, `proto/` | The protobuf contract and its generated code. `gen/` is committed. |

Discover layout with `tree` or ripgrep; do not trust stale listings.

## Commands

| Action | Command |
|--------|---------|
| init | `make init` |
| build | `make build` |
| test | `go test ./...` |
| race test | `go test ./... -race` |
| lint | `golangci-lint run` |
| fmt | `gofmt -w .` |
| quality gate | `make check` |
| run server | `make run` (or `go run ./cmd/fiber serve`) |
| regenerate proto | `make proto` (needs `buf` on PATH) |
| end-to-end demo | `go run ./examples/local` |
| compose stack | `make up` (Postgres, Redis, fiber), `make down` |
| compose end-to-end | `make e2e` |
| registry on Postgres | `FIBER_POSTGRES_TEST_URL=postgres://... go test ./pkg/registry/` |

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
  `agentspec`, `attachment`) stay stdlib-only. Nothing under `pkg/` imports `internal/`.
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
  `event:mandatum.answer.<id>#0`, written by `Runtime.Answer`, by a stop, or by
  an expiry, and whichever was first is the answer. The body never reads the
  clock or a request to decide; it reads that record. An attempt runs at most
  one turn: the attempt that asked parks, and the answer wakes the next.
- **An approval is a second pair of eyes.** `Runtime.Answer` refuses an
  approval from the requester, or from the credential that raised the run,
  unless the pinned spec sets `self_approval`, and admits only the spec's
  `approvers`/`answerers` when it names them. Never widen this to tenant-wide.
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
- **Tenant is never read from a request body.** The server sets it from the
  caller's credential and overwrites whatever the client sent.
- **Neither is the actor.** A tag's actor is the credential's subject unless
  the credential is marked `ActAs`, which is what a surface that authenticated
  the person itself holds. Authorship and approvals are decided on it.
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
- **New registry backend**: implement `registry.Store`; the adapter to the
  server's interface already exists. Make `newStore` in
  `pkg/registry/memory_test.go` able to build it, so it runs the same suite
  `Memory` and `Postgres` do.
- **New agent provider**: extend `pkg/agentrt`'s provider construction. Keep
  `agenttest.ScriptedProvider` working, since it is what makes the example and
  the tests run with no API key.
- **New event kind**: add it to `pkg/envelope`, give it a typed body in
  `pkg/agentrt/payload`, and teach the connector renderers about it. Kinds are
  fields, never topics. A kind that ends a run must also be taught to
  everything that waits for a run to end: `render.Status.Terminal`, the
  server's `terminalKind`, the CLI's `terminal`, and the ledger status mapping
  in `internal/cli/core.go`.
