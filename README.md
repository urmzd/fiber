<p align="center">
  <h1 align="center">fiber</h1>
  <p align="center">
    Define agents, run them durably, and stream answers across your tools.
    <br /><br />
    <a href="https://github.com/urmzd/fiber/issues">Report Bug</a>
    &middot;
    <a href="https://pkg.go.dev/github.com/urmzd/fiber">Go Docs</a>
  </p>
</p>

<p align="center">
  <a href="https://github.com/urmzd/fiber/releases"><img src="https://img.shields.io/github/v/release/urmzd/fiber" alt="Release"></a>
  &nbsp;
  <a href="https://github.com/urmzd/fiber/actions/workflows/ci.yml"><img src="https://github.com/urmzd/fiber/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  &nbsp;
  <a href="https://pkg.go.dev/github.com/urmzd/fiber"><img src="https://pkg.go.dev/badge/github.com/urmzd/fiber.svg" alt="Go Reference"></a>
  &nbsp;
  <a href="LICENSE"><img src="https://img.shields.io/github/license/urmzd/fiber" alt="License"></a>
</p>

<p align="center">
  <img src="showcase/durable-replay.png" alt="Fiber's offline demo: a replayed run completes without repeating the model call, with events delivered to Slack and a webhook" width="80%">
</p>
<p align="center"><em>Real output from the offline example. The model and destination services are simulated.</em></p>

> ## Status
>
> **fiber is v0 alpha software. It is pre-1.0 and not recommended for
> production.**
>
> Interfaces are still moving and may change between minor versions, and a minor
> release may require code changes on your side.
>
> What that means concretely today:
>
> - **Works and is tested.** The in-process composition (`fiber.New`) on the
>   in-memory backends, the topic and delivery model, durable runs with
>   replay, and the Slack, GitHub, Jira, cron and webhook connectors.
> - **New and lightly exercised.** The Postgres backends for every store and
>   for the bus, the knowledge layer, the built-in `author` agent, and history
>   compaction. Each backend passes the same conformance suite as its
>   in-memory counterpart in CI's integration job; none of it has seen
>   production load.
>
> See [Limitations](#limitations) for the full list.

fiber is a Go library for agents and a low-latency pub/sub bus. Define an agent
with a spec, tag it from Slack, GitHub, Jira or a schedule, and its events
stream to any subscriber while being delivered to any destination you name.
It is a library you compose in Go: there is no binary, no service, no
configuration file and no environment variable. `fiber.New` composes the
packages under `pkg/`, every backend is a constructed value passed as an
option, and anything not passed is the in-memory implementation.
[warp](https://github.com/urmzd/warp) is the platform built on it.

**Where a tag is raised and where its answer lands are independent.** A GitHub
request can stream into Slack while a webhook receives only its completion
event. Each run pins an agent revision, so retries use the same definition.

## Features

- **Agents are documents.** Create one by submitting a spec. No redeploy, no code, no restart.
- **Subscribe once.** `agent:docs-bot` receives current and future runs of that agent through one topic.
- **Route by event.** Send one run to multiple destinations; each chooses text, citations, lifecycle events, or all of them.
- **Connectors act, not just talk.** The `Actor` face contributes native verbs (transition a Jira ticket, request a review) as agent tools, sandboxed by NGAC, each emitting an auditable `action.taken` event.
- **Backends are values.** `fiber.WithPostgres(pool)` puts the registry, run ledger and queue, knowledge, thread journals and tool providers on one pool; `fiber.WithBus` takes the in-memory, Redis Streams or Postgres bus. Anything not given is memory, which is a complete implementation, not a stub.
- **Revisions are pinned per run.** A run accepted under revision 6 executes as revision 6 forever, even after a revise, because replay demands it.
- **Citations are first-class events**, carrying the exact quoted source and resolvable provenance.
- **Every run reports what it spent.** Each model call is a `usage.call` event (tokens, cache share, model, context window, and cost where saige's price catalog has the model), and the run's total, delegations included and each call counted once, is on its terminal event and `App.Status`.
- **A run can wait for a person.** A tool listed in a spec's `approvals`, or the built-in `ask_person` tool, parks the run on a question (approve or deny a call, pick an option, or reply). The run frees its worker, survives restarts, publishes the question on `lifecycle.parked`, and resumes with the first answer (`App.Answer`). An unanswered question takes its default when it expires.

## Installation

```sh
go get github.com/urmzd/fiber
```

Requires the Go version in [`go.mod`](go.mod). Real inference needs provider
credentials, a local Ollama, or an OpenAI-compatible server. The Postgres and
Redis backends need those services (`make up` starts both with Docker).

## Quick Start

Run the complete offline example from a checkout. After Go downloads its
dependencies, the demo needs no API key or external services:

```sh
git clone https://github.com/urmzd/fiber.git
cd fiber
go run ./examples/local
```

It creates an agent, revises it, tags it from a simulated GitHub trigger with
delivery routed to Slack and a webhook, shows a third party listening to the
agent's topic, and demonstrates that a replayed run does not pay for the model
twice.

### Compose it

```go
pool, err := pgxpool.New(ctx, dsn) // your pool, from your configuration
pg := bus.NewPostgres(pool)        // or bus.NewRedis(client), or omit for memory
if err := pg.Migrate(ctx); err != nil {
    return err
}
app, err := fiber.New(ctx,
    fiber.WithTenant("acme"),
    fiber.WithPostgres(pool), // every store family, migrated by New
    fiber.WithBus(pg),
)
if err != nil {
    return err
}
defer app.Close()
go app.Run(ctx) // delivery and execution, in this process

app.Registry().Create(ctx, "acme", agentspec.AgentSpec{
    Name: "docs-bot", Provider: "anthropic", Model: "claude-haiku-5-5",
    SystemPrompt: "You answer questions about the deploy pipeline. Cite your sources.",
}, "ada")
acc, err := app.Invoke(ctx, envelope.Tag{
    ID: "req-1", Tenant: "acme", Agent: "docs-bot", Origin: "api",
    Text: "how does a release get out the door?",
    Deliver: []envelope.Route{{Target: address.MustParse("webhook://acme/deploys"),
        Kinds: []string{"lifecycle.completed"}}},
})
```

`App.Bus().Subscribe` streams any topic (`agent:docs-bot` sees every run of
that agent); `App.Status`, `Stop`, `Compact` and `Answer` act on a run;
`App.WebhookHandler()` is an `http.Handler` for the connectors that receive
webhooks, for your program to mount.
[`examples/downstream`](examples/downstream/main.go) is a complete program.

### Use a model

Set `ANTHROPIC_API_KEY` for `"provider": "anthropic"`. To use OpenAI, or any server that speaks its Chat Completions API (vLLM,
LM Studio, llama.cpp, OpenRouter, Groq), set `"provider": "openai"` and the
model that server hosts. For a local server, point fiber at it instead of
setting a key:

```sh
export OPENAI_BASE_URL="http://localhost:8000/v1"   # omit for OpenAI itself
export OPENAI_API_KEY="your-api-key"                # optional for a local server
```

Models saige's catalog does not know report their token usage and no cost.

An agent can name fallback models, tried in order when a call to its primary
model fails with an error another model could fix (an outage, a rate limit, a
refused credential):

```json
"fallbacks": [
  {"provider": "openai", "model": "gpt-5"},
  {"provider": "ollama", "model": "llama4"}
]
```

Each fallback names a provider the deployment knows and a model, none repeats
the primary or another fallback, and there are at most four. By default any
error another model could fix falls through. `"fallback_on"` narrows that to
named classes: `unavailable` (an outage), `rate_limit`, `transient` (a timeout
or reset connection, and also a rate limit or outage), `auth`,
`context_length` and `content_filter`, or `any` alone, which is the default.
For example `"fallback_on": ["rate_limit", "unavailable"]`. It is a set, and
naming it without fallbacks is refused. Whatever it names, an invalid
request, a stopped run or an exhausted budget does not fall through, and a
stream that already produced output is not replayed on another model. Every
fallback is built with the agent, so one whose provider is not configured
(say, `OPENAI_API_KEY` is unset) fails the build like the primary would. Each
call is priced on the rate card of the model that answered it. Order is part
of the revision: reordering fallbacks is an edit. A spec without fallbacks
hashes exactly as it did before they existed.

An agent can be granted an MCP provider's tools as one name:
`"tools": ["mcp:urmzd"]` grants the tools an admin accepted on the provider
`urmzd` as read, and `"mcp:urmzd:write"` its write tools as well. Read only is
the default, so leaving the class off can only grant less, and the class is
checked again on every call. The model is shown each member tool
(`urmzd__create_issue`), never the group, and approvals name members:
`"approvals": ["urmzd__create_issue"]`. The group is expanded for the run's
tenant each time a run builds the agent, so a newly accepted tool reaches the
agent without a new revision; tools that are not accepted, or that changed
since, are never offered. An App expands groups against its tool
provider store (Postgres with `fiber.WithPostgres`) and resolves
`env:NAME` credential references from its environment. A host embedding
`pkg/runtime` does the same with `runtime.WithToolGroupSource` and an
`mcptools.Grants`; one embedding `pkg/agentrt` alone supplies groups with
`agentrt.WithToolGroups` or from its tool catalog.

A provider is reached with one shared credential, its `CredentialRef`, unless
it is person-scoped (`CredentialScope: toolprovider.ScopePerson`): then each
run reaches it with the credential of the person the run acts for, the actor
on its tag, which `fiber.WithPersonCredentials` looks up. That actor is the
one recorded when the run was accepted, so nothing a model says can change
it. Each person gets a session of their own, opened with their own
credential. A person with none gets no tools from the provider, the model is
told they have not connected one, and the operator log names them; a shared
credential is never used instead, and a person-scoped provider cannot even
name one.

```go
fiber.WithPersonCredentials(mcptools.PersonResolverFunc(
    func(ctx context.Context, tenant string, p toolprovider.Provider, who envelope.Actor) (mcptools.PersonCredential, error) {
        tok, ok := vault.Token(ctx, tenant, who.User, p.Name) // your own store, keyed by your account id
        if !ok {
            return mcptools.PersonCredential{}, mcptools.ErrNoPersonCredential
        }
        return mcptools.PersonCredential{Secret: mcptools.NewSecret(tok)}, nil
    }))
```

## Examples

| Example | What it demonstrates |
|---------|----------------------|
| [`local`](examples/local/) | Offline agent revisions, selective delivery, citations, and replay, composed by `fiber.New` |
| [`downstream`](examples/downstream/) | A program composing fiber with its own model provider and tool |

See [the example index](examples/README.md) for more.

## Documentation

| Document | Covers |
|----------|--------|
| [docs/architecture/overview.md](docs/architecture/overview.md) | Topics, connector faces, N:M delivery, revision pinning, package DAG, limitations |
| [AGENTS.md](AGENTS.md) | AI-facing conventions, commands, rules, extension guide |
| [examples/README.md](examples/README.md) | Runnable example index |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development workflow and commit convention |
| [SECURITY.md](SECURITY.md) | Security boundaries and reporting |

API reference: [pkg.go.dev/github.com/urmzd/fiber](https://pkg.go.dev/github.com/urmzd/fiber)

## Where this sits

fiber is layer four of five: **warp > fiber > saige > playscape > anchor**.
It builds on [saige](https://github.com/urmzd/saige)'s agent loop,
[playscape](https://github.com/urmzd/playscape)'s sandbox and
[anchor](https://github.com/urmzd/anchor)'s durability, and adds addressing,
distribution, connectors and the control plane.
[warp](https://github.com/urmzd/warp) is the only thing that configures or
runs it: fiber is composed in Go and reads no configuration of its own. See
[the ecosystem guide](https://github.com/urmzd/warp/blob/main/docs/ecosystem.md)
for who owns what and the limitations that span the stack.

## Architecture

fiber builds on three libraries and adds addressing, distribution and the
control plane:

| Concern | Owner |
|---------|-------|
| Agent loop, typed deltas, RAG, citations | [saige](https://github.com/urmzd/saige) |
| Durability, replay, per-run journal | [anchor](https://github.com/urmzd/anchor) |
| Sandboxed execution nodes, NGAC | [playscape](https://github.com/urmzd/playscape) |
| Topics, bus, routing, connectors, specs | fiber |

See the [architecture guide](docs/architecture/overview.md) for delivery,
authorization, and replay guarantees.

## Composition

Every choice is an option to `fiber.New`. Configuration from files or the
environment belongs to the program composing fiber
([warp](https://github.com/urmzd/warp) is one), which turns it into these.

| Option | Chooses | Default |
|--------|---------|---------|
| `WithPostgres(pool)` | Registry, run ledger and queue, knowledge, threads, tool providers on one pool | memory |
| `WithBus(b)` | `bus.NewMemory`, `bus.NewRedis(client)`, `bus.NewPostgres(pool)` | memory |
| `WithRegistry`, `WithDurable(ledger, queue)`, `WithThreads`, `WithKnowledge`, `WithBlobs`, `WithToolProviders` | One store, built by the caller | memory, or the `WithPostgres` one |
| `WithEmbedder(e)` | The embedder of the knowledge store New builds | offline hashing |
| `WithExecutor`, `WithRuntimeOptions`, `WithWorkerOptions` | The sandbox, and options for the runtime and worker | playscape sandbox |
| `WithWorkspace(dir)` | Where the sandbox writes agent artifacts | a temporary directory |
| `WithTenant`, `WithConcurrency`, `WithCompaction`, `WithAuthor`, `WithLogger`, `WithMetrics` | Deployment | none, one, off, offline, discard, none |
| `WithSlack`, `WithGitHub`, `WithJira`, `WithWebhooks`, `WithSchedules` | Built-in connectors, typed Go values | off |
| `WithModelProvider`, `WithOpenAICompatible`, `WithTools`, `WithToolSource`, `WithConnector`, `WithToolProvider` | Extensions | none |
| `WithPersonCredentials` | Each person's own credential for person-scoped MCP providers | none, so those providers offer no tools |

Model providers read their own credentials when an agent is built:
`ANTHROPIC_API_KEY`, `OPENAI_API_KEY` and `OPENAI_BASE_URL`, `OLLAMA_HOST`,
and the variable an `OpenAICompatible.KeyEnv` names. An MCP provider's
`env:NAME` credential reference resolves the same way, as does a reference a
`WithPersonCredentials` resolver returns for a person. Connector setup is
covered in the [GitHub](docs/architecture/github.md) and
[Slack](docs/architecture/slack.md) guides.

## Extending fiber

`WithModelProvider`, `WithOpenAICompatible`, `WithTools`, `WithToolSource`,
`WithConnector` and `WithToolProvider` add to the composition:

```go
app, err := fiber.New(ctx,
    fiber.WithModelProvider("house", houseProvider),
    fiber.WithOpenAICompatible(agentrt.OpenAICompatible{
        Name: "fireworks", BaseURL: "https://api.fireworks.ai/inference/v1", KeyEnv: "FIREWORKS_API_KEY"}),
    fiber.WithTools(runbookTool{}),
    fiber.WithToolProvider(toolprovider.Provider{Name: "issues", Transport: toolprovider.TransportHTTP,
        Endpoint: "https://mcp.example.com/mcp", CredentialRef: "env:ISSUES_TOKEN"},
        toolprovider.Tool{Name: "search", SchemaHash: "4f1c…", Class: toolprovider.ClassRead}),
)
```

A declared MCP tool is accepted only while the server's schema for it
matches its `SchemaHash`, and its description may not change once accepted.
Anything the server adds or changes stays pending, so a restart never accepts
what the program does not declare. `fiber.New` logs every pending tool with
its schema hash; review it, then pin it. A server that cannot be reached at
startup is logged and its tools are unavailable to runs. A person-scoped
provider is not dialled at startup, since it has no credential of its own:
each tool declared for it carries the `Description` and `InputSchema` the
server offers, and is accepted in that form when its `SchemaHash` matches.
Review one with a credential of your own
(`mcptools.Connect(ctx, p, mcptools.WithPersonCredential(mine))`).

A new backend implements the package's interface and passes its conformance
suite: `bustest`, `registrytest`, `knowledgetest`, `threadtest`,
`toolprovidertest`, `blobtest`.

## Upgrading

**fiber is a library from this release.** The `fiber` binary, its commands,
the HTTP and Connect API and its protobuf contract, `registry.ServerStore`,
the providers file and every `FIBER_*` variable are gone. Compose
`fiber.New` with options instead: `FIBER_POSTGRES_URL` is
`WithPostgres(pool)`, `FIBER_REDIS_URL` is `WithBus(bus.NewRedis(client))`
(size the client with `bus.RedisPoolSize`), a providers file's entries are
`WithOpenAICompatible` and `WithToolProvider`, and the connector variables are
`WithSlack`, `WithGitHub`, `WithJira` and `WithWebhooks`. `GetRun` is
`App.Status`, the stop and compact routes are `App.Stop` and `App.Compact`,
`InvokeService.Answer` is `App.Answer`, and `fiber index` is `App.Index`.

**The binary's defaults are not the library's.** The binary served tenant
`default` unless `FIBER_TENANT` said otherwise, so the agents, threads and
runs it wrote live under that tenant: pass `WithTenant("default")` (or the
`FIBER_TENANT` you had) to keep them. `WithSlack`, `WithGitHub` and `WithJira`
are `ErrConfig` without a tenant. The binary also kept files on disk under
its workspace; the library keeps them in memory unless you pass
`WithBlobs(blob.NewFS(dir))`.

**Persisted names moved to `fiber`.** Postgres objects, Redis keys, anchor
activity and record names and the agent-spec hash domain no longer use the
old name. Drain runs before upgrading, including runs parked on a question,
which wait until they are answered, stopped or expire: a run still pending
from the old version is failed with a reason when the new one first claims
it, and is not resumed. Stop every old process before the first new one
starts. Postgres migrates itself and keeps its rows. See
[docs/renaming.md](docs/renaming.md).

## Limitations

- The bus is chosen independently of the stores: `WithPostgres` does not move the bus, `WithBus` does.
- The Postgres bus serializes publishes on one advisory lock per schema, so its throughput is one short transaction per event; Redis is the better bus for a chatty deployment.
- The Postgres bus stores every event it carries, body included, as a row in the `fiber_bus_events` table of the database it is given. A row stays until its run's retention trim drops it or a sweep finds it older than `bus.WithIndexTTL` (24 hours by default; the sweep runs from publishes, so with none nothing is removed). Those rows reach that database's WAL, replicas and backups like any other, and tool events carry their call's arguments. Where arguments can hold secrets, scrub them with `agentrt.WithToolArguments`, shorten the TTL, or use the Redis bus.
- Every store is in memory unless `WithPostgres` or its own option is given. On Postgres they survive a restart, but the run queue fiber builds has no `DeltaLog`, so anchor's per-run delta journal is not written; the bus still carries every event.
- Files (`blob`) are in memory unless `WithBlobs(blob.NewFS(dir))` puts them on disk; there is no Postgres blob store.
- fiber authenticates nothing. An embedding program that accepts tags or stop and answer requests from callers sets the tenant and actor from the caller's credential.
- playscape's queue is at-most-once in beta, so durability comes from anchor rather than from playscape.
- Citations are derived from RAG context blocks. saige's `CitationDelta`, which reports what a model or tool cited, is not translated into events yet.
- The bus is at-least-once with no consumer groups. If durable competing consumers become a requirement, NATS JetStream behind the `Bus` interface is a better answer than growing a broker here.
- Agent specs load from JSON, not YAML.
- `App.Status` reports a stopped run as `stopped`. A stop is recorded in the run ledger before it is announced on the bus, so once it is acknowledged the run ends stopped unless its turn had already returned its answer. A route that selects only `lifecycle.completed` is not told about a stop.
- A request to compact a running turn (`App.Compact`) is not recorded: it reaches only the attempt running when it is made, before that turn's next model call. See [Compacting a run on request](docs/architecture/overview.md#compacting-a-run-on-request).
- A program declares MCP tool providers with `fiber.WithToolProvider`, or writes them to the tool provider store (`App.ToolProviders`). A group grant that cannot be expanded (no such provider for the tenant, the server unreachable, nothing the grant's class admits) is dropped like a single unavailable tool: the run continues without it, the model is told it is unavailable, the operator is logged why, and `Runner.Unavailable` lists it. `agentrt.WithStrictTools` fails the build with `agentrt.ErrNoSuchToolGroup` instead.
- A person-scoped MCP provider's tools are expanded per person, so two people in one thread can be offered different tools, and the thread's prompt cache misses when the person changes. At most 128 person sessions are kept open (`mcptools.WithPersonSessions` on a hand-built `Grants`); past that the one used longest ago is closed and a turn still holding its tools sees a tool error. A stdio provider runs a server per open person session. The resolver is asked on every turn, and a new value behind an unchanged reference is not noticed until the reference, the secret or the provider record changes.
- Only MCP tool providers take a person's credential. The Slack, GitHub and Jira connectors' actions and sinks still act with the credential the deployment configured for them.
- A granted tool this deployment cannot resolve (no connector contributes it, or an MCP tool is held back) is dropped rather than failing the run. The model is told it is unavailable and the server logs a warning naming the tool and the reason; `agentrt.WithStrictTools` restores the old refusal.
- Tool events carry the call's arguments, capped at 8 KiB; scrub sensitive ones with `agentrt.WithToolArguments`.
- A parked run waits on a person through the run ledger. An answer or a stop wakes it at once; an expired question is noticed by the worker's janitor poll (30 seconds by default), so its default applies up to that late. The requester cannot approve their own calls unless the spec sets `self_approval`, and `approvers`/`answerers` restrict who may answer. A tool implementing `agentrt.Approving` decides per call whether it needs approval and may name that call's approvers, which then replace the spec's. A host Delegator that blocks on a child run holds a worker and can exhaust them; `runtime.WithChildRuns` parks the parent on the child instead. Approvals inside an in-turn sub-task (a `tag_<agent>` call without child runs or a Delegator) are refused, not asked. See [Parking a run on a person](docs/architecture/overview.md#parking-a-run-on-a-person).
- Cost is reported only for models saige's price catalog prices, in integer micro-units with the catalog's `as_of`; anything else reports tokens and no cost. Usage is delivered only to routes that select `usage`. A turn that crashes mid-call is on the bus but not in the run's total. In-turn delegation is one level deep (a sub-task holds no spawn grant), and a host that runs delegations as runs of their own rolls them up with `runtime.ReportChildUsage`. See [Usage](docs/architecture/overview.md#usage).

## Reproduce the Screenshot

Install [teasr](https://github.com/urmzd/teasr), then run from this checkout:

```sh
go run ./examples/local
teasr run --config teasr.toml
```

The first run warms Go's build cache. The capture runs the real demo and writes
[`showcase/durable-replay.png`](showcase/durable-replay.png).

## License

Apache 2.0. See [LICENSE](LICENSE).
