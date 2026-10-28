<p align="center">
  <h1 align="center">fiber</h1>
  <p align="center">
    Define agents, run them durably, and stream answers across your tools.
    <br /><br />
    <a href="#installation">Install</a>
    &middot;
    <a href="https://github.com/urmzd/fiber/issues">Report Bug</a>
    &middot;
    <a href="https://pkg.go.dev/github.com/urmzd/fiber">Go Docs</a>
  </p>
</p>

<p align="center">
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

fiber is a Go library for agents and a low-latency pub/sub bus. Define an agent
with a spec, tag it from Slack, GitHub, Jira or a schedule, and its events
stream to any subscriber while being delivered to any destination you name.
You compose it in Go: there is no service, no configuration file and no
environment variable. Every backend is a value passed to `fiber.New`, and
anything you do not pass is the in-memory implementation.

**Where a tag is raised and where its answer lands are independent.** A GitHub
request can stream into Slack while a webhook receives only its completion
event. Each run pins an agent revision, so retries use the same definition.

## Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Examples](#examples)
- [Usage](#usage)
- [Configuration](#configuration)
- [API](#api)
- [Development](#development)
- [Documentation](#documentation)
- [License](#license)

## Features

- **Agents are documents.** Create one by submitting a spec. No redeploy, no code, no restart. A run accepted under revision 6 executes as revision 6 forever, even after a revise.
- **Subscribe by channel and type.** A run publishes on the channel of the event that started it. Every sink subscribed to that channel receives it, and each picks text, citations, lifecycle events, or all of them by type prefix.
- **Connectors act, not just talk.** Connectors contribute native verbs (transition a Jira ticket, request a review) as sandboxed agent tools, each emitting an auditable `action.taken` event.
- **Backends are values.** `fiber.WithPostgres(pool)` puts every store on one pool; `fiber.WithBus` takes the in-memory, Redis Streams or Postgres bus. Memory is a complete implementation, not a stub.
- **Citations are first-class events**, carrying the exact quoted source and resolvable provenance.
- **Every run reports what it spent.** Each model call is a `usage.call` event (tokens, cache share, model, context window, and cost for priced models), and the run's total is on its terminal event and `App.Status`.
- **A run can wait for a person.** A tool listed in a spec's `approvals`, or the built-in `ask_person` tool, parks the run on a question. The run frees its worker, survives restarts, and resumes with the first answer (`App.Answer`) or takes its default when the question expires.

**Early development:** v0 alpha, pre-1.0, not recommended for production. Interfaces may change between minor versions. The in-memory composition, durable replay and the Slack, GitHub, Jira, cron and webhook connectors are tested; the Postgres backends, knowledge layer, built-in `author` agent and history compaction pass CI's conformance suites but have not seen production load. See [Limitations](#limitations).

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

It creates an agent, revises it, asks it from a simulated GitHub event whose
channel a Slack sink and a webhook subscribe to, shows a third party listening
to the channels, and demonstrates that a replayed run does not pay for the model
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

app.Definitions().Put(ctx, definition.Record{Tenant: "acme", CreatedBy: "ada", Content: `---
apiVersion: saige/v1
name: docs-bot
version: 1.0.0
model:
  use: anthropic/claude-haiku-5-5
---
You answer questions about the deploy pipeline. Cite your sources.
`})
// docs-bot answers what is posted on its channel.
app.Memberships().Join(ctx, "acme", "slack://T01/C02", "docs-bot", membership.Rule{Types: []string{"message"}})
err = app.Publish(ctx, event.Envelope{
    ID: "req-1", Tenant: "acme", Time: time.Now(),
    Source: "slack://T01/C02", Channel: "slack://T01/C02", Type: event.TypeMessagePosted,
    Data: event.MessagePosted{Request: event.Request{
        Text: "how does a release get out the door?", Mentions: []string{"docs-bot"},
    }},
})
```

[`examples/downstream`](examples/downstream/main.go) is a complete program.

## Examples

| Example | Description |
|---------|-------------|
| [`local`](examples/local/) | Offline agent revisions, selective delivery, citations, and replay, composed by `fiber.New` |
| [`downstream`](examples/downstream/) | A program composing fiber with its own model provider and tool |

See [`examples/`](examples/README.md) for all runnable examples.

## Usage

### Use a model

Set `ANTHROPIC_API_KEY` for `"provider": "anthropic"`. To use OpenAI, or any
server that speaks its Chat Completions API (vLLM, LM Studio, llama.cpp,
OpenRouter, Groq), set `"provider": "openai"` and the model that server hosts.
For a local server, point fiber at it instead of setting a key:

```sh
export OPENAI_BASE_URL="http://localhost:8000/v1"   # omit for OpenAI itself
export OPENAI_API_KEY="your-api-key"                # optional for a local server
```

Models the price catalog does not know report their token usage and no cost.

### Fall back to other models

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

### Grant MCP tools

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

### Validate a spec locally

`fiber run` runs one agent spec once, in process, and prints what the run
does. It composes `fiber.New` with the in-memory backends and a temporary
workspace, registers the spec, raises one tag with your message, and prints
the run's events as they stream. It listens on nothing, reads no
configuration file and no environment variable of its own, and keeps nothing
once it exits.

Releases publish the Go module, not a prebuilt binary. Install the command at
the version of fiber you build against, or run it from a checkout with
`go run ./cmd/fiber`:

```sh
go install github.com/urmzd/fiber/cmd/fiber@latest
fiber run -offline docs-bot.json "how does a release get out the door?"
```

```text
[lifecycle.accepted] docs-bot revision 1, run r22e035b6d38cc721ba356a8a0290f805
[lifecycle.started] attempt 1
docs-bot is running with the offline provider, so no model was called. ...
[usage.call] MODEL: 0 tokens in (0 cached), 0 out
[lifecycle.completed] attempt 1, 1 model calls, 0 tokens in, 0 out
```

Each line is one event under its kind, the name an old webhook `kinds` selector names,
except the answer and any reasoning, which stream inline as they arrive.
Warnings from the operator log, such as a granted tool this composition
cannot resolve, go to standard error.

| Flag | Does |
|------|------|
| `-offline` | Runs on the offline provider: no model is called and no key is needed. The spec's fallbacks are dropped with its provider. |
| `-provider NAME`, `-model NAME` | Replace the spec's provider and model. A real provider reads its vendor's variables when the agent is built (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY` and `OPENAI_BASE_URL`, `OLLAMA_HOST`). |
| `-tools` | Lists the tools the agent would be granted, marks those whose calls wait for approval, and names each grant left out and why, then exits without running anything. It needs no key and no MESSAGE. |
| `-json` | Prints one JSON object per event, per line: the event as the bus carries it, with its payload as JSON. With `-tools`, the listing as one object. |
| `-timeout D` | Gives up after `D`, such as `30s` (default `5m`; `0` waits forever). |

Flags go before DEFINITION. DEFINITION is an `.agent.md` file (a saige
definition: YAML frontmatter, then the system prompt), or `-` for standard
input. `-policy FILE` reads the agent's policy sidecar from JSON; without it
the policy is the zero value, which grants nothing. A policy field that does
not exist is an error rather than ignored.

| Exit status | Means |
|-------------|-------|
| `0` | The run completed, or the listing was printed. |
| `1` | The run did not complete: it failed, was stopped, parked on a question, or ran out of time. A failed attempt is retried as in any deployment, and the run counts as failed only once the run ledger says so. |
| `2` | The command line or the spec is wrong, and nothing ran. |

A local run has what this composition has: the built-in providers, the
`ask_person` tool, the `author_*` tools every App carries, and a
`tag_<agent>` tool per agent the spec may spawn (only the one spec is
registered, so a delegation finds no agent to tag). No connector and no MCP
tool provider is configured, so a connector's actions (`jira_transition`) and
`mcp:` grants are unavailable here: the run goes ahead without them, as a
deployment's would, and `-tools` says which and why. Nothing here can answer
a question, so a run that parks on an approval or on `ask_person` exits `1`.
The knowledge store is empty, so `sources` retrieve nothing. To list what a
run would be granted in your own composition, call `App.ToolsFor(ctx, tag)`
there.

### Extend fiber

Add your own model providers, tools, connectors and MCP tool providers as
options to `fiber.New`:

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

To add a backend, implement the package's interface and pass its conformance
suite: `bustest`, `registrytest`, `knowledgetest`, `threadtest`,
`toolprovidertest`, `blobtest`.

### Upgrade from the server binary

**fiber is a library from this release.** The `fiber` server binary, its
commands, the HTTP and Connect API and its protobuf contract,
`registry.ServerStore`, the providers file and every `FIBER_*` variable are
gone. Compose `fiber.New` with options instead: `FIBER_POSTGRES_URL` is
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

**Persisted names moved to `fiber`.** Postgres objects, Redis keys, durable
activity and record names and the agent-spec hash domain no longer use the
old name. Drain runs before upgrading, including runs parked on a question,
which wait until they are answered, stopped or expire: a run still pending
from the old version is failed with a reason when the new one first claims
it, and is not resumed. Stop every old process before the first new one
starts. Postgres migrates itself and keeps its rows. See
[docs/renaming.md](docs/renaming.md).

### Limitations

- The bus is chosen independently of the stores: `WithPostgres` does not move the bus, `WithBus` does.
- The Postgres bus serializes publishes on one advisory lock per schema, so its throughput is one short transaction per event; Redis is the better bus for a chatty deployment.
- The Postgres bus stores every event it carries, body included, as a row in the `fiber_bus_events` table of the database it is given. A row stays until its run's retention trim drops it or a sweep finds it older than `bus.WithIndexTTL` (24 hours by default; the sweep runs from publishes, so with none nothing is removed). Those rows reach that database's WAL, replicas and backups like any other, and tool events carry their call's arguments. Where arguments can hold secrets, scrub them with `agentrt.WithToolArguments`, shorten the TTL, or use the Redis bus.
- Every store is in memory unless `WithPostgres` or its own option is given. On Postgres they survive a restart, but the run queue fiber builds has no `DeltaLog`, so the per-run delta journal is not written; the bus still carries every event.
- Files (`blob`) are in memory unless `WithBlobs(blob.NewFS(dir))` puts them on disk; there is no Postgres blob store.
- fiber authenticates nothing. An embedding program that accepts tags or stop and answer requests from callers sets the tenant and actor from the caller's credential.
- Citations are derived from RAG context blocks. Citations a model or tool reports itself are not translated into events yet.
- The bus is at-least-once with no consumer groups.
- Agent specs load from JSON, not YAML.
- `App.Status` reports a stopped run as `stopped`. A stop is recorded in the run ledger before it is announced on the bus, so once it is acknowledged the run ends stopped unless its turn had already returned its answer. A sink that subscribes only to `run.completed` is not told about a stop.
- A request to compact a running turn (`App.Compact`) is not recorded: it reaches only the attempt running when it is made, before that turn's next model call. See [Compacting a run on request](docs/architecture/overview.md#compacting-a-run-on-request).
- A program declares MCP tool providers with `fiber.WithToolProvider`, or writes them to the tool provider store (`App.ToolProviders`). A group grant that cannot be expanded (no such provider for the tenant, the server unreachable, nothing the grant's class admits) is dropped like a single unavailable tool: the run continues without it, the model is told it is unavailable, the operator is logged why, and `Runner.Unavailable` lists it. `agentrt.WithStrictTools` fails the build with `agentrt.ErrNoSuchToolGroup` instead.
- A person-scoped MCP provider's tools are expanded per person, so two people in one thread can be offered different tools, and the thread's prompt cache misses when the person changes. At most 128 person sessions are kept open (`mcptools.WithPersonSessions` on a hand-built `Grants`); past that the one used longest ago is closed and a turn still holding its tools sees a tool error. A stdio provider runs a server per open person session. The resolver is asked on every turn, and a new value behind an unchanged reference is not noticed until the reference, the secret or the provider record changes.
- Only MCP tool providers take a person's credential. The Slack, GitHub and Jira connectors' actions and sinks still act with the credential the deployment configured for them.
- A granted tool this deployment cannot resolve (no connector contributes it, or an MCP tool is held back) is dropped rather than failing the run. The model is told it is unavailable and the server logs a warning naming the tool and the reason; `agentrt.WithStrictTools` restores the old refusal.
- Tool events carry the call's arguments, capped at 8 KiB; scrub sensitive ones with `agentrt.WithToolArguments`.
- A parked run waits on a person through the run ledger. An answer or a stop wakes it at once; an expired question is noticed by the worker's janitor poll (30 seconds by default), so its default applies up to that late. The requester cannot approve their own calls unless the spec sets `self_approval`, and `approvers`/`answerers` restrict who may answer. `tagged_only` is a hint for hosts: an agent that sets it runs only when a message names it, and is never asked to take an untagged one on its own; fiber itself carries and hashes it, nothing more. A tool implementing `agentrt.Approving` decides per call whether it needs approval and may name that call's approvers, which then replace the spec's. A host Delegator that blocks on a child run holds a worker and can exhaust them; `runtime.WithChildRuns` parks the parent on the child instead. Approvals inside an in-turn sub-task (a `tag_<agent>` call without child runs or a Delegator) are refused, not asked. See [Parking a run on a person](docs/architecture/overview.md#parking-a-run-on-a-person).
- Cost is reported only for models the price catalog prices, in integer micro-units with the catalog's `as_of`; anything else reports tokens and no cost. Usage is delivered only to sinks that subscribe to `run.usage`. A turn that crashes mid-call is on the bus but not in the run's total. In-turn delegation is one level deep (a sub-task holds no spawn grant), and a host that runs delegations as runs of their own rolls them up with `runtime.ReportChildUsage`. See [Usage](docs/architecture/overview.md#usage).

## Configuration

Pass every choice as an option to `fiber.New`. Read files or the environment
in your own program and turn them into these options; fiber reads neither.

| Option | Chooses | Default |
|--------|---------|---------|
| `WithPostgres(pool)` | Registry, run ledger and queue, knowledge, threads, tool providers on one pool | memory |
| `WithBus(b)` | `bus.NewMemory`, `bus.NewRedis(client)`, `bus.NewPostgres(pool)` | memory |
| `WithRegistry`, `WithDurable(ledger, queue)`, `WithThreads`, `WithKnowledge`, `WithBlobs`, `WithToolProviders` | One store, built by the caller | memory, or the `WithPostgres` one |
| `WithEmbedder(e)` | The embedder of the knowledge store New builds | offline hashing |
| `WithTurnRuntime`, `WithRuntimeOptions`, `WithWorkerOptions` | The turn runtime (a `turn.Runtime`), and options for the runtime and worker | built-in sandbox |
| `WithWorkspace(dir)` | Where the sandbox writes agent artifacts | a temporary directory |
| `WithTenant`, `WithConcurrency`, `WithCompaction`, `WithAuthor`, `WithLogger`, `WithMetrics` | Deployment | none, one, off, offline, discard, none |
| `WithSlack`, `WithGitHub`, `WithJira`, `WithWebhooks`, `WithSchedules` | Built-in connectors, typed Go values | off |
| `WithModelProvider`, `WithOpenAICompatible`, `WithTools`, `WithToolSource`, `WithConnector`, `WithToolProvider` | Extensions | none |
| `WithPersonCredentials` | Each person's own credential for person-scoped MCP providers | none, so those providers offer no tools |

Model providers read their own credentials when an agent is built:
`ANTHROPIC_API_KEY`, `OPENAI_API_KEY` and `OPENAI_BASE_URL`, `OLLAMA_HOST`,
and the variable an `OpenAICompatible.KeyEnv` names. An MCP provider's
`env:NAME` credential reference resolves the same way, as does a reference a
`WithPersonCredentials` resolver returns for a person. Set up connectors with
the [GitHub](docs/architecture/github.md) and [Slack](docs/architecture/slack.md)
guides.

## API

| Method | Does |
|--------|------|
| `App.Publish` | Puts an ingress event on the bus; the channel's members each start a run |
| `App.Runtime().Accept` | Starts one agent's run for an event, without membership |
| `App.Bus().Subscribe` | Streams a tenant's events by channel and type prefix |
| `App.Status`, `App.Stop`, `App.Compact`, `App.Answer` | Act on a run |
| `App.ToolsFor` | Lists what an agent's run of an event would be granted, without running it |
| `App.WebhookHandler()` | An `http.Handler` for the connectors that receive webhooks, for your program to mount |

Full reference: [pkg.go.dev/github.com/urmzd/fiber](https://pkg.go.dev/github.com/urmzd/fiber)

## Development

| Action | Command |
|--------|---------|
| Set up | `make init` |
| Build | `make build` |
| Test | `go test ./...` (`-race` for the race detector) |
| Lint | `golangci-lint run` |
| Quality gate | `make check` |
| Start Postgres and Redis | `make up` (stop with `make down`) |
| Run the suites on real backends | `make integration` |

### Reproduce the screenshot

Install [teasr](https://github.com/urmzd/teasr), then run from this checkout:

```sh
go run ./examples/local
teasr run --config teasr.toml
```

The first run warms Go's build cache. The capture runs the real demo and writes
[`showcase/durable-replay.png`](showcase/durable-replay.png).

## Documentation

| Document | Covers |
|----------|--------|
| [docs/architecture/overview.md](docs/architecture/overview.md) | Topics, connector faces, delivery, revision pinning, design |
| [docs/architecture/github.md](docs/architecture/github.md) | GitHub connector setup |
| [docs/architecture/slack.md](docs/architecture/slack.md) | Slack connector setup |
| [docs/renaming.md](docs/renaming.md) | Upgrading persisted data across the rename |
| [examples/README.md](examples/README.md) | Runnable example index |
| [AGENTS.md](AGENTS.md) | AI-facing conventions, commands, rules, extension guide |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development workflow and commit convention |
| [SECURITY.md](SECURITY.md) | Security boundaries and reporting |

## License

Apache 2.0. See [LICENSE](LICENSE).
