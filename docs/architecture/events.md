# Events, channels, definitions and the Runtime

> **Design, being implemented.** This document is the target for the event
> union work. It replaces the Tag, topic, registry and Executor sections of
> [overview.md](overview.md). Where the code still uses the old shapes, the
> old sections describe what runs today, and this document describes where
> each piece goes.

The design turns on four decisions:

1. Every event is one envelope with a typed body. `type` names the body.
2. One bus carries every event. Subscribers listen by channel and type prefix.
3. Agents are saige definitions. fiber keeps no registry and no `AgentSpec`.
4. A run executes through a small `Runtime` interface. saige is the default.

A run is no longer started by a free-text `Tag`. It is started by an event: a
chat message is `message.posted.v1` with a `text` field.

## 1. Envelope

Every event, in every direction, is one envelope. The fields follow
[CloudEvents](https://cloudevents.io) naming.

| Field | Type | Meaning |
|-------|------|---------|
| `id` | string | Unique per source. Redelivery of the same thing keeps the same id. |
| `source` | URI | What produced the event: `slack://T01`, `github://acme`, `fiber://run/<run>`. |
| `type` | string | The only discriminator. Carries the version. See [Types](#2-types). |
| `subject` | string | The resource inside the source, such as `issues/42`. Optional. |
| `channel` | URI | Where the event is published. See [Channels](#5-channels). |
| `time` | RFC 3339 | When the source says the event happened. |
| `hops` | int | How many events led to this one. See [Rules](#6-rules). |
| `data` | JSON | The body for `type`, as inline JSON. Never base64. |

Run-scoped events (every `run.*` and `control.*` type) also carry:

| Field | Meaning |
|-------|---------|
| `tenant` | The tenant the run belongs to. Set by the host from the principal, never read from the wire. |
| `run` | The run id. |
| `seq` | The position in the run, banded as today (see [Rules](#6-rules)). |
| `agent` | The definition name that produced the event. |
| `version` | The pinned definition version. |
| `digest` | The pinned definition digest. |

Private host data, such as a Slack bot user id or a GitHub installation id,
goes in `Envelope.Private`. It is tagged `json:"-"`: it is never rendered to
the model, never stored on the bus as part of `data`, and never sent to an
egress adapter.

```json
{
  "id": "Ev01J9",
  "source": "slack://T01",
  "type": "message.posted.v1",
  "subject": "C02/1699.001",
  "channel": "slack://T01/C02",
  "time": "2026-10-28T14:03:11Z",
  "hops": 0,
  "data": {"text": "summarise this thread", "actor": {"id": "U07", "name": "ada"}}
}
```

## 2. Types

`type` is the only discriminator, and it carries the version. The grammar is:

```
type    = family *( "." name ) ".v" N
family  = name
name    = [a-z0-9_]+
N       = [1-9][0-9]*
```

A breaking change to a body is a new type (`.v2`), registered next to the old
one. Nothing else in the envelope says what `data` holds.

### Run events, from the old kinds

Every `envelope.Kind` constant in `pkg/envelope` maps to one catalog type.

| Old `envelope.Kind` | New type | Notes |
|---------------------|----------|-------|
| `lifecycle.accepted` | `run.accepted.v1` | |
| `lifecycle.started` | `run.started.v1` | |
| `lifecycle.parked` | `run.parked.v1` | |
| `lifecycle.resumed` | `run.resumed.v1` | |
| `lifecycle.completed` | `run.completed.v1` | |
| `lifecycle.failed` | `run.failed.v1` | |
| `lifecycle.stopped` | `run.stopped.v1` | |
| `lifecycle.child.parked` | `run.child.parked.v1` | |
| `delta.text` | `run.delta.text.v1` | |
| `delta.thinking` | `run.delta.thinking.v1` | |
| `delta.tool.call` | `run.delta.tool.call.v1` | |
| `delta.tool.done` | `run.delta.tool.done.v1` | |
| `delta.citation` | `run.delta.citation.v1` | |
| `action.taken` | `run.action.taken.v1` | |
| `usage.call` | `run.usage.v1` | Opt-in |
| `usage.child` | `run.usage.child.v1` | Opt-in |
| `context.compacted` | `run.context.compacted.v1` | Opt-in |
| `control.stop` | `control.stop.v1` | Control channel only |
| `control.compact` | `control.compact.v1` | Control channel only |
| `control.created` | dropped | No producer |
| `control.revised` | dropped | No producer |
| `control.deleted` | dropped | No producer |

Lifecycle events used to share one payload. Each now has its own body, so
`run.failed.v1` carries an error and `run.parked.v1` carries the question,
and neither carries fields that belong to the other.

### Ingress types

| Type | Raised by |
|------|-----------|
| `message.posted.v1` | A Slack mention or direct message, a console message, any surface where a person writes to an agent |
| `slack.command.invoked.v1` | A Slack slash command |
| `slack.session.stopped.v1` | A person pressing stop on a Slack agent session |
| `github.issue_comment.created.v1` | A comment on an issue or pull request |
| `github.pull_request_review_comment.created.v1` | A review comment on a diff line |
| `github.pull_request_review.submitted.v1` | A submitted review |
| `github.issues.opened.v1` | A new issue |
| `github.issues.labeled.v1` | A label added to an issue |
| `github.pull_request.opened.v1` | A new pull request |
| `github.pull_request.ready_for_review.v1` | A draft marked ready |
| `github.pull_request.review_requested.v1` | A review requested from an agent's login |
| `github.pull_request.synchronize.v1` | New commits pushed to a pull request |
| `github.pull_request.labeled.v1` | A label added to a pull request |
| `jira.comment.created.v1` | A comment on a Jira issue |
| `jira.issue.created.v1` | A new Jira issue |
| `jira.issue.assigned.v1` | A Jira issue assigned to an agent |
| `cron.tick.v1` | A schedule firing |

GitHub types follow `github.<event>.<action>.v1`, using GitHub's own event
and action names, so a new GitHub event is added without a naming debate.

## 3. Go shape

```go
// Body is the typed data of one event type. It is sealed: only this
// module defines bodies.
type Body interface {
	Type() string
	isBody()
}

type Envelope struct {
	ID      string    `json:"id"`
	Source  string    `json:"source"`
	Type    string    `json:"type"`
	Subject string    `json:"subject,omitempty"`
	Channel string    `json:"channel"`
	Time    time.Time `json:"time"`
	Hops    int       `json:"hops"`

	Tenant  string `json:"tenant,omitempty"`
	Run     string `json:"run,omitempty"`
	Seq     uint64 `json:"seq,omitempty"`
	Agent   string `json:"agent,omitempty"`
	Version string `json:"version,omitempty"`
	Digest  string `json:"digest,omitempty"`

	Data    Body    `json:"data"`
	Private Private `json:"-"`
}

// Unknown keeps a type this build does not know, byte for byte.
type Unknown struct {
	TypeName string
	Raw      json.RawMessage
}
```

`Envelope.UnmarshalJSON` reads `type` first, looks it up in the catalog and
decodes `data` with that entry's `New`. A type the catalog does not hold
decodes to `Unknown{Type, Raw}`. It is never guessed from the shape of
`data`, and it is never an error: a subscriber that did not ask for the type
never sees it, and one that did receives the raw bytes. Marshalling an
`Unknown` writes `Raw` back unchanged.

A type that is known but whose `data` does not decode is an error. Fail
closed: a known type with a broken body is never passed on as `Unknown`.

## 4. Catalog entry

Every type is registered once, in one catalog.

```go
type Entry struct {
	Type      string                        // "message.posted.v1"
	New       func() Body                   // a zero body to decode into
	Render    func(Envelope) (string, error) // the body as model input
	Sensitive bool
	OptIn     bool
}
```

- `Render` turns the event into the text the model sees. It is byte stable:
  the same event renders the same bytes on every replay.
- `OptIn` keeps a family out of subscriptions that did not name it. The
  `run.usage.*` and `run.context.*` families are opt-in, as `usage` and
  `context` were.
- `Sensitive` follows one rule.

**Sensitivity rule.** A body is sensitive when it carries human-authored free
text or private host data. A sensitive body is rendered to the model, but it
is redacted from egress logs and webhooks unless the subscriber opts in
explicitly. Every ingress body with user text (`message.posted.v1`, the
GitHub and Jira comment and issue types, `slack.command.invoked.v1`) is
sensitive. Structural run bodies (`run.started.v1`, `run.usage.v1`) are not.
The model's own output (`run.delta.text.v1`) is not human-authored, and is
not sensitive.

`TestCatalogComplete` walks every registered body and fails on a type with no
renderer, no `New`, or a name outside the grammar. Adding a type without a
renderer fails the build's tests.

## 5. Channels

A channel is where an event is published. Channels are URI shaped and built
from `address.Address`:

```
<scheme>://<workspace>/<path>[?params]

slack://T01/C02
slack://T01/C02?thread=1699.001
github://acme/handbook/issues/42
jira://acme.atlassian.net/PROJ-7
cron://default/nightly-report
```

**The control channel** carries `control.stop.v1` and `control.compact.v1`:

```
control://<tenant>/<verb>/<sha256(tenant, run)[:16]>
```

The last segment is a hash so a run id is never spelled in a channel a
surface might log. Only a subscription that names a control channel exactly
ever matches it. A prefix, an empty path or a wildcard never does.

**Subscriptions.**

```go
type Subscription struct {
	Tenant   string   // from the principal, never from the request
	Channels []string // channel prefixes
	Types    []string // type prefixes
	Run      string   // optional: one run only
	From     string   // optional: resume after this cursor
}
```

An empty `Channels` or `Types` list matches nothing. Default deny: there is no
"everything" subscription, and a zero `Subscription` receives no events.

Prefixes are segment aware. `slack://T01/C0` does not match `slack://T01/C02`,
and `run.delta` does not match `run.deltas.v1`. Channel segments split on
`/`, type segments on `.`.

## 6. Rules

- **Hops.** Every event carries `hops`. An event produced in answer to
  another has `hops` one higher. `DefaultMaxHops` is 8. An event over the
  limit is refused at publish, so two agents cannot answer each other
  forever.
- **Self-wake.** When `event.Agent` equals the subscriber, the event never
  wakes that subscriber. An agent's own output does not start a run of the
  same agent.
- **At-least-once.** Delivery is at-least-once. Egress adapters are
  idempotent, as sinks are today: one message per run, edited as events
  arrive.
- **Run key.** `RunKey(tenant, event id, subscriber)` is the run id. A
  redelivered event computes the same key and joins the existing run. Two
  agents woken by one event get two runs. Child run ids are derived as they
  are today and do not change.
- **Sequence.** Per-run `seq` banding is kept: each attempt and each child
  writes in its own band, so ordering inside a run survives retries.
- **Membership.** An agent is woken only by channels it is a member of and
  types its policy allows. Membership is default deny.

## 7. Definitions

Agents are saige definitions (`saige/agent/definition`) plus a fiber
**Policy** sidecar, both in `pkg/definition`. fiber has no `AgentSpec`, no
revision counter and no registry of its own.

A definition is one `.agent.md` file: YAML frontmatter, then the system
prompt.

```markdown
---
apiVersion: saige/v1
name: docs-bot
version: 1.0.0
description: answers questions about how we ship
model:
  use: anthropic/claude-haiku-5-5
  fallback: [openai/gpt-5-mini]
tools:
  registry: [lookup_runbook]
  mcp:
    - server: issues
subagents:
  - ref: helper@^1
approval:
  ask: [lookup_runbook]
---
You answer questions about the deploy pipeline. Cite your sources.
```

The sidecar holds what saige does not own. Its zero value grants nothing, and
its hash (`Policy.Hash`, domain `fiber/policy/v1`) is part of every pin.

| Field (JSON) | Meaning |
|--------------|---------|
| `subscriptions` | Channels and type prefixes the agent subscribes to |
| `workspace_areas` | Sandbox areas the agent may touch |
| `mcp_write` | `tools.mcp` servers whose write tools it may call |
| `knowledge` | Corpora it may retrieve from |
| `fallback_on` | When a run falls back to another model |
| `approvers` | Who may approve its `approval.ask` calls |
| `self_approval` | Whether the person who asked may approve |
| `answerers` | Who may answer a parked question |
| `tagged_only` | Keep the agent out of messages that do not name it |

**Validation.** A definition passes saige's own checks, then fiber's, which
fail closed: a model is required and is `provider/model` (no presets); the
provider is in the deployment's table; at most 4 fallbacks, each with a
credential when the deployment checks credentials; sub-agents delegate only;
`approval` supports `ask` only, for tools the agent holds. Fields fiber cannot
honor (dials, skills, memory, guardrails, compaction, harness tools, MCP allow
lists) are refused, not ignored. Names follow saige's rule and are valid
channel segments (`event.ValidToken`). Budgets are converted to `int64`
micro-USD at load; an amount that cannot be represented exactly is refused.

**The pin.** A run pins, in its durable input:

| Field | Source |
|-------|--------|
| `Name` | The definition name |
| `Version` | The exact semantic version resolved at accept time |
| `Digest` | saige's `Resolved.Digest` (`sha256:<hex>`) |
| `Subagents` | The exact version of every subagent |
| `PolicyHash` | The hash of the Policy sidecar |
| `Hash` | One hash over all of the above |

**Replay** reloads the exact versions named in the pin. It fails terminally
when the reloaded digest or policy hash does not match, when a pinned row is
missing, or when the digest is empty. A newer prompt never runs under an old
pin.

**The definition Source** is scoped by tenant and insert only. A version, once
written, is never updated, so a pin stays resolvable for as long as the run
can be replayed. `Put` of identical content is a no-op and of other content
under a written version is `ErrExists`. Retiring an agent stops new runs and
new versions; `Get` still reads every version. `Memory` and `Postgres`
(`fiber_definitions`, `fiber_definition_retirements`, no UPDATE path) pass
`definitiontest.Run`. A Resolver builds a saige registry over exactly the
tree a root reaches, per tenant, so saige's reference and cycle checks apply.

**From the old specs.** `FromAgentSpec` converts a stored spec
deterministically: revision N becomes version N.0.0, `access.spawn` becomes
`subagents`, `approvals` becomes `approval.ask`, and everything else goes to
the Policy. A spec whose names break the name rule is refused with a
`NameError` listing them.

**Local runs.** `fiber run [flags] agent.agent.md MESSAGE` stores the file
(and `-policy policy.json`) in memory and runs it once.

## 8. Runtime

A run executes one turn at a time through a small interface:

```go
// Runtime runs one turn of a pinned definition for one input event and
// streams catalog events back.
type Runtime interface {
	Turn(ctx context.Context, in Input, emit func(context.Context, event.Envelope) error) (Outcome, error)
}

type Input struct {
	Tenant string
	Run    string
	Pin    Pin            // the definition, fixed at accept time
	Event  event.Envelope // the event that started or resumed the run
}
```

- The **native** implementation wraps saige: it resolves the pin, builds the
  agent and turns saige deltas into `run.delta.*` events.
- Another implementation can run any agent loop, as long as it emits
  catalog events.
- **Sandbox** is a decorator around any `Runtime`. It runs the wrapped turn
  as a playscape task under the pin's policy, so isolation does not depend on
  which runtime is inside it.
- **anchor** stays the durable layer. The turn is still an anchor activity,
  and `emit` still writes the journal before the bus.

`Runtime` replaces `runtime.Executor` and the `agentrt` coupling between the
runtime and saige.

## 9. Adapters

Surfaces are adapters. Delivery does not know which surface it serves.

- **Ingress adapters** (Slack, GitHub, Jira, cron, the console) verify the
  request, translate it into one typed event, and publish it on the bus.
  They no longer pick an agent: membership does.
- **Egress adapters** (Slack, GitHub, Jira, webhook, console SSE) subscribe by
  channel plus type prefix and render what arrives onto their surface.

Implemented in `pkg/connector` (`Trigger.Ingest(ctx, publish)`,
`Subscriber`) and `pkg/egress`, which replaced the router.

- **Ingress.** Slack publishes `message.posted.v1` (mentions from `Bots`),
  `slack.command.invoked.v1` and `slack.session.stopped.v1`. GitHub publishes
  `github.*.v1`, with `head_sha` pinned before publish when checks are on; a
  bot sender is dropped except for `synchronize`, whose id is
  `github:commit:<repo>:<number>:<sha>`. Jira publishes `jira.*.v1`. Cron
  publishes `cron.tick.v1` with id `<name>@<unix>` on `cron://<tenant>/<name>`,
  and joins the schedule's agent to that channel. A `Prepare` hook takes
  `*event.Envelope`.
- **Egress.** A sink subscribes to its own scheme's channels with the run
  types, or names its channels and types (`connector.Subscriber`). A sink
  renders only its own scheme: a target of another connector is a config
  error. Opt-in types are delivered only when named. Rendering state is kept
  per `(run, channel)`.

**Webhook body v2.** The outbound webhook posts the envelope as JSON, with
`specversion` 1.0 and the header `Fiber-Event-Version: 2`. The delivery id is
`<run>/<seq>`. An endpoint names its `Channels` (required) and `Types`.
Sensitive bodies are redacted unless the endpoint sets `Sensitive`. Old kind
selectors (`lifecycle.completed`, `delta`, `usage`) in `Kinds` are read
through `legacy.KindSelector` at config load. An unknown selector is a config
error, not an empty match.

## 10. Upgrade

- **Activity names.** The durable turn registers `fiber.turn.v2`,
  `fiber.turn.agent.v2` and `fiber.turn.resume.v2`. A run recorded under the
  v1 names (`fiber.turn`, `fiber.turn.agent`, `fiber.turn.resume`) or before
  the rename (`mandatum.turn`) fails terminally on replay, with the reason
  "started before the event union; send the message again", instead of
  running a body it was not recorded with.
- **Bus tables.** The channel bus writes new tables. The old `fiber_bus_*`
  tables are read by nothing after the upgrade and are safe to drop after
  24 hours, when every row in them has aged out.
- **Legacy decoders.** Stored JSON written before the union (thread entries,
  run records, journal events with a `kind`) is read through legacy decoders
  that map each old kind to its new type, using the table in
  [Types](#2-types).
- **Deploy.** Deploy with a single replica, or stop old replicas briefly
  before starting new ones. Old and new replicas do not share a bus.

## 11. Kept stored names

These names are in stored data and do not change:

| Kind | Names |
|------|-------|
| Tables | `fiber_runs`, `fiber_run_records`, `fiber_run_queue`, `fiber_threads`, `fiber_thread_entries`, `fiber_knowledge_documents`, `fiber_knowledge_chunks`, `fiber_tool_providers`, `fiber_tool_snapshots` |
| Record keys | `fiber.question.<id>`, `fiber.answer.<id>`, `fiber.child.<id>`, `fiber.stop`, `fiber.usage.a<attempt>`, `fiber.knowledge.v1` |
| Activity names | `fiber.turn`, `fiber.turn.agent`, `fiber.turn.resume`, kept only so a v1 run is recognised and failed terminally |

The tables `fiber_agents` and `fiber_agent_revisions` belonged to the
registry. They are no longer read or written. Their agents move to
`fiber_definitions` through `FromAgentSpec`, and the tables can be dropped
once no run pinned to a revision is still replayable.
