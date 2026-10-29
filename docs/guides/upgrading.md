# Upgrading

How to move data and hosts across fiber's breaking releases, newest first.
Drain runs before every upgrade below: a run recorded under an older shape
fails terminally on replay instead of running a body it was not recorded with.

## To the event union (v0.26.0)

Every event is one envelope with a typed body, the bus is keyed by channel,
agents are saige definitions, and a turn runs through `turn.Runtime`. The new
shapes are specified in [events.md](../architecture/events.md). This section is
the one place the names from before the union appear.

**Removed packages and shapes.**

| Before | Now |
|--------|-----|
| `pkg/envelope` (`Event`, `Kind`, `Filter`, `Subscription`) | `pkg/event` (`Envelope`, `Body`, the catalog) and `bus.Subscription` |
| `pkg/topic` (`agent`, `agent:<name>`, `agent:<name>:<run>`, `agent:_control:<verb>-<key>`) | URI channels and `control://<tenant>/<verb>/<hash>` (`event.ControlChannel`) |
| `pkg/agentspec` (`AgentSpec`, revisions) | saige `.agent.md` definitions plus `definition.Policy` |
| `pkg/registry` and `registrytest` | `definition.Source` (`Memory`, `Postgres`) and `definitiontest` |
| `pkg/agentrt` and `pkg/agentrt/payload` | `pkg/turn/native` and the bodies in `pkg/event` |
| `runtime.Executor` | `turn.Runtime`, with `pkg/turn/sandbox` as a decorator |
| The router and `Route` kind filters | `pkg/egress` subscriptions by channel and type prefix |
| `Tag`, with free text in `Tag.Text` and context in `Tag.Meta` | An ingress event; the text is `Request.Text`, the context `Request.Context`, private keys `Envelope.Private` |
| A connector's `Trigger` choosing an agent | Ingress publishes on the surface's channel; membership and dispatch choose |
| `fiber.WithRegistry`, `App.Registry` | `fiber.WithDefinitions`, `App.Definitions` |
| `App.Invoke(ctx, tag)` | `App.Publish(ctx, event)` with `App.Memberships`, or `App.Runtime().Accept(ctx, event, agent)` |
| `App.ToolsFor(ctx, tag)` | `App.ToolsFor(ctx, event, agent)` |
| `fiber.WithExecutor` | `fiber.WithTurnRuntime` |
| `App.Router` | `App.Egress`, and `App.Dispatcher` for waking members |
| Webhook body v1 (`kind`, `topic`, `payload`) | Webhook body v2, the envelope itself, with `Fiber-Event-Version: 2` |

**Old kinds to types.** Every `envelope.Kind` maps to one type, through
`legacy.TypeOf`:

| Old kind | Type |
|----------|------|
| `lifecycle.<state>` (`accepted`, `started`, `parked`, `resumed`, `completed`, `failed`, `stopped`) | `run.<state>.v1` |
| `lifecycle.child.parked` | `run.child.parked.v1` |
| `delta.<name>` (`text`, `thinking`, `tool.call`, `tool.done`, `citation`) | `run.delta.<name>.v1` |
| `action.taken` | `run.action.taken.v1` |
| `usage.call` | `run.usage.v1` |
| `usage.child` | `run.usage.child.v1` |
| `context.compacted` | `run.context.compacted.v1` |
| `control.stop`, `control.compact` | `control.stop.v1`, `control.compact.v1` |
| `control.created`, `control.revised`, `control.deleted` | dropped: they had no producer |

- **Activity names.** The durable turn registers `fiber.turn.v2`,
  `fiber.turn.agent.v2` and `fiber.turn.resume.v2`. A run recorded under the
  v1 names (`fiber.turn`, `fiber.turn.agent`, `fiber.turn.resume`) or before
  the rename (`mandatum.turn`) fails terminally on replay, with the reason
  "started before the event union; send the message again", instead of
  running a body it was not recorded with.
- **Bus tables.** The channel bus writes new tables. The old `fiber_bus_*`
  tables are read by nothing after the upgrade and are safe to drop after
  24 hours, when every row in them has aged out.
- **Legacy decoders.** Stored JSON written before the union is read through
  `pkg/event/legacy`, which reads raw JSON and imports none of the old
  packages: `DecodeBusRow` and `DecodeEntry` for bus rows and journal events
  with a `kind`, and `DecodeTag` for an old run input. `DecodeTag` chooses the
  type from the tag's meta (`cron_*` keys make `cron.tick.v1`, `github_*` a
  `github.*` type, `jira_*` a `jira.*` type, a slash command
  `slack.command.invoked.v1`, anything else `message.posted.v1`) and renders
  exactly the request text the tag rendered.
- **Agents.** `definition.FromAgentSpec` converts a stored spec
  deterministically: revision N becomes version N.0.0, provider and model
  become `model.use`, fallbacks `model.fallback`, `access.spawn` becomes
  `subagents`, `approvals` becomes `approval.ask`, `mcp:<p>[:write]` becomes
  `tools.mcp` with the write bit in `Policy.MCPWrite`, and everything else goes
  to the Policy. A spec whose names break saige's name rule is refused with a
  `NameError` listing them. On Postgres, `fiber.New` moves the registry's
  tables `fiber_agents` and `fiber_agent_revisions` into `fiber_definitions`
  once (`Postgres.ImportRegistry`): every revision N becomes version N.0.0
  with its author and time, a deleted agent is retired, and an agent that
  cannot move is logged and runs nothing. The move is marked in
  `fiber_definition_imports` and never repeated. The old tables are not
  written, and can be dropped once the move is marked and no run pinned to a
  revision is still replayable.
- **Webhook selectors.** An endpoint's old `Kinds` (`lifecycle.completed`,
  `delta`, `usage`) are read through `legacy.KindSelector` at config load. An
  unknown or dropped selector is a config error, not an empty match.
- **Subscribers to topics.** A host that let callers subscribe to
  `agent:<name>` or `agent:<name>:<run>` translates those to channel and run
  subscriptions itself. `legacy.Topic` spells a run's old address, and
  `legacy.RunChannel` is the channel its old events are read onto.
- **Deploy.** Deploy with a single replica, or stop old replicas briefly
  before starting new ones. Old and new replicas do not share a bus.

### Kept stored names

These names are in stored data and do not change:

| Kind | Names |
|------|-------|
| Tables | `fiber_runs`, `fiber_run_records`, `fiber_run_queue`, `fiber_threads`, `fiber_thread_entries`, `fiber_knowledge_documents`, `fiber_knowledge_chunks`, `fiber_tool_providers`, `fiber_tool_snapshots` |
| Record keys | `fiber.question.<id>`, `fiber.answer.<id>`, `fiber.child.<id>`, `fiber.stop`, `fiber.usage.a<attempt>`, `fiber.knowledge.v1` |
| Activity names | `fiber.turn`, `fiber.turn.agent`, `fiber.turn.resume`, kept only so a v1 run is recognised and failed terminally |

## From the server binary (v0.21.0)

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

## Across the rename from mandatum (v0.13.0)

fiber was named mandatum before v0.13.0. Its sibling projects were renamed
around the same time; the platform side is in the
[Humagen upgrade guide](https://github.com/urmzd/humagen/blob/main/docs/guides/upgrading-from-warp.md).

| Previous | Current | Responsibility |
|----------|---------|----------------|
| Collegium, Vultus and Warp | [Humagen](https://github.com/urmzd/humagen) | Shared human-and-agent workspace, API and presentation |
| Mandatum | Fiber | Agent library: bus, connectors, routing, durable runs |
| Legatus | Playscape | Governed execution environments and workspace isolation |
| Duraturo | Anchor | Durable execution, replay and recovery |

### Persisted identifiers that changed

| What | Before | Now |
|------|--------|-----|
| Postgres tables, indexes and constraints | `mandatum_*` | `fiber_*` |
| Redis bus key namespace (`bus.DefaultKeyPrefix`) | `mandatum:` | `fiber:` |
| anchor activities and records | `mandatum.turn`, `mandatum.turn.agent`, `mandatum.turn.resume`, `mandatum.park`, `mandatum.stop`, `mandatum.question.*`, `mandatum.answer.*`, `mandatum.usage.*`, `mandatum.child.*` | the same under `fiber.` |
| Agent hash domain | the old spec domain under `mandatum/` | the same under `fiber/`, retired with the event union (see [To the event union](#to-the-event-union-v0260)) |
| Knowledge document hash domain | `mandatum.knowledge.v1` | `fiber.knowledge.v1` |

### Upgrading a deployment

1. **Drain first, parked runs included.** Stop accepting tags and let every
   run end on the old version. A run in flight across the upgrade cannot be
   resumed: its recorded activities and records carry the old names, and the
   new code reads only the new ones. A run parked on a question does not end
   by itself: it waits until someone answers it, it is stopped, or its
   question expires (`runtime.WithQuestionTTL`, 24 hours by default, applied
   at the next janitor poll). Answer or stop each one on the old version.
   This lists what is still pending there:

   ```sql
   SELECT run_id, created_at FROM mandatum_runs WHERE status = 'pending' ORDER BY created_at;
   ```

   and the old version's run status says which of them are parked and on
   what question.

   A run that is still pending when the new version starts is not resumed.
   The first time a worker claims it (the janitor's poll reaches a parked
   one within its interval, 30 seconds by default) it ends failed, once and
   terminally: the ledger records why, `lifecycle.failed` with the same
   reason is published on its topic, and the runtime logs an error naming
   the run. The agent does not run again for it. A surface the old process
   delivered to is not updated, because a run's delivery routes are
   registered in the process that accepted it; anyone subscribed to the
   topic is told. Raise it again as a new tag (the same tag id lands on the
   failed run), and its answer is delivered where the new tag's routes say.
   After the upgrade, these are the runs it applies to:

   ```sql
   SELECT run_id, created_at FROM fiber_runs
   WHERE status = 'pending' AND envelope->>'Name' = 'mandatum.turn';
   ```

2. **Stop every old process before the first new one starts.** A process
   from before the rename creates its `mandatum_*` tables again when it
   starts, and writes there. If the new version finds an old table beside
   its new one, its `Migrate` fails with an error naming both instead of
   choosing between them; stop the old process, move any rows it wrote, drop
   the old table, and start again.
3. **Postgres moves itself.** Every store's `Migrate` (which `fiber.New`
   runs for `WithPostgres`, and `durable.Open` runs for the ledger) first
   renames its own `mandatum_*` tables in the current schema, with their
   indexes and constraints, to `fiber_*`, in the same transaction as the rest
   of the migration, so rows are kept and the order stores migrate in does not
   matter. The old agent registry tables are read once, to move their
   agents into saige definitions in `fiber_definitions`, and never written
   (see [events.md](../architecture/events.md#7-definitions)). `pkg/internal/pgrename` is the only code that knows the old names.
4. **Redis starts empty.** The bus's keys are a reconnect window, not a
   record, so the old `mandatum:` keys are simply abandoned; delete them when
   convenient.
5. **Knowledge re-embeds once.** A document's hash includes its domain, so
   the first re-index after the upgrade treats every document as changed.
6. **Anything that stored a revision hash** outside fiber (a link, an audit
   log) holds the old address. The new one is the definition content hash,
   `App.Definitions().Get(ctx, tenant, name, version)` then `.Digest` (see
   [events.md](../architecture/events.md#7-definitions)).

Historical changelog entries keep the names they were written with.
