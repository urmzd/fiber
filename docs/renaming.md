# Ecosystem naming

Humagen is the platform formerly named Warp. The library names are unchanged.
See the [Humagen upgrade guide](https://github.com/urmzd/humagen/blob/main/docs/upgrading-from-warp.md)
for accepted old names and identifiers kept for stored data.

| Previous | Current | Responsibility |
|----------|---------|----------------|
| Collegium, Vultus and Warp | [Humagen](https://github.com/urmzd/humagen) | Shared human-and-agent workspace, API and presentation |
| Mandatum | Fiber | Agent library: bus, connectors, routing, durable runs |
| Legatus | Playscape | Governed execution environments and workspace isolation |
| Duraturo | Anchor | Durable execution, replay and recovery |

## Persisted identifiers that changed

| What | Before | Now |
|------|--------|-----|
| Postgres tables, indexes and constraints | `mandatum_*` | `fiber_*` |
| Redis bus key namespace (`bus.DefaultKeyPrefix`) | `mandatum:` | `fiber:` |
| anchor activities and records | `mandatum.turn`, `mandatum.turn.agent`, `mandatum.turn.resume`, `mandatum.park`, `mandatum.stop`, `mandatum.question.*`, `mandatum.answer.*`, `mandatum.usage.*`, `mandatum.child.*` | the same under `fiber.` |
| Agent hash domain | the old spec domain under `mandatum/` | the same under `fiber/`, retired with the event union (see [events.md](architecture/events.md#10-upgrade)) |
| Knowledge document hash domain | `mandatum.knowledge.v1` | `fiber.knowledge.v1` |

## Upgrading a deployment

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
   matter. The old agent registry tables are no longer read: agents are
   saige definitions in `fiber_definitions` (see
   [events.md](architecture/events.md#7-definitions)). `pkg/internal/pgrename` is the only code that knows the old names.
4. **Redis starts empty.** The bus's keys are a reconnect window, not a
   record, so the old `mandatum:` keys are simply abandoned; delete them when
   convenient.
5. **Knowledge re-embeds once.** A document's hash includes its domain, so
   the first re-index after the upgrade treats every document as changed.
6. **Anything that stored a revision hash** outside fiber (a link, an audit
   log) holds the old address. The new one is the definition content hash,
   `App.Definitions().Get(ctx, tenant, name, version)` then `.Digest` (see
   [events.md](architecture/events.md#7-definitions)).

Historical changelog entries keep the names they were written with.
