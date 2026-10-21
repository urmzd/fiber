# Ecosystem naming

The rename is complete. Every component uses its current name in code,
docs and persisted identifiers.

| Previous | Current | Responsibility |
|----------|---------|----------------|
| Collegium and Vultus | Warp | Shared human-and-agent workspace, API and presentation |
| Mandatum | Fiber | Agent library: bus, connectors, routing, durable runs |
| Legatus | Playscape | Governed execution environments and workspace isolation |
| Duraturo | Anchor | Durable execution, replay and recovery |

## Persisted identifiers that changed

| What | Before | Now |
|------|--------|-----|
| Postgres tables, indexes, sequences and constraints | `mandatum_*` | `fiber_*` |
| Redis bus key namespace (`bus.DefaultKeyPrefix`) | `mandatum:` | `fiber:` |
| anchor activities and records | `mandatum.turn`, `mandatum.turn.agent`, `mandatum.turn.resume`, `mandatum.park`, `mandatum.stop`, `mandatum.question.*`, `mandatum.answer.*`, `mandatum.usage.*`, `mandatum.child.*` | the same under `fiber.` |
| Agent-spec hash domain | `mandatum/agentspec/v1` | `fiber/agentspec/v1` |
| Knowledge document hash domain | `mandatum.knowledge.v1` | `fiber.knowledge.v1` |
| GitHub review format | `mandatum.github.review.v1` | `fiber.github.review.v1` |

## Upgrading a deployment

1. **Drain first.** Stop accepting tags and let every run finish on the old
   version. A run in flight across the upgrade is not replayable: its
   recorded activities and records carry the old names, and the new code
   looks for the new ones.
2. **Postgres moves itself.** Every store's `Migrate` (which `fiber.New`
   runs for `WithPostgres`, and `durable.Open` runs for the ledger) first
   renames its own `mandatum_*` tables in the current schema, with their
   indexes and constraints, to `fiber_*`, in the same transaction as the rest
   of the migration, so rows are kept and the order stores migrate in does not
   matter. The registry then re-addresses every stored
   revision whose hash was computed under the old agent-spec domain, so
   revisions still pass the hash check on read and need no re-import. A
   revision matching neither domain is left alone and still fails that
   check. `pkg/internal/pgrename` is the only code that knows the old names.
3. **Redis starts empty.** The bus's keys are a reconnect window, not a
   record, so the old `mandatum:` keys are simply abandoned; delete them when
   convenient.
4. **Knowledge re-embeds once.** A document's hash includes its domain, so
   the first re-index after the upgrade treats every document as changed.
5. **Anything that stored a revision hash** outside fiber (a link, an audit
   log) holds the old address. The new one is `App.Registry().Get(...).Hash`.

Historical changelog entries keep the names they were written with.
