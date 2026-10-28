# Examples

Runnable programs that demonstrate fiber rather than describe it. Every
example in this directory runs with no API key, no network and no
infrastructure: the in-memory implementations of the same interfaces stand in
for Postgres, Redis and a model provider, so `go run` is the only setup.

| Example | Command | What it proves |
| --- | --- | --- |
| [`downstream`](downstream) | `go run ./examples/downstream` | A team's own program on fiber with no fork: it composes `fiber.New` with a model provider, an OpenAI-compatible endpoint and a tool, stores `agents/oncall.agent.md`, accepts one `message.posted.v1` in process and prints the answer. |
| [`local`](local) | `go run ./examples/local` | The whole architecture end to end: an agent as a saige definition, immutable versions, an event raised on one channel answered by two egress adapters, N:M selectivity, an uninvolved third party reading the stream, first-class citations, and a replayed run that does not re-execute a recorded activity. |

## local

```
go run ./examples/local
```

One process, one file of story (`main.go`) and one file of scaffolding
(`fakes.go`). It prints seven numbered sections, each one a claim fiber makes
and a demonstration of it:

1. **An agent is a definition file.** `agents/docs-bot.v1.agent.md` (a saige
   definition: name, version, model, prompt) and its Policy sidecar
   `docs-bot.policy.json` (its knowledge) are stored as version 1.0.0, with
   saige's digest and the policy hash.
2. **A version is immutable.** Storing `docs-bot.v2.agent.md` writes version
   2.0.0 and leaves 1.0.0 byte for byte what it was. Both still resolve. This
   is why a run can pin its version and digest at accept time and replay under
   them forever.
3. **Origin and destination are independent.** A `github.issue_comment.created.v1`
   event arrives on `github://acme/handbook/issues/42` and is answered into
   `slack://T01/C02?thread=1699.001` and `webhook://acme/deploys`, because those
   egress adapters subscribe to the issue's channel. The trigger is GitHub; neither
   destination is. Nothing on the path from trigger to sink holds both ends at
   once, which is the difference between a mesh and a pile of integrations.
4. **One run, two sinks, two granularities.** The Slack sink subscribes to the
   `run` type prefix and edits one message as the answer streams in; the
   webhook subscribes to `run.completed.v1` only and posts exactly once. Same run, same bus, same
   events. Selectivity is a field on the subscription, not a second channel and
   not a second run.
5. **A third party can listen.** A subscriber holds one subscription to the
   `github://` and `cron://` channel prefixes. It raised nothing, no sink names it, and
   neither run can tell it is there. It sees both runs in full, with `Seq` as a
   resume cursor.
6. **Citations are first-class events.** `run.delta.citation.v1` arrives on the same
   stream and in the same order as the text, carrying document, section and a
   resolvable URI. Citations are derived from what the turn retrieved, not from
   what the model claimed, so a reader can check them.
7. **A replayed run does not re-execute a recorded activity.** The example arms
   one broker outage on a run's `run.completed.v1` publish. The turn fails,
   anchor replays the workflow from the top, and the printed model-call
   counter moves by exactly one across two attempts: the agent activity was
   served from the ledger. The subscriber sees a `run.resumed.v1` event in
   its own sequence band rather than a run that went silent.

### What is real and what is swapped

Nothing above the storage seam is simplified for the demo. The agent loop, the
typed deltas, the translation into catalog events, the sequence banding, publication,
routing, per-lane ordering and the durable replay are all the production code
paths.

| Concern | The example runs | A deployment runs |
| --- | --- | --- |
| Control plane | `definition.Memory` (`fiber.WithDefinitions`), agents read from `agents/*.agent.md` | `definition.Postgres` (`fiber.WithPostgres`) |
| Bus | `pkg/bus.Memory` (`fiber.WithBus`) | `bus.NewRedis` or `bus.NewPostgres` (`fiber.WithBus`) |
| Ledger and queue | anchor `ledger.Memory`, `queue.Memory` | anchor's Postgres ledger and queue (`fiber.WithPostgres`) |
| Model | saige `agenttest.ScriptedProvider`, via the definition's `offline/scripted` model | `anthropic`, `openai` (or any OpenAI-compatible server) or `ollama`, by changing `model.use` in the definition |
| Execution | an in-process `turn.Runtime` (`fiber.WithTurnRuntime`) | `runtime.NewSandbox`, a playscape node under NGAC, which `fiber.New` builds by default |
| Connectors | two fakes implementing `connector.Sink` (`fiber.WithConnector`) | `pkg/connectors/slack`, `pkg/connectors/webhook` (`fiber.WithSlack`, `fiber.WithWebhooks`) |

Every row is an option. `main.go` does not otherwise change.

### Reading the output

Two details are worth knowing before the first run.

**Sequence numbers jump.** Each attempt of a run gets its own sequence band, so
attempt 1 opens at 101 and attempt 2 at 201. That is what lets a retry append
its output after an interrupted attempt's partial output instead of vanishing
behind the bus's high-water mark. The example shrinks the band from its default
of 2^20 to 100 (`runtime.WithStride`) purely so the numbers stay readable.

**The surface output is printed settled, not live.** Egress carries events
for one `(run, target)` pair on a single goroutine and deliberately imposes no
order across targets, so two sinks printing directly would interleave
differently on every run. The fake sinks therefore record what they wrote to
their surface keyed by the sequence that caused it, and `main` prints the
transcript in `(seq, surface)` order once the run is terminal. What each sink
received, and when, is unchanged.

The example is deterministic: a fixed clock stamps the events, so two runs print
the same bytes.
