# Slack connector capabilities

Mandatum owns signature verification, mention and slash-command translation,
threaded message delivery, reaction APIs, and the optional `AgentAPI` methods
for session status and command registration. Ordinary messaging clients need
only implement `API`; agent-session features are a separate capability.

`Config.Prepare` lets an application validate a verified tag before handoff.
The configured team boundary applies to mentions, direct messages, stop events,
and slash commands after signature verification. A delivery from another team
is acknowledged without invoking application policy or raising a tag.

Session lifecycle policy, command discovery, refusal logging, app manifests,
and installation configuration live in
[collegium](https://github.com/urmzd/collegium/blob/main/docs/slack.md).
Use `COLLEGIUM_SLACK_*` with `collegium serve` for that experience.
The standalone mandatum CLI still supports the token, signing secret, team,
bot mapping and default agent; it does not enable product session or command
policy through `MANDATUM_SLACK_SESSIONS` or `MANDATUM_SLACK_COMMANDS`.

## Stopping a session

Slack sends `agent_session_stopped` when a person presses stop, and the
connector hands it to `Config.Stopped` as a `slack.Stop`: the session's address
and the user who pressed it. The connector knows sessions, not runs, so the
application maps the session to the run it stands for and asks the runtime to
stop that run:

```go
Stopped: func(ctx context.Context, stop slack.Stop) error {
    runID := runFor(stop.Session) // the application's own bookkeeping
    return rt.Stop(ctx, runtime.StopRequest{
        Tenant: tenant, RunID: runID,
        Reason: "Stopped in Slack", By: "slack:" + stop.User,
    })
},
```

The run id is the one `Acceptance.RunID` reported and every event of the run
carries. It is not the Slack event id the tag was raised with. The run then ends
with `lifecycle.stopped`, and the sink edits the session's message to say so
under what the agent had written. The standalone mandatum CLI does not set
`Stopped`, because it keeps no session-to-run mapping.
