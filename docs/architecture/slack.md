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
