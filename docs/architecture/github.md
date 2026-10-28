# GitHub connector capabilities

Fiber owns signature verification, normalized mentions and webhook events,
API context/file reads, comments and review-thread replies, idempotent delivery,
commit comparison, workflow API operations, and check-run transport.

Application policy is supplied through `Config.Automation`, `Config.Prepare`,
`Config.Render`, and `Config.Conclusion`. These hooks do not bypass webhook
verification or repository confinement. With no hooks, the connector answers
explicit mentions with ordinary Markdown and exposes no `/fix` or `/check` tools.

PR auto-label activation, per-commit review policy, PR guides, signed mutation
grants, agent specs and deployment configuration now live in
[Humagen](https://github.com/urmzd/humagen/blob/main/docs/github.md).
Evidence-backed Markdown presentation lives in Humagen's `internal/presentation/github` package.

For the product setup use `HUMAGEN_GITHUB_*` with `humagen serve`.
`fiber.WithGitHub` takes only transport configuration, as a `fiber.GitHub`
value: the token, the webhook secret, owners, bot mappings, read
repositories, whether to publish checks, and the app's login with the agent
that answers it. Auto labels, guides and mutation grants are Humagen's.

## Who a comment asks

A comment, a review, a review comment, or a new issue or pull request that
names an agent is published as a `github.*.v1` event on the issue or pull
request's channel, with every agent it names in `Mentions`. One that names no
agent but mentions the app's own login (`Config.Login`, such as
`humagen[bot]`, mentioned as `@humagen`) is addressed to the app, the way
Slack delivers only messages that mention the app, and is published naming
no agent. The app's mention is taken out of the text in both cases. Anything
else publishes nothing, so a busy repository wakes no agent on every comment,
and a bot's comment publishes nothing whatever it mentions.

The connector chooses no agent. The channel's members decide who answers
(`pkg/membership`, `pkg/dispatch`). A login in `Bots` names its agent wherever
it is mentioned, the app's own too, so when `Bots` maps the app's login,
`@humagen what changed?` mentions that agent.
