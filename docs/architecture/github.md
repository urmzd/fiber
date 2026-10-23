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
names an agent raises a tag for the first agent it names. One that names no
agent but mentions the app's own login (`Config.Login`, such as `humagen[bot]`,
mentioned as `@humagen`) is addressed to the app, the way Slack delivers only
messages that mention the app. `Config.DefaultAgent` answers it, and with none
it raises nothing. The app's mention is taken out of the text in both cases.
Anything else raises nothing, so a busy repository wakes no agent on every
comment, and a bot's comment raises nothing whatever it mentions.

A login in `Bots` names its agent wherever it is mentioned, the app's own
too. So when `Bots` maps the app's login, its mention is a tag for that agent,
and `DefaultAgent` is not asked: `@humagen what changed?` and `@humagen @docs-bot
summarize` both ask the agent `Bots` maps the app to, since the first agent
the text names answers.

A host that decides who answers once a tag reaches it, such as Humagen, gives a
name of its own as `DefaultAgent` and resolves it before the run is accepted.
