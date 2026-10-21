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
[warp](https://github.com/urmzd/warp/blob/main/docs/github.md).
Evidence-backed Markdown presentation lives in warp's `go/github` package.

For the product setup use `WARP_GITHUB_*` with `warp serve`.
`fiber.WithGitHub` takes only transport configuration, as a `fiber.GitHub`
value: the token, the webhook secret, owners, bot mappings, read
repositories and whether to publish checks. Auto labels, guides and
mutation grants are warp's.
