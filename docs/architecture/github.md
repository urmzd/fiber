# GitHub connector capabilities

Mandatum owns signature verification, normalized mentions and webhook events,
API context/file reads, comments and review-thread replies, idempotent delivery,
commit comparison, workflow API operations, and check-run transport.

Application policy is supplied through `Config.Automation`, `Config.Prepare`,
`Config.Render`, and `Config.Conclusion`. These hooks do not bypass webhook
verification or repository confinement. With no hooks, the connector answers
explicit mentions with ordinary Markdown and exposes no `/fix` or `/check` tools.

PR auto-label activation, per-commit review policy, PR guides, signed mutation
grants, agent specs and deployment configuration now live in
[collegium](https://github.com/urmzd/collegium/blob/main/docs/github.md).
Evidence-backed Markdown presentation lives in vultus's `go/github` package.

For the product setup use `COLLEGIUM_GITHUB_*` with `collegium serve`.
The standalone mandatum CLI retains only transport configuration:
`MANDATUM_GITHUB_TOKEN`, `MANDATUM_GITHUB_WEBHOOK_SECRET`,
`MANDATUM_GITHUB_OWNERS`, `MANDATUM_GITHUB_BOTS`,
`MANDATUM_GITHUB_READ_REPOSITORIES`, and `MANDATUM_GITHUB_CHECKS`.
`MANDATUM_GITHUB_AUTO_LABELS`, `MANDATUM_GITHUB_GUIDES` and
`MANDATUM_GITHUB_MUTATIONS` are no longer supported here.
