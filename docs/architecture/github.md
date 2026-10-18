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
The standalone fiber CLI retains only transport configuration:
`FIBER_GITHUB_TOKEN`, `FIBER_GITHUB_WEBHOOK_SECRET`,
`FIBER_GITHUB_OWNERS`, `FIBER_GITHUB_BOTS`,
`FIBER_GITHUB_READ_REPOSITORIES`, and `FIBER_GITHUB_CHECKS`.
`FIBER_GITHUB_AUTO_LABELS`, `FIBER_GITHUB_GUIDES` and
`FIBER_GITHUB_MUTATIONS` are no longer supported here.
