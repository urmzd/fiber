# Security Policy

## Reporting a Vulnerability

Report security issues privately through
[GitHub Security Advisories](https://github.com/urmzd/fiber/security/advisories/new)
rather than in a public issue. Please include a description, the affected
version or commit, and a reproduction if you have one.

## Security boundaries

Two parts of fiber are security boundaries. A change near either one needs
tests proving confinement still holds, and a pull request that touches them
without such tests will not be merged.

### `pkg/signature`

For a webhook-style trigger this is the only boundary there is. There is no
session and no handshake: the sole evidence that a request came from Slack or
GitHub, rather than from anyone who learned the URL, is a MAC over the raw
request body. A run, the agent's tools, and everything those tools may do all
hang off this check returning nil.

Invariants:

- Comparison is constant-time, via `hmac.Equal`. Never `==`, never
  `bytes.Equal` on a digest: a comparison that short-circuits leaks through
  timing how long a prefix matched, and a public endpoint can be probed
  repeatedly.
- Verification happens before the body is parsed, and hashes the exact bytes
  received. Not a re-encoded struct, not a normalized copy.
- Every ambiguity fails closed: a missing header, an empty header, a duplicated
  header, a malformed digest, and an unconfigured secret all return an error.
  An empty secret is treated as broken rather than permissive, because HMAC with
  an empty key is computable by anyone.
- Slack's replay window is enforced, and a stale timestamp is rejected without
  the digest being computed at all.

GitHub signs the body alone, with no timestamp in the base string, so a captured
GitHub request stays valid as long as the secret does. No code in this package
can change that. That defense lives downstream in event id idempotency, where a
replayed delivery joins the run it duplicated rather than starting a second one.
This is stated plainly rather than implying a protection the package cannot
provide.

Required test coverage for changes: known-good vectors pinning the exact signing
base string, tampered body, tampered signature, wrong secret, missing and
duplicated headers, malformed digests, replay window on both sides of its
boundary, and a fuzz pass proving no input panics.

### The NGAC policy path

An agent's `Access` block compiles into playscape's policy graph and is enforced
by the sandbox with default deny. It governs both the workspace areas a tool may
touch and the spawn allowlist that decides which agents an agent may delegate
to.

Required test coverage for changes: path traversal, sibling prefixes, list-leak,
spawn gating, and prohibition override. Default is always deny.

## Tenant isolation

Tenant is an authorization scope, and fiber authenticates nothing: it has no
transport and no notion of a caller. `App.Publish`, `Runtime.Accept`, `Stop`,
`Answer` and `Status` trust the tenant and actor they are handed. A program
that accepts events or requests from callers (Humagen is one) sets the
event's `Tenant`, its request's `Actor` and `Actor.User` from its own credential and overwrites whatever a client
sent; that check lives in that program, not here.

What fiber does guarantee is that a tenant, once set, scopes everything
after it. A run's id is derived from the tenant and the tag id together, so a
second tenant reusing a first tenant's tag id gets a run of its own and
cannot join the first run or replace where its events are delivered.
`Status` reports another tenant's run as `ErrNotFound`, exactly like one
that does not exist; `Answer` finds no question on it, and `Stop` records
nothing on it. `TestATenantReusingAnotherTenantsTagIDCannotJoinOrRerouteItsRun`
asserts the separate runs, the separate deliveries and the `ErrNotFound`.
Connectors that receive webhooks stamp the deployment's tenant
(`fiber.WithTenant`) on the tags they raise, because a webhook carries no
credential to derive one from.

## Tool credentials

An MCP tool provider is reached with one shared credential or, when it is
person-scoped, with the credential of the person a run acts for. That person
is the actor on the run's accepted tag, which the runtime reads from the run
ledger and puts on every turn's context; nothing a model produces can change
it. A person-scoped provider names no shared credential (the store refuses a
record that does), `mcptools.Connect` refuses to open one without a person's
own credential, each person's session is opened with exactly theirs and kept
apart from everyone else's, and a person with none gets no tools. A shared
credential is never a fallback. Every resolved credential, shared or a
person's, is redacted from tool results, errors and logs.

The host decides which credential belongs to which actor
(`mcptools.PersonResolver`), so a host that keys credentials on an actor field
it does not set from a trusted credential is where this can fail; key them
on `Actor.User`, the host's own account id, set only from a credential trusted
to act for others.

## Supported Versions

fiber is pre-1.0. Security fixes land on the latest minor release.
