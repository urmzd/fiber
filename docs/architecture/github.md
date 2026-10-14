# GitHub agent delivery

The GitHub connector accepts signed mentions on issues, pull requests, reviews,
and review comments. It also accepts review requests for configured agent logins.
Responses use one Markdown comment per run, edited as events arrive.

## Automatic labels

Set repository-specific label mappings alongside the GitHub token and webhook
secret:

```sh
export MANDATUM_GITHUB_AUTO_LABELS='{"acme/handbook":{"agent:auto":"docs-bot"}}'
```

Register `docs-bot` with mandatum and subscribe the GitHub webhook to `issues`
and `pull_request` events. Applying `agent:auto` starts a run using the subject's
title and body. The run answers on that subject unless delivery routes override
it. Repository matching is case-insensitive; label names match exactly.

Applying the label starts the first run. Subsequent PR `synchronize` events
start a review of the new head while the configured label remains. Removing the
label stops automatic commit reviews; unrelated label changes do not start runs.
Draft and closed PRs do not trigger commit reviews. Bot comments are ignored;
bot-authored commits on activated PRs can trigger reviews of requested fixes. `MANDATUM_GITHUB_OWNERS` also restricts inbound triggers.
A GitHub delivery ID is required for automatic labels and commit events: retries retain their run
identity, while removing and reapplying a label produces a new delivery.
Automatic commit reviews also deduplicate by repository, PR, agent and head SHA;
explicit user mentions start independent requests.

An empty mapping disables automatic runs. Malformed mappings fail startup.
The configured agent must exist in the registry when the trigger is accepted.

## Reading context and structured reviews

`github_read_context` returns the subject and a page of comments. For PRs it
also returns the head SHA, reviews, inline comments and changed-file patches.
Follow `next_page` until absent. A missing patch is unavailable evidence.
Reads default to the source repository. To allow GitHub context from Slack,
Jira, or links in another repository, grant exact repository names:

```sh
export MANDATUM_GITHUB_READ_REPOSITORIES='acme/handbook,acme/service'
```

The read tools accept an `address` containing a `github://` issue/PR address or
an HTTPS GitHub issue/PR URL. References are parsed into API coordinates, never
fetched as arbitrary URLs. These grants permit reads only; mutation targets stay
bound to the original signed PR request. Native reaction counts (`+1`, `-1`)
are included in context so the next answer can account for feedback. Reactions
do not start runs or grant permission to change code. Reads require the token's Issues and Pull
requests permissions. GitHub's 3,000-file PR API limit still applies.

The [agent template](../../examples/github-review/agent.json) grants this read
tool and specifies the structured review contract. Replace `YOUR_MODEL_ID`
with a model available to your configured OpenAI provider, then register it:

```sh
mandatum agent create -f examples/github-review/agent.json
```

Ordinary questions receive Markdown answers. Reviews return a JSON report with
`format: "mandatum.github.review.v1"`, which the sink converts to Markdown:
severity counts, summary, comments, an aligned findings table, related concepts,
and native reaction/mention instructions. Counts are derived from findings.
Every finding requires a suggestion, observed evidence with a source, and a
follow-up prompt. The formatter checks completeness, not the truth of claims.
Reports with missing evidence are visibly rejected rather than shown as clean.

Structured output is held while the run streams, then rendered in its existing
comment. Lifecycle-only routes also render the final report. Ordinary Markdown
answers retain their streaming behavior.

## Check progress

Set `MANDATUM_GITHUB_CHECKS=true` with a GitHub App token that has Checks write
permission ([GitHub check-run API](https://docs.github.com/en/rest/checks/runs)).
Each PR run gets a `mandatum/<agent>` check on its pinned head SHA.
The check shows queued/running state, tools and the final review. Failed runs or
review errors yield failure; invalid structured reports require action. Other
completed turns are neutral: completion alone does not prove tests passed.
Issues continue using comments without checks.

PR comment webhooks omit the head, so enabling checks resolves it through the
API before accepting the tag. Context reads reject a changed head rather than
silently reviewing newer code under an older check. Check recovery searches by
run ID and commit before creating, including after a lost create response.
GitHub provides no atomic create-if-absent check operation; multiple processes
must serialize delivery of the same run. Comment handles still live in memory,
so comment recovery across process restarts remains a limitation.

## PR guide and review threads

Enable the guide and identify the authenticated bot login:

```sh
export MANDATUM_GITHUB_GUIDES=true
export MANDATUM_GITHUB_BOTS='github-agent[bot]=github-agent'
```

The first accepted PR trigger posts one guide with intent, the current SHA,
changed-file navigation, review instructions and one bot mention. The same
comment updates on subsequent commits; each agent run still owns its separate
review. Guides are discovered by a stable marker and configured author, so
retries and connector restarts do not create another activation comment.
A copied marker in a human comment is ignored. Configure a bot mapping for every
agent that receives PR triggers while guides are enabled.

Guide updates compare the current PR head immediately before writing. Within a
connector, guide writes are serialized. GitHub does not offer compare-and-swap
comment updates: a commit arriving between the final head read and comment edit
can leave the guide temporarily stale until its new webhook is processed.
Multiple server instances need shared serialization for guide creation.

Tags in inline PR review comments answer inside the same native review thread,
including when the tag is itself a reply. The answer is edited as it streams.
Issue and PR conversation tags continue answering in the main conversation.
Explicit delivery overrides retain their configured destination.

## Requested fixes

Enable mutations and register the optional
[fixer spec](../../examples/github-review/fixer.json) after configuring its model:

```sh
export MANDATUM_GITHUB_MUTATIONS=true
mandatum agent create -f examples/github-review/fixer.json
```

A repository writer can request a fix in a PR conversation or review thread:

```text
@github-fixer /fix handle the retry timeout and add a regression test
```

`github_read_file` reads complete UTF-8 source at an immutable SHA before editing.
`github_apply_changes` creates one atomic commit with up to 20 file additions,
replacements or deletions, bounded to 500 KB total replacement content. The
source PR must be open and its branch must be in the same repository. Fork PRs
are read-only for this tool. Branch protections still apply.

Fixes require an explicit `/fix` mention from a person with effective GitHub
write permission. Permission is checked at acceptance and again before writing.
The connector signs a grant bound to the run ID, actor, repository, PR and head.
The runtime supplies the actual run identity separately from model arguments,
so copying a source URI into another invocation does not authorize a write.
The CLI uses the webhook secret as the signing key with a distinct protocol
prefix; mutations require at least 16 bytes. Rotating it invalidates old grants.
Automatic reviews and ordinary questions never receive a mutation grant.
Explicit `/check` requests receive a separate workflow-only grant.

Commits use GitHub's atomic `expectedHeadOid` comparison. If another commit wins,
the tool refuses to overwrite it. Retrying an identical request recovers its
commit when that commit is still the branch head, including after a lost API
response. A later branch update requires a fresh request. Results report the
actual commit SHA and URL, and explicitly state that this action did not run
tests. The GitHub token needs Contents write permission for commits and Metadata
read permission to check the requester. Workflow-file changes may need additional
GitHub permission.

## Running and observing checks

With mutations enabled, a repository writer can request:

```text
@github-fixer /check 123456789
```

The number is an existing GitHub Actions run ID. The connector binds the grant
to that workflow, its observed attempt, and the PR head. `github_run_checks`
reruns that exact run, so it retains the original commit and ref instead of
using a moving branch. `github_workflow_status` lists runs for a commit or polls
one run, including its attempt, state, conclusion and GitHub URL. The token needs
Actions read permission for status and Actions write permission for reruns.

Requested execution is reported as pending until GitHub returns a result. A
newer or currently running attempt is observed rather than restarted. GitHub
has no atomic compare-and-rerun endpoint: concurrent requests and the interval
before an accepted attempt becomes visible remain governed by GitHub's API.
No local shell commands or arbitrary workflows are dispatched. The repository
must already have a workflow run; commits can trigger its configured CI normally.

The UI is GitHub Markdown, as clarified in the request; no separate browser UI
or vitrum component integration is needed.

## Local preview

Run the formatter without a GitHub token or model:

```sh
go run ./examples/github-review
```

The [rendered preview](../../examples/github-review/preview.md) uses synthetic
findings to demonstrate severity counts, comments, the aligned evidence table,
related concepts, and native feedback instructions. It is not a repository review.
