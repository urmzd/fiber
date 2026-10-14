🔴 **1 error** · 🟡 **1 warning** · 🔵 **1 info**

## Summary

Example review: one error, one warning, and one informational finding. These are synthetic fixtures, not findings about this repository.

## Comments

- This preview demonstrates the comment layout. No repository code or live PR was reviewed.
- A real review cites the exact commit, observed test results, and source locations.

## Findings

| Level | Finding | Suggestion | Evidence | Follow-up prompt |
| :---: | :--- | :--- | :--- | :--- |
| 🔴 Error | Retry duplicates a write | Reuse the request ID when retrying. | Synthetic test: expected one write, observed two. (Fixture: retry&#95;test.go:42) | Tag the bot with /fix to add a deduplication guard and regression test. |
| 🟡 Warning | Timeout recovery is untested | Add a case for timeout after the server accepts the write. | Synthetic test suite covers only the successful response. (Fixture: retry&#95;test.go:18) | Ask the bot to inspect the failure path and propose a focused test. |
| 🔵 Info | Delivery remains ordered | Preserve per-run ordering during the change. | Synthetic delivery trace keeps event sequence 1, 2, 3. (Fixture: ordered-delivery trace) | Ask how ordering interacts with retry deduplication. |

## Related concepts

- GitHub check runs: [source](https://docs.github.com/en/rest/checks/runs)

---

React with 👍 or 👎 on this comment. Reply and tag the agent to continue the review.
