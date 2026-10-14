// Command github-review renders the structured response format without a token
// or model. Its synthetic findings demonstrate the UI, not repository defects.
package main

import (
	"fmt"
	"log"

	"github.com/urmzd/mandatum/pkg/connectors/github"
)

func main() {
	report := github.ReviewReport{
		Format:   github.ReviewFormat,
		Summary:  "Example review: one error, one warning, and one informational finding. These are synthetic fixtures, not findings about this repository.",
		Comments: []string{"This preview demonstrates the comment layout. No repository code or live PR was reviewed.", "A real review cites the exact commit, observed test results, and source locations."},
		Findings: []github.ReviewFinding{
			{Severity: "error", Title: "Retry duplicates a write", Suggestion: "Reuse the request ID when retrying.", Evidence: []github.ReviewEvidence{{Observation: "Synthetic test: expected one write, observed two.", Source: "Fixture: retry_test.go:42"}}, Prompt: "Tag the bot with /fix to add a deduplication guard and regression test."},
			{Severity: "warning", Title: "Timeout recovery is untested", Suggestion: "Add a case for timeout after the server accepts the write.", Evidence: []github.ReviewEvidence{{Observation: "Synthetic test suite covers only the successful response.", Source: "Fixture: retry_test.go:18"}}, Prompt: "Ask the bot to inspect the failure path and propose a focused test."},
			{Severity: "info", Title: "Delivery remains ordered", Suggestion: "Preserve per-run ordering during the change.", Evidence: []github.ReviewEvidence{{Observation: "Synthetic delivery trace keeps event sequence 1, 2, 3.", Source: "Fixture: ordered-delivery trace"}}, Prompt: "Ask how ordering interacts with retry deduplication."},
		},
		Related: []github.ReviewLink{{Title: "GitHub check runs", URL: "https://docs.github.com/en/rest/checks/runs"}},
	}
	output, err := github.FormatReview(report)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(output)
}
