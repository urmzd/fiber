package github_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/connectors/github"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/envelope"
)

func workflowAction(t *testing.T, c *github.Connector, name string) connector.Action {
	t.Helper()
	for _, a := range c.Actions() {
		if a.Name == name {
			return a
		}
	}
	t.Fatal("missing " + name)
	return connector.Action{}
}

func TestCheckRequestRerunsOnlyTheAuthorizedAttemptAndPollsRealResults(t *testing.T) {
	fake := github.NewFake()
	ref := github.Ref{Owner: "acme", Repo: "repo", Number: 42}
	evidence := github.Context{Subject: github.ContextSubject{Number: 42, State: "open"}}
	evidence.Subject.Head.SHA = commitSHA
	if err := fake.SeedContext(ref, evidence); err != nil {
		t.Fatal(err)
	}
	fake.SetWritePermission(ref, "human", true)
	initial := github.WorkflowRun{ID: 123, HeadSHA: commitSHA, Attempt: 1, Status: "completed", Conclusion: "failure", URL: "https://github.com/acme/repo/actions/runs/123"}
	if err := fake.SeedWorkflow(ref, initial); err != nil {
		t.Fatal(err)
	}
	c, tags := start(t, github.Config{API: fake, Mutations: true, MutationSecret: []byte("a stable signing secret for tests"), Agents: mention.NewNames("reviewer")})
	raw := `{"action":"created","repository":{"name":"repo","owner":{"login":"acme"}},"sender":{"login":"human","type":"User"},"issue":{"number":42,"pull_request":{"url":"https://api.github.com/repos/acme/repo/pulls/42"}},"comment":{"id":91,"body":"@reviewer /check 123"}}`
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "issue_comment", "check-request-1", raw))
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var tag envelope.Tag
	select {
	case tag = <-tags:
	default:
		t.Fatal("missing tag")
	}
	ctx := connector.WithRunID(context.Background(), tag.ID)
	execute := workflowAction(t, c, "github_run_checks")
	result, err := execute.Invoke(ctx, tag.Source, json.RawMessage(`{}`))
	if err != nil || !strings.Contains(result.Summary, "Execution has not been verified") {
		t.Fatalf("result %+v err=%v", result, err)
	}
	for range 2 {
		if _, err := execute.Invoke(ctx, tag.Source, json.RawMessage(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	current, _ := fake.WorkflowRun(ctx, ref, 123)
	if current.Attempt != 2 || current.Status != "queued" || current.Conclusion != "" {
		t.Fatalf("invented completion or duplicate rerun: %+v", current)
	}
	poll := workflowAction(t, c, "github_workflow_status")
	result, err = poll.Invoke(ctx, tag.Source, json.RawMessage(`{"run_id":123}`))
	if err != nil || !strings.Contains(result.Summary, `"status":"queued"`) {
		t.Fatalf("poll %+v %v", result, err)
	}
	current.Status = "completed"
	current.Conclusion = "success"
	if err := fake.SeedWorkflow(ref, current); err != nil {
		t.Fatal(err)
	}
	// Retrying the original authorized attempt after completion must not rerun it again.
	if _, err := execute.Invoke(ctx, tag.Source, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	final, _ := fake.WorkflowRun(ctx, ref, 123)
	if final.Attempt != 2 || final.Conclusion != "success" {
		t.Fatal("completed retry started another attempt")
	}
	for key, value := range map[string]string{"write_workflow": "456", "write_attempt": "2", "head_sha": strings.Repeat("b", 40)} {
		if _, err := execute.Invoke(ctx, tag.Source.WithParam(key, value), json.RawMessage(`{}`)); err == nil {
			t.Fatalf("altered %s allowed", key)
		}
	}
	if _, err := execute.Invoke(connector.WithRunID(ctx, "other-run"), tag.Source, json.RawMessage(`{}`)); err == nil {
		t.Fatal("cross-run check grant accepted")
	}
}

func TestWorkflowRerunUsesExactRunEndpointAndObservesNewerAttempts(t *testing.T) {
	posts := 0
	attempt := 1
	status := "completed"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			if r.URL.Path != "/repos/acme/repo/actions/runs/123/rerun" {
				t.Error("wrong rerun endpoint")
			}
			posts++
			attempt++
			status = "queued"
			w.WriteHeader(201)
			return
		}
		_ = json.NewEncoder(w).Encode(github.WorkflowRun{ID: 123, HeadSHA: commitSHA, Attempt: attempt, Status: status})
	}))
	defer server.Close()
	api := github.NewHTTPAPIAt(server.URL, "token", server.Client())
	ref := github.Ref{Owner: "acme", Repo: "repo", Number: 42}
	first, err := api.RerunWorkflow(context.Background(), ref, 123, commitSHA, 1)
	if err != nil || !first.Requested {
		t.Fatalf("first %+v error %v", first, err)
	}
	status = "completed"
	second, err := api.RerunWorkflow(context.Background(), ref, 123, commitSHA, 1)
	if err != nil || second.Requested || second.Attempt != 2 || posts != 1 {
		t.Fatalf("second %+v posts=%d err=%v", second, posts, err)
	}
	if _, err := api.RerunWorkflow(context.Background(), ref, 123, strings.Repeat("b", 40), 2); err == nil || posts != 1 {
		t.Fatal("reran a different commit")
	}
}

func TestWorkflowListsKeepPaginationAndCommitFilter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("head_sha") != commitSHA || r.URL.Query().Get("page") != "2" {
			t.Error("lost filter")
		}
		runs := make([]github.WorkflowRun, 30)
		for i := range runs {
			runs[i] = github.WorkflowRun{ID: int64(i + 1), HeadSHA: commitSHA}
		}
		_ = json.NewEncoder(w).Encode(github.WorkflowPage{Runs: runs})
	}))
	defer server.Close()
	api := github.NewHTTPAPIAt(server.URL, "token", server.Client())
	result, err := api.WorkflowRuns(context.Background(), github.Ref{Owner: "acme", Repo: "repo", Number: 42}, commitSHA, 2)
	if err != nil || result.NextPage != 3 || len(result.Runs) != 30 {
		t.Fatal(fmt.Sprint(result, err))
	}
}
