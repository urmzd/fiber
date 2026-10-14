package runtime_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/duraturo"
	"github.com/urmzd/duraturo/pkg/ledger"
	dqueue "github.com/urmzd/duraturo/pkg/queue"
	"github.com/urmzd/duraturo/pkg/worker"
	"github.com/urmzd/legatus/pkg/controlplane"
	"github.com/urmzd/legatus/pkg/metrics"
	"github.com/urmzd/legatus/pkg/node/inproc"
	"github.com/urmzd/legatus/pkg/tool"
	"github.com/urmzd/legatus/pkg/workspace"
	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/mandatum/pkg/agentrt"
	"github.com/urmzd/mandatum/pkg/agentrt/payload"
	"github.com/urmzd/mandatum/pkg/agentspec"
	"github.com/urmzd/mandatum/pkg/bus"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/connectors/github"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/registry"
	"github.com/urmzd/mandatum/pkg/router"
	"github.com/urmzd/mandatum/pkg/runtime"
	"github.com/urmzd/mandatum/pkg/signature"
)

// githubIntegrationBus preserves the real bus and records the runtime's output.
// Readiness comes from the actual router subscription, not an arbitrary sleep.
type githubIntegrationBus struct {
	bus.Bus
	subscribed chan struct{}
	once       sync.Once
	observed   recorder
}

func (b *githubIntegrationBus) Subscribe(ctx context.Context, sub envelope.Subscription) (bus.Stream, error) {
	stream, err := b.Bus.Subscribe(ctx, sub)
	if err == nil {
		b.once.Do(func() { close(b.subscribed) })
	}
	return stream, err
}
func (b *githubIntegrationBus) Publish(ctx context.Context, event envelope.Event) error {
	if err := b.Bus.Publish(ctx, event); err != nil {
		return err
	}
	return b.observed.Publish(ctx, event)
}

func TestGitHubCollaborationFlowsThroughDurableSandboxAndRouter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	const secret = "github-integration-test-secret-32"
	firstSHA, secondSHA := strings.Repeat("a", 40), strings.Repeat("b", 40)
	ref := github.Ref{Owner: "acme", Repo: "service", Number: 42}
	api := github.NewFake()
	seed := func(sha string) {
		t.Helper()
		evidence := github.Context{Subject: github.ContextSubject{Number: 42, Title: "Handle retries", State: "open"}, Files: []github.ContextFile{{Filename: "retry.go", Status: "modified", Patch: "+return nil"}}, ReviewComments: []github.ContextComment{{ID: 91, Body: "Explain this path", Reactions: &github.ReactionCounts{Total: 2, ThumbsUp: 1, ThumbsDown: 1}}}}
		evidence.Subject.Head.SHA = sha
		if err := api.SeedContext(ref, evidence); err != nil {
			t.Fatal(err)
		}
		if err := api.SeedFiles(ref, map[string]string{"retry.go": "package service\n"}); err != nil {
			t.Fatal(err)
		}
	}
	seed(firstSHA)
	api.SetWritePermission(ref, "maintainer", true)
	gh, err := github.New(github.Config{API: api, Verifier: signature.NewGitHub(secret), Owners: []string{"acme"}, Bots: map[string]string{"github-agent[bot]": "github-agent"}, AutoLabels: map[string]map[string]string{"acme/service": {"agent:auto": "github-agent"}}, Checks: true, Guides: true, Mutations: true, MutationSecret: []byte(secret), Interval: -1})
	if err != nil {
		t.Fatal(err)
	}
	connectors := connector.NewRegistry()
	if err := connectors.Register(gh); err != nil {
		t.Fatal(err)
	}
	store := registry.NewMemory()
	raw, err := os.ReadFile("../../examples/github-review/fixer.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec agentspec.AgentSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	spec.Name = "github-agent"
	spec.Provider = agentrt.ProviderOffline
	spec.Model = "scripted"
	if _, err := store.Create(ctx, "acme", spec, "test"); err != nil {
		t.Fatal(err)
	}
	report := func(sha string) string {
		encoded, err := json.Marshal(github.ReviewReport{Format: github.ReviewFormat, Summary: "Reviewed commit " + sha, Comments: []string{"Read the complete file. Tests not run."}, Findings: []github.ReviewFinding{{Severity: "info", Title: "Package declaration", Suggestion: "Preserve the package name.", Evidence: []github.ReviewEvidence{{Observation: "retry.go declares package service.", Source: "https://github.com/acme/service/blob/" + sha + "/retry.go#L1"}}, Prompt: "Ask about the retry implementation."}}})
		if err != nil {
			t.Fatal(err)
		}
		return string(encoded)
	}
	scripts := [][][]saigetypes.Delta{
		{agenttest.ToolCallResponse("context", "github_read_context", map[string]any{}), agenttest.ToolCallResponse("file", "github_read_file", map[string]any{"path": "retry.go"}), agenttest.TextResponse(report(firstSHA))},
		{agenttest.ToolCallResponse("context", "github_read_context", map[string]any{}), agenttest.TextResponse(report(secondSHA))},
		{agenttest.ToolCallResponse("context", "github_read_context", map[string]any{}), agenttest.TextResponse("The thread context includes both thumbs-up and thumbs-down feedback.")},
		{agenttest.ToolCallResponse("file", "github_read_file", map[string]any{"path": "retry.go"}), agenttest.ToolCallResponse("fix", "github_apply_changes", map[string]any{"message": "fix: document retry", "files": []map[string]any{{"path": "retry.go", "content": "package service\n// Retry requests retain their identity.\n"}}}), agenttest.TextResponse("Applied the requested fix. Tests have not been run.")},
		{agenttest.ToolCallResponse("rerun", "github_run_checks", map[string]any{}), agenttest.ToolCallResponse("poll", "github_workflow_status", map[string]any{"run_id": 123}), agenttest.TextResponse("Workflow attempt 2 is queued; it has not passed yet.")},
	}
	var scriptMu sync.Mutex
	scriptIndex := 0
	provider := agentrt.WithProvider(agentrt.ProviderOffline, func(agentrt.Spec) (saigetypes.Provider, error) {
		scriptMu.Lock()
		defer scriptMu.Unlock()
		if scriptIndex >= len(scripts) {
			return nil, fmt.Errorf("unexpected extra model turn")
		}
		script := scripts[scriptIndex]
		scriptIndex++
		return &agenttest.ScriptedProvider{Responses: script}, nil
	})
	ws, err := workspace.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tools := tool.NewRegistry()
	plane := controlplane.NewMemory(inproc.NewFactory(tools, ws), metrics.Nop())
	history := registry.NewSpecs(store)
	sandbox, err := runtime.NewSandbox(plane, tools, history, runtime.WithCatalog(runtime.Connectors(connectors)), runtime.WithAgentOptions(provider), runtime.WithSandboxLogger(quiet))
	if err != nil {
		t.Fatal(err)
	}
	broker := &githubIntegrationBus{Bus: bus.NewMemory(), subscribed: make(chan struct{})}
	lgr, q := ledger.NewMemory(), dqueue.NewMemory()
	durable, err := runtime.New(duraturo.New(lgr, q), history, broker, sandbox, runtime.WithLogger(quiet))
	if err != nil {
		t.Fatal(err)
	}
	routing := router.New(broker, connectors, router.WithLogger(quiet))
	routerDone, workerDone, ingestDone := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	tags := make(chan envelope.Tag, 8)
	go func() { routerDone <- routing.Run(ctx) }()
	go func() {
		workerDone <- worker.New(lgr, q, worker.WithRegistry(durable.Registry()), worker.WithConcurrency(1), worker.WithJanitorEvery(0), worker.WithLogger(quiet)).Run(ctx)
	}()
	go func() { ingestDone <- gh.Ingest(ctx, tags) }()
	t.Cleanup(func() { cancel(); <-routerDone; <-workerDone; <-ingestDone })
	select {
	case <-broker.subscribed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	wait := func(ready func() bool) {
		t.Helper()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for !ready() {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	wait(gh.Ingesting)
	webhook := func(event, id, action, sha, body string) envelope.Tag {
		t.Helper()
		data := map[string]any{"action": action, "repository": map[string]any{"name": "service", "owner": map[string]string{"login": "acme"}}, "sender": map[string]string{"login": "maintainer", "type": "User"}, "pull_request": map[string]any{"number": 42, "state": "open", "head": map[string]string{"sha": sha}, "labels": []map[string]string{{"name": "agent:auto"}}}, "label": map[string]string{"name": "agent:auto"}}
		if event == "pull_request_review_comment" {
			data["comment"] = map[string]any{"id": 92, "in_reply_to_id": 91, "body": body}
		}
		if event == "issue_comment" {
			delete(data, "pull_request")
			data["issue"] = map[string]any{"number": 42, "pull_request": map[string]string{"url": "https://api.github.com/repos/acme/service/pulls/42"}}
			data["comment"] = map[string]any{"id": 101, "body": body}
		}
		encoded, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(encoded)
		request := httptest.NewRequest(http.MethodPost, "/github", strings.NewReader(string(encoded)))
		request.Header.Set(github.HeaderEvent, event)
		request.Header.Set(github.HeaderDelivery, id)
		request.Header.Set(signature.HeaderGitHubSignature, "sha256="+hex.EncodeToString(mac.Sum(nil)))
		response := httptest.NewRecorder()
		gh.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("webhook %d: %s", response.Code, response.Body.String())
		}
		select {
		case tag := <-tags:
			tag.Tenant = "acme"
			return tag
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return envelope.Tag{}
		}
	}
	run := func(tag envelope.Tag) string {
		t.Helper()
		id := runtime.RunID(tag.ID)
		if err := routing.Register(id, tag.Routes()); err != nil {
			t.Fatal(err)
		}
		accepted, err := durable.Accept(ctx, tag)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := durable.Result(ctx, accepted.RunID); err != nil {
			t.Fatal(err)
		}
		wait(func() bool {
			for _, check := range api.Checks() {
				if check.Check.ExternalID == id && check.Check.Status == "completed" {
					return true
				}
			}
			return false
		})
		return id
	}
	assertAction := func(runID, name, contains string) {
		t.Helper()
		for _, event := range broker.observed.ofKind(envelope.KindActionTaken) {
			if event.RunID != runID {
				continue
			}
			action, err := payload.Decode[payload.Action](event)
			if err != nil {
				t.Fatal(err)
			}
			if action.Name == name {
				if action.Error != "" || !strings.Contains(action.Summary, contains) {
					t.Fatalf("action %+v", action)
				}
				return
			}
		}
		t.Fatalf("no %s action in %s", name, runID)
	}
	first := webhook("pull_request", "initial-label", "labeled", firstSHA, "")
	firstRun := run(first)
	assertAction(firstRun, "github_read_file", "package service")
	if len(api.Comments()) != 2 {
		t.Fatalf("want guide and review, got %+v", api.Comments())
	}
	if body := api.Comments()[1].Body; !strings.Contains(body, "🔵 **1 info**") || !strings.Contains(body, "| Level |") || strings.Contains(body, github.ReviewFormat) {
		t.Fatalf("unformatted review %s", body)
	}
	// Signed webhook redelivery joins the same durable run and does not rerun the model.
	if duplicate := run(webhook("pull_request", "initial-label", "labeled", firstSHA, "")); duplicate != firstRun {
		t.Fatal("duplicate run identity")
	}
	if len(api.Comments()) != 2 || len(api.Checks()) != 1 {
		t.Fatal("duplicate delivery created new output")
	}
	seed(secondSHA)
	secondRun := run(webhook("pull_request", "commit-change", "synchronize", secondSHA, ""))
	assertAction(secondRun, "github_read_context", secondSHA)
	if len(api.Comments()) != 3 || !strings.Contains(api.Comments()[0].Body, secondSHA) {
		t.Fatal("guide was not updated separately from new review")
	}
	threadRun := run(webhook("pull_request_review_comment", "thread-question", "created", secondSHA, "@github-agent explain this"))
	assertAction(threadRun, "github_read_context", `"+1":1`)
	comments := api.Comments()
	if comments[len(comments)-1].ReviewRoot != 91 {
		t.Fatal("reply left the review thread")
	}
	fixRun := run(webhook("issue_comment", "fix-request", "created", secondSHA, "@github-agent /fix document retry identity"))
	assertAction(fixRun, "github_apply_changes", "Created commit")
	commits := api.Commits()
	if len(commits) != 1 || commits[0].Parent != secondSHA {
		t.Fatalf("commits %+v", commits)
	}
	changedSHA := commits[0].Result.SHA
	if err := api.SeedWorkflow(ref, github.WorkflowRun{ID: 123, HeadSHA: changedSHA, Attempt: 1, Status: "completed", Conclusion: "failure", URL: "https://github.com/acme/service/actions/runs/123"}); err != nil {
		t.Fatal(err)
	}
	checkRun := run(webhook("issue_comment", "check-request", "created", changedSHA, "@github-agent /check 123"))
	assertAction(checkRun, "github_run_checks", "Requested a rerun")
	assertAction(checkRun, "github_workflow_status", `"status":"queued"`)
	observed, err := api.WorkflowRun(ctx, ref, 123)
	if err != nil || observed.Attempt != 2 || observed.Conclusion != "" {
		t.Fatalf("workflow %+v error %v", observed, err)
	}
	scriptMu.Lock()
	calls := scriptIndex
	scriptMu.Unlock()
	if calls != 5 {
		t.Fatalf("executed %d model turns, want five", calls)
	}
	for _, check := range api.Checks() {
		if check.Check.HeadSHA == "" || check.Check.Conclusion != "neutral" {
			t.Fatalf("invalid completion claim %+v", check)
		}
	}
}
