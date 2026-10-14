package github_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/connectors/github"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/envelope"
)

var commitSHA = strings.Repeat("a", 40)

func TestChecksStreamAndKeepTheirPinnedCommit(t *testing.T) {
	fake := github.NewFake()
	c, err := github.New(github.Config{API: fake, Checks: true, Interval: -1})
	if err != nil {
		t.Fatal(err)
	}
	target := address.MustParse("github://acme/repo/pull/42").WithParam("head_sha", commitSHA)
	events := []envelope.Event{event(1, envelope.KindAccepted, `{}`), event(2, envelope.KindStarted, `{}`), event(3, envelope.KindToolCall, `{"id":"t1","name":"github_read_context"}`), event(4, envelope.KindCompleted, `{"text":"Review complete. Tests not run."}`)}
	for _, e := range events {
		for range 2 {
			if err := c.Deliver(context.Background(), target, e); err != nil {
				t.Fatal(err)
			}
		}
	}
	checks := fake.Checks()
	if len(checks) != 1 || checks[0].Check.HeadSHA != commitSHA || checks[0].Check.Status != "completed" || checks[0].Check.Conclusion != "neutral" || checks[0].Edits != 3 {
		t.Fatalf("checks %+v", checks)
	}
	if len(fake.Comments()) != 1 {
		t.Fatal("duplicate comments")
	}
	// A restarted sink finds the check rather than creating another check run.
	recovered, _ := github.New(github.Config{API: fake, Checks: true, Interval: -1})
	if err := recovered.Deliver(context.Background(), target, events[3]); err != nil {
		t.Fatal(err)
	}
	if len(fake.Checks()) != 1 {
		t.Fatal("duplicate check after recovery")
	}
}

func TestCheckConclusionReflectsReviewErrorsAndMalformedReports(t *testing.T) {
	for _, tc := range []struct{ text, conclusion string }{
		{`{"format":"mandatum.github.review.v1","summary":"Found a bug","findings":[{"severity":"error","title":"Bug","suggestion":"Fix it","evidence":[{"observation":"Failure","source":"test.go:1"}],"prompt":"Add a regression test"}]}`, "failure"},
		{`{"format":"mandatum.github.review.v1","summary":"Missing evidence","findings":[{"severity":"error"}]}`, "action_required"},
	} {
		fake := github.NewFake()
		c, _ := github.New(github.Config{API: fake, Checks: true})
		p, _ := json.Marshal(map[string]string{"text": tc.text})
		if err := c.Deliver(context.Background(), address.MustParse("github://acme/repo/pull/1").WithParam("head_sha", commitSHA), event(1, envelope.KindCompleted, string(p))); err != nil {
			t.Fatal(err)
		}
		if got := fake.Checks()[0].Check.Conclusion; got != tc.conclusion {
			t.Fatalf("conclusion %s", got)
		}
	}
}

func TestCheckRecoveryUsesExternalIDAndNeverPatchesTheHead(t *testing.T) {
	posts, patches := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			fmt.Fprintf(w, `{"check_runs":[{"id":17,"external_id":"run-1","name":"mandatum/reviewer","head_sha":%q}]}`, commitSHA)
		case "PATCH":
			patches++
			var patch map[string]any
			_ = json.NewDecoder(r.Body).Decode(&patch)
			if _, ok := patch["head_sha"]; ok {
				t.Error("patch moved check head")
			}
			fmt.Fprint(w, `{}`)
		case "POST":
			posts++
			fmt.Fprint(w, `{"id":18}`)
		}
	}))
	defer server.Close()
	api := github.NewHTTPAPIAt(server.URL, "token", server.Client())
	id, err := api.EnsureCheck(context.Background(), github.Ref{Owner: "acme", Repo: "repo", Number: 1}, github.Check{Name: "mandatum/reviewer", ExternalID: "run-1", HeadSHA: commitSHA, Status: "in_progress"})
	if err != nil || id != 17 || posts != 0 || patches != 1 {
		t.Fatalf("id=%d posts=%d patches=%d err=%v", id, posts, patches, err)
	}
}

func TestLabeledPRCommitsTriggerPinnedReviews(t *testing.T) {
	for _, tc := range []struct {
		name, action, sender string
		labels               []string
		draft, want          bool
	}{
		{"new commit", "synchronize", "User", []string{"auto"}, false, true},
		{"bot fix commit", "synchronize", "Bot", []string{"auto"}, false, true},
		{"inactive", "synchronize", "User", nil, false, false},
		{"draft", "synchronize", "User", []string{"auto"}, true, false},
		{"bot mention loop", "opened", "Bot", []string{"auto"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, tags := start(t, github.Config{AutoLabels: map[string]map[string]string{"acme/repo": {"auto": "reviewer"}}})
			labels := []map[string]string{}
			for _, l := range tc.labels {
				labels = append(labels, map[string]string{"name": l})
			}
			raw, _ := json.Marshal(map[string]any{"action": tc.action, "repository": map[string]any{"name": "repo", "owner": map[string]string{"login": "acme"}}, "sender": map[string]string{"type": tc.sender}, "pull_request": map[string]any{"number": 42, "title": "Change", "state": "open", "draft": tc.draft, "labels": labels, "head": map[string]string{"sha": commitSHA}}})
			w := httptest.NewRecorder()
			c.ServeHTTP(w, deliver(t, "pull_request", "delivery", string(raw)))
			if w.Code != 200 {
				t.Fatalf("status %d", w.Code)
			}
			select {
			case tag := <-tags:
				if !tc.want || tag.Source.Params["head_sha"] != commitSHA || tag.Meta["github_head_sha"] != commitSHA || !strings.Contains(tag.Text, commitSHA) {
					t.Fatalf("tag %+v", tag)
				}
			default:
				if tc.want {
					t.Fatal("missing review")
				}
			}
		})
	}
}

func TestPRCommentPinsTheHeadBeforeAcceptance(t *testing.T) {
	fake := github.NewFake()
	evidence := github.Context{Subject: github.ContextSubject{Number: 42}}
	evidence.Subject.Head.SHA = commitSHA
	if err := fake.SeedContext(github.Ref{Owner: "urmzd", Repo: "mandatum", Number: 42}, evidence); err != nil {
		t.Fatal(err)
	}
	c, tags := start(t, github.Config{API: fake, Checks: true, Agents: mention.NewNames("docs-bot")})
	raw := strings.Replace(issueComment, `"number": 42,`, `"number": 42, "pull_request":{"url":"https://api.github.com/repos/urmzd/mandatum/pulls/42"},`, 1)
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "issue_comment", "pin-comment", raw))
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	select {
	case tag := <-tags:
		if tag.Source.Params["head_sha"] != commitSHA {
			t.Fatalf("unpinned tag %+v", tag)
		}
	default:
		t.Fatal("missing tag")
	}
}

func TestPinnedReviewCannotReadEvidenceFromANewerCommit(t *testing.T) {
	fake := github.NewFake()
	evidence := github.Context{Subject: github.ContextSubject{Number: 42}}
	evidence.Subject.Head.SHA = strings.Repeat("b", 40)
	if err := fake.SeedContext(github.Ref{Owner: "acme", Repo: "repo", Number: 42}, evidence); err != nil {
		t.Fatal(err)
	}
	c, _ := github.New(github.Config{API: fake})
	target := address.MustParse("github://acme/repo/pull/42").WithParam("head_sha", commitSHA)
	_, err := c.Actions()[0].Invoke(context.Background(), target, json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "PR head changed") {
		t.Fatalf("expected stale evidence error, got %v", err)
	}
}

func TestIssuesDoNotCreateCommitChecks(t *testing.T) {
	fake := github.NewFake()
	c, _ := github.New(github.Config{API: fake, Checks: true})
	if err := c.Deliver(context.Background(), address.MustParse("github://acme/repo/issues/1"), event(1, envelope.KindStarted, `{}`)); err != nil {
		t.Fatal(err)
	}
	if len(fake.Checks()) != 0 || len(fake.Comments()) != 1 {
		t.Fatal("incorrect issue delivery")
	}
}

func TestAutomaticCommitReviewIdentityIgnoresDeliveryRetries(t *testing.T) {
	c, tags := start(t, github.Config{AutoLabels: map[string]map[string]string{"acme/repo": {"auto": "reviewer"}}})
	raw := fmt.Sprintf(`{"action":"synchronize","repository":{"name":"repo","owner":{"login":"acme"}},"sender":{"type":"User"},"pull_request":{"number":42,"labels":[{"name":"auto"}],"head":{"sha":%q}}}`, commitSHA)
	ids := []string{}
	for _, delivery := range []string{"first", "second"} {
		w := httptest.NewRecorder()
		c.ServeHTTP(w, deliver(t, "pull_request", delivery, raw))
		if w.Code != 200 {
			t.Fatalf("status %d", w.Code)
		}
		select {
		case tag := <-tags:
			ids = append(ids, tag.ID)
		default:
			t.Fatal("missing tag")
		}
	}
	if ids[0] != ids[1] {
		t.Fatalf("one commit has two run identities: %v", ids)
	}
}
