package github_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/urmzd/mandatum/pkg/connectors/github"
)

func TestPRGuideUpdatesAcrossCommitsWithoutDuplicateActivationComments(t *testing.T) {
	fake := github.NewFake()
	ref := github.Ref{Owner: "acme", Repo: "repo", Number: 42}
	cfg := github.Config{API: fake, Guides: true, Bots: map[string]string{"reviewer[bot]": "reviewer"}, AutoLabels: map[string]map[string]string{"acme/repo": {"auto": "reviewer"}}}
	c, tags := start(t, cfg)
	for i, sha := range []string{commitSHA, strings.Repeat("b", 40)} {
		evidence := github.Context{Subject: github.ContextSubject{Number: 42, Title: "Add feature", Body: "Why the feature matters"}, Files: []github.ContextFile{{Filename: "feature.go", Status: "modified", Patch: "+change"}}}
		evidence.Subject.Head.SHA = sha
		if err := fake.SeedContext(ref, evidence); err != nil {
			t.Fatal(err)
		}
		action := "labeled"
		if i > 0 {
			action = "synchronize"
		}
		raw := fmt.Sprintf(`{"action":%q,"repository":{"name":"repo","owner":{"login":"acme"}},"sender":{"type":"User"},"label":{"name":"auto"},"pull_request":{"number":42,"labels":[{"name":"auto"}],"head":{"sha":%q}}}`, action, sha)
		for retry := range 2 {
			w := httptest.NewRecorder()
			c.ServeHTTP(w, deliver(t, "pull_request", fmt.Sprintf("delivery-%d", i), raw))
			if w.Code != 200 {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			select {
			case <-tags:
			default:
				t.Fatal("missing tag")
			}
			if retry == 0 && i == 0 {
				c, tags = start(t, cfg)
			} // recovery against the same remote API
		}
		comments := fake.Comments()
		if len(comments) != 1 || comments[0].Edits != i || !strings.Contains(comments[0].Body, sha) || strings.Count(comments[0].Body, "@reviewer") != 1 {
			t.Fatalf("guide %+v", comments)
		}
	}
	// A stale update cannot replace the new guide.
	err := fake.UpdateGuide(context.Background(), ref, github.Guide{Agent: "reviewer", BotLogin: "reviewer", HeadSHA: commitSHA, Body: "stale"})
	if err == nil || strings.Contains(fake.Comments()[0].Body, "stale") {
		t.Fatal("stale guide replaced current one")
	}
}

func TestGuideRecoveryOnlyEditsTheConfiguredBotComment(t *testing.T) {
	for _, login := range []string{"human", "reviewer[bot]"} {
		t.Run(login, func(t *testing.T) {
			posts, patches := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == "GET" && strings.Contains(r.URL.Path, "/issues/"):
					_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 17, "body": "<!-- mandatum:pr-guide:reviewer -->\nold guide", "user": map[string]string{"login": login}}})
				case r.Method == "GET":
					fmt.Fprintf(w, `{"head":{"sha":%q}}`, commitSHA)
				case r.Method == "POST":
					posts++
					fmt.Fprint(w, `{"id":18}`)
				case r.Method == "PATCH":
					patches++
					if !strings.HasSuffix(r.URL.Path, "/17") {
						t.Error("wrong comment edited")
					}
					fmt.Fprint(w, `{}`)
				}
			}))
			defer server.Close()
			api := github.NewHTTPAPIAt(server.URL, "token", server.Client())
			err := api.UpdateGuide(context.Background(), github.Ref{Owner: "acme", Repo: "repo", Number: 42}, github.Guide{Agent: "reviewer", BotLogin: "reviewer", HeadSHA: commitSHA, Body: "new guide"})
			if err != nil {
				t.Fatal(err)
			}
			if login == "human" && (posts != 1 || patches != 0) {
				t.Fatal("human comment overwritten")
			}
			if login != "human" && (posts != 0 || patches != 1) {
				t.Fatal("bot guide not recovered")
			}
		})
	}
}

func TestGuideDoesNotWriteAfterThePRHeadChanges(t *testing.T) {
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			writes++
			fmt.Fprint(w, `{"id":1}`)
			return
		}
		if strings.Contains(r.URL.Path, "/issues/") {
			fmt.Fprint(w, `[]`)
		} else {
			fmt.Fprintf(w, `{"head":{"sha":%q}}`, strings.Repeat("b", 40))
		}
	}))
	defer server.Close()
	api := github.NewHTTPAPIAt(server.URL, "token", server.Client())
	err := api.UpdateGuide(context.Background(), github.Ref{Owner: "acme", Repo: "repo", Number: 42}, github.Guide{Agent: "reviewer", BotLogin: "reviewer", HeadSHA: commitSHA, Body: "guide"})
	if err == nil || writes != 0 {
		t.Fatalf("writes=%d error=%v", writes, err)
	}
}
