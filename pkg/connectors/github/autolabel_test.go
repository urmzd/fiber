package github_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/urmzd/mandatum/pkg/connectors/github"
)

func TestAutomaticLabelsAreExplicitAndRepositoryScoped(t *testing.T) {
	for _, tc := range []struct {
		name, event, action, owner, repo, label, sender, delivery string
		want                                                      bool
	}{
		{"issue label", "issues", "labeled", "acme", "repo", "agent:auto", "User", "delivery", true},
		{"pull label", "pull_request", "labeled", "acme", "repo", "agent:auto", "User", "delivery", true},
		{"repository case", "issues", "labeled", "ACME", "Repo", "agent:auto", "User", "delivery", true},
		{"other repository", "issues", "labeled", "acme", "other", "agent:auto", "User", "delivery", false},
		{"other owner", "issues", "labeled", "other", "repo", "agent:auto", "User", "delivery", false},
		{"other label", "issues", "labeled", "acme", "repo", "bug", "User", "delivery", false},
		{"label removed", "issues", "unlabeled", "acme", "repo", "agent:auto", "User", "delivery", false},
		{"bot loop", "issues", "labeled", "acme", "repo", "agent:auto", "Bot", "delivery", false},
		{"missing delivery", "issues", "labeled", "acme", "repo", "agent:auto", "User", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, tags := start(t, github.Config{Owners: []string{"acme"}, AutoLabels: map[string]map[string]string{"acme/repo": {"agent:auto": "reviewer"}}})
			resource := "issue"
			if tc.event == "pull_request" {
				resource = "pull_request"
			}
			payload, err := json.Marshal(map[string]any{
				"action": tc.action, "repository": map[string]any{"name": tc.repo, "owner": map[string]string{"login": tc.owner}},
				"sender": map[string]string{"login": "human", "type": tc.sender},
				"label":  map[string]string{"name": tc.label},
				resource: map[string]any{"number": 42, "title": "Review this", "body": "Context"},
			})
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			c.ServeHTTP(w, deliver(t, tc.event, tc.delivery, string(payload)))
			if w.Code != 200 {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			select {
			case tag := <-tags:
				if !tc.want {
					t.Fatalf("unexpected tag: %+v", tag)
				}
				if tag.Agent != "reviewer" || tag.ID != tc.delivery || tag.Text != "Review this\n\nContext" {
					t.Fatalf("incorrect tag: %+v", tag)
				}
			default:
				if tc.want {
					t.Fatal("missing tag")
				}
			}
		})
	}
}

func TestInvalidAutomaticLabelConfigurationIsRejected(t *testing.T) {
	for _, mappings := range []map[string]map[string]string{
		{"repo": {"auto": "reviewer"}},
		{"acme/repo": {"": "reviewer"}},
		{"acme/repo": {"auto": "bad:name"}},
		{"acme/repo": {"auto": "reviewer"}, "ACME/Repo": {"auto": "other"}},
	} {
		if _, err := github.New(github.Config{API: github.NewFake(), AutoLabels: mappings}); err == nil {
			t.Fatalf("accepted invalid mapping: %v", mappings)
		}
	}
}

func TestAutomaticLabelsRespectOwnerRestrictions(t *testing.T) {
	c, tags := start(t, github.Config{
		Owners:     []string{"allowed"},
		AutoLabels: map[string]map[string]string{"blocked/repo": {"auto": "reviewer"}},
	})
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "issues", "delivery", `{"action":"labeled","repository":{"name":"repo","owner":{"login":"blocked"}},"sender":{"type":"User"},"issue":{"number":1},"label":{"name":"auto"}}`))
	if w.Code != 200 {
		t.Fatalf("status %d", w.Code)
	}
	select {
	case tag := <-tags:
		t.Fatalf("owner restriction bypassed: %+v", tag)
	default:
	}
}
