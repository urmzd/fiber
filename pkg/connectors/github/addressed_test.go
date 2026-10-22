package github_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/urmzd/fiber/pkg/connectors/github"
	"github.com/urmzd/fiber/pkg/connectors/mention"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/topic"
)

// addressed is a connector whose app comments as fiber[bot] and runs
// docs-bot, with triage answering what is addressed to the app alone.
func addressed(t *testing.T) (*github.Connector, <-chan envelope.Tag) {
	t.Helper()
	return start(t, github.Config{Agents: mention.NewNames("docs-bot"), Login: "fiber[bot]", DefaultAgent: "triage"})
}

// comment is issueComment with its body replaced.
func comment(body string) string {
	return strings.Replace(issueComment,
		`"body": "can @docs-bot summarize what changed here, and @docs-bot check the retry path too?"`,
		`"body": `+quote(body), 1)
}

func quote(s string) string {
	out, _ := json.Marshal(s)
	return string(out)
}

func serve(t *testing.T, c *github.Connector, event, delivery, body string) {
	t.Helper()
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, event, delivery, body))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", w.Code, w.Body.String())
	}
}

func silent(t *testing.T, tags <-chan envelope.Tag) {
	t.Helper()
	select {
	case tag := <-tags:
		t.Errorf("raised a tag for %q: %q", tag.Agent, tag.Text)
	default:
	}
}

// A comment that mentions the app and names no agent is addressed to the app,
// as Slack delivers a mention of its app, and the default agent answers it
// without the app's mention in its text.
func TestACommentToTheAppThatNamesNoAgentIsTheDefaultAgents(t *testing.T) {
	c, tags := addressed(t)
	serve(t, c, "issue_comment", "d-app", comment("@fiber what changed here?"))
	tag := receive(t, tags)
	if tag.Agent != "triage" || tag.Text != "what changed here?" {
		t.Fatalf("tag is %q with %q, want triage with the app's mention taken out", tag.Agent, tag.Text)
	}
	if tag.Source.String() != "github://urmzd/fiber/issues/42" || tag.Actor.ID != "urmzd" || tag.Meta["github_comment_id"] != "2201234567" {
		t.Fatalf("tag is %+v", tag)
	}
	if err := tag.Validate(); err != nil {
		t.Fatal(err)
	}
}

// An agent the comment names answers it, wherever the app is mentioned, and
// the app's mention leaves the text as the agent's does.
func TestAnAgentNamedBesideTheAppAnswers(t *testing.T) {
	c, tags := addressed(t)
	for delivery, body := range map[string]string{
		"d-first": "@fiber @docs-bot summarize",
		"d-after": "@docs-bot summarize, @fiber",
	} {
		serve(t, c, "issue_comment", delivery, comment(body))
		tag := receive(t, tags)
		if tag.Agent != "docs-bot" || !strings.HasPrefix(tag.Text, "summarize") || strings.Contains(tag.Text, "@") {
			t.Fatalf("%q raised %q with %q, want docs-bot without either mention", body, tag.Agent, tag.Text)
		}
	}
}

// Every human event that reads text reads it the same way: a review, a
// review comment, a new issue and a new pull request addressed to the app are
// the default agent's.
func TestEveryHumanEventAddressedToTheAppIsTheDefaultAgents(t *testing.T) {
	pull := map[string]any{
		"number": 7, "title": "Coalesce edits", "body": "@fiber does this look right?", "state": "open",
		"user": map[string]string{"login": "urmzd", "type": "User"}, "head": map[string]string{"ref": "x", "sha": "deadbeef"},
	}
	repo := map[string]any{"name": "fiber", "full_name": "urmzd/fiber", "owner": map[string]string{"login": "urmzd", "type": "User"}}
	sender := map[string]string{"login": "urmzd", "type": "User"}
	cases := map[string]map[string]any{
		"pull_request_review": {
			"action": "submitted", "pull_request": pull, "repository": repo, "sender": sender,
			"review": map[string]any{"id": 3, "body": "@fiber does this look right?", "user": sender},
		},
		"pull_request_review_comment": {
			"action": "created", "pull_request": pull, "repository": repo, "sender": sender,
			"comment": map[string]any{"id": 4, "body": "@fiber does this look right?", "user": sender},
		},
		"issues": {
			"action": "opened", "repository": repo, "sender": sender,
			"issue": map[string]any{"number": 9, "title": "Retries", "body": "@fiber does this look right?", "state": "open", "user": sender},
		},
		"pull_request": {"action": "opened", "pull_request": pull, "repository": repo, "sender": sender},
	}
	c, tags := addressed(t)
	for event, body := range cases {
		raw, _ := json.Marshal(body)
		serve(t, c, event, "d-"+event, string(raw))
		tag := receive(t, tags)
		if tag.Agent != "triage" || !strings.HasSuffix(tag.Text, "does this look right?") || strings.Contains(tag.Text, "@fiber") {
			t.Fatalf("%s raised %q with %q", event, tag.Agent, tag.Text)
		}
	}
}

// Nothing is raised for a comment that addresses neither the app nor an
// agent, so a busy repository wakes nobody; nor for one addressed to the app
// with no default to answer it, or by a connector that does not know its own
// login; nor for the app's own comments, however they mention it.
func TestACommentAddressedToNobodyRaisesNothing(t *testing.T) {
	c, tags := addressed(t)
	serve(t, c, "issue_comment", "d-nobody", comment("@urmzd can you take a look? fiber@example.com knows"))
	silent(t, tags)
	serve(t, c, "issue_comment", "d-bot", strings.Replace(comment("@fiber what changed?"),
		`"sender": {"login": "urmzd", "type": "User"}`, `"sender": {"login": "fiber[bot]", "type": "Bot"}`, 1))
	silent(t, tags)

	for name, cfg := range map[string]github.Config{
		"no default": {Agents: mention.NewNames("docs-bot"), Login: "fiber[bot]"},
		"no login":   {Agents: mention.NewNames("docs-bot"), DefaultAgent: "triage"},
	} {
		c, tags := start(t, cfg)
		serve(t, c, "issue_comment", "d-"+name, comment("@fiber what changed?"))
		silent(t, tags)
	}
}

// A host that decides who answers once the tag reaches it gives a name of its
// own as the default. fiber's control name is one no agent can take, so a tag
// for it can only be resolved by the host before the run is accepted.
func TestAHostMayLeaveTheDefaultForItselfToResolve(t *testing.T) {
	c, tags := start(t, github.Config{Agents: mention.NewNames("docs-bot"), Login: "fiber", DefaultAgent: topic.ControlName})
	serve(t, c, "issue_comment", "d-host", comment("@Fiber what changed?"))
	if tag := receive(t, tags); tag.Agent != topic.ControlName || tag.Text != "what changed?" {
		t.Fatalf("tag is %q with %q", tag.Agent, tag.Text)
	}
}

func TestALoginOrDefaultThatCannotBeUsedIsRefused(t *testing.T) {
	for name, cfg := range map[string]github.Config{
		"a login with a space":       {Login: "fiber bot"},
		"a login with a slash":       {Login: "acme/fiber"},
		"a bare suffix":              {Login: "[bot]"},
		"a default that is no agent": {Login: "fiber", DefaultAgent: "docs bot"},
	} {
		cfg.API = github.NewFake()
		if _, err := github.New(cfg); err == nil {
			t.Errorf("%s: New accepted %+v", name, cfg)
		}
	}
}

// A login in Bots names its agent wherever it is mentioned, the app's own
// too. So when Bots maps the app's login, its mention is a tag for that agent
// and DefaultAgent is not asked, also when another agent is named after it:
// the first agent the text names answers.
func TestTheAppsLoginInBotsIsATagForItsAgent(t *testing.T) {
	c, tags := start(t, github.Config{
		Agents: mention.NewNames("docs-bot"), Bots: map[string]string{"fiber[bot]": "ops"},
		Login: "fiber[bot]", DefaultAgent: "triage",
	})
	for delivery, body := range map[string]string{
		"d-alone":  "@fiber what changed?",
		"d-before": "@fiber @docs-bot summarize",
	} {
		serve(t, c, "issue_comment", delivery, comment(body))
		if tag := receive(t, tags); tag.Agent != "ops" || strings.Contains(tag.Text, "@fiber") {
			t.Fatalf("%q raised %q with %q, want ops without the app's mention", body, tag.Agent, tag.Text)
		}
	}
}
