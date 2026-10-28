package event_test

import (
	"strings"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/attachment"
	"github.com/urmzd/fiber/pkg/event"
)

func ingressSamples() []event.Body {
	req := event.Request{
		Text: "summarise this", Actor: event.Actor{ID: "U07", Display: "ada"}, Mentions: []string{"helper"},
		Attachments: []attachment.Attachment{{Digest: strings.Repeat("a", 64), MediaType: "text/plain", Name: "notes.txt", Size: 5}},
		Context:     map[string]string{"project": "handbook"},
	}
	gh := event.GitHubRef{
		Owner: "acme", Repo: "handbook", Number: 42, Pull: true, Title: "Fix the docs", Sender: "ada",
		State: "open", Author: "ada", URL: "https://github.com/acme/handbook/pull/42", BaseRef: "main",
		HeadRef: "fix", HeadSHA: strings.Repeat("b", 40), Labels: []string{"docs", "review"}, InstallationID: 99,
	}
	jira := event.JiraRef{
		Site: "acme", Key: "PROJ-7", WebhookEvent: "comment_created", IssueEvent: "issue_commented", ID: "10007",
		Summary: "Broken build", Status: "Open", IssueType: "Bug", Priority: "High", Project: "PROJ",
		ProjectName: "Project", Assignee: "Ada", Reporter: "Grace", Labels: []string{"ci"},
		URL: "https://acme.atlassian.net/browse/PROJ-7",
	}
	return []event.Body{
		event.MessagePosted{Request: req, Slack: &event.SlackRef{
			Team: "T01", Channel: "C02", User: "U07", Event: "app_mention", TS: "1699.002", ThreadTS: "1699.001",
			ChannelType: "channel", AppID: "A01",
		}},
		event.SlackCommandInvoked{Request: req, Command: "/ask", Team: "T01", Channel: "C02", User: "U07", AppID: "A01"},
		event.SlackSessionStopped{Team: "T01", Channel: "C02", ThreadTS: "1699.001", User: "U07"},
		event.GitHubIssueCommentCreated{Request: req, GitHubRef: gh, CommentID: 7},
		event.GitHubReviewCommentCreated{Request: req, GitHubRef: gh, CommentID: 8, ReviewRootID: 6},
		event.GitHubReviewSubmitted{Request: req, GitHubRef: gh, CommentID: 9},
		event.GitHubIssueOpened{Request: req, GitHubRef: event.GitHubRef{Owner: "acme", Repo: "handbook", Number: 43, Title: "Docs", Sender: "ada"}},
		event.GitHubIssueLabeled{Request: req, GitHubRef: event.GitHubRef{Owner: "acme", Repo: "handbook", Number: 43, Title: "Docs", Sender: "ada", Labels: []string{"triage"}}},
		event.GitHubPullOpened{Request: req, GitHubRef: gh},
		event.GitHubPullReadyForReview{Request: req, GitHubRef: gh},
		event.GitHubReviewRequested{Request: req, GitHubRef: gh},
		event.GitHubPullSynchronize{Request: req, GitHubRef: gh},
		event.GitHubPullLabeled{Request: req, GitHubRef: gh},
		event.JiraCommentCreated{Request: req, JiraRef: jira, CommentID: "100"},
		event.JiraIssueCreated{Request: req, JiraRef: jira},
		event.JiraIssueAssigned{Request: req, JiraRef: jira},
		event.CronTick{Request: req, Schedule: "nightly", Expression: "0 3 * * *", Occurrence: at},
	}
}

func renderOf(t *testing.T, e event.Envelope) string {
	t.Helper()
	entry, ok := event.Lookup(e.Type)
	if !ok {
		t.Fatalf("%s is not registered", e.Type)
	}
	out, err := entry.Render(e)
	if err != nil {
		t.Fatalf("render %s: %v", e.Type, err)
	}
	return out
}

func TestIngressRendersTheTriggerContext(t *testing.T) {
	for _, tc := range []struct {
		body event.Body
		want string
	}{
		{event.MessagePosted{Request: event.Request{Text: "hi"}}, "hi"},
		{event.MessagePosted{Request: event.Request{Text: "hi", Context: map[string]string{"b": "2", "a": "1"}}},
			"Context from the trigger:\n- a: 1\n- b: 2\n\nhi"},
		{event.MessagePosted{Request: event.Request{Text: "hi"}, Slack: &event.SlackRef{Team: "T", Channel: "C", User: "U", Event: "message"}},
			"Context from the trigger:\n- slack_channel: C\n- slack_event: message\n- slack_team: T\n- slack_ts: \n- slack_user: U\n\nhi"},
		{event.SlackCommandInvoked{Request: event.Request{Text: "/ask x"}, Command: "/ask", Team: "T", Channel: "C", User: "U"},
			"Context from the trigger:\n- slack_channel: C\n- slack_command: /ask\n- slack_event: slash_command\n- slack_team: T\n- slack_user: U\n\n/ask x"},
		{event.GitHubIssueOpened{Request: event.Request{Text: "t"}, GitHubRef: event.GitHubRef{Owner: "o", Repo: "r", Number: 1, Title: "T", Sender: "s", Draft: true}},
			"Context from the trigger:\n- github_action: opened\n- github_event: issues\n- github_kind: issue\n- github_number: 1\n- github_owner: o\n- github_repo: r\n- github_sender: s\n- github_title: T\n\nt"},
		{event.JiraIssueAssigned{Request: event.Request{Text: "t"}, JiraRef: event.JiraRef{Site: "s", Key: "K-1", WebhookEvent: "jira:issue_updated"}},
			"Context from the trigger:\n- jira_assigned: true\n- jira_event: jira:issue_updated\n- jira_key: K-1\n- jira_site: s\n\nt"},
		{event.CronTick{Request: event.Request{Text: "t", Context: map[string]string{"cron_schedule": "spoofed"}}, Schedule: "n", Expression: "* * * * *", Occurrence: at.In(time.FixedZone("x", 3600))},
			"Context from the trigger:\n- cron_expression: * * * * *\n- cron_occurrence: 2026-10-28T14:03:11Z\n- cron_schedule: n\n\nt"},
	} {
		if got := renderOf(t, sample(tc.body)); got != tc.want {
			t.Errorf("%s renders\n%q, want\n%q", tc.body.Type(), got, tc.want)
		}
	}
}

func TestRequestTextLeavesOutPrivateKeys(t *testing.T) {
	for _, c := range []struct {
		meta map[string]string
		want string
	}{
		{map[string]string{"channel": "ops", "_pinned": "x"}, "Context from the trigger:\n- channel: ops\n\nhi"},
		{map[string]string{"_pinned": "x"}, "hi"},
		{nil, "hi"},
	} {
		if got := event.RequestText("hi", c.meta); got != c.want {
			t.Errorf("RequestText(%v) = %q, want %q", c.meta, got, c.want)
		}
	}
}

// Private data and "_" context keys never reach a model.
func TestPrivateNeverRendered(t *testing.T) {
	const secret = "secret-4711"
	for _, b := range ingressSamples() {
		e := sample(withPrivateContext(b, secret))
		e.Private = event.Private{"_installation": secret, "token": secret}
		if got := renderOf(t, e); strings.Contains(got, secret) {
			t.Errorf("%s rendered private data: %q", e.Type, got)
		}
	}
}

// withPrivateContext adds a "_" context key to a body that has a Request.
func withPrivateContext(b event.Body, v string) event.Body {
	add := func(r event.Request) event.Request {
		ctx := map[string]string{"_host.sources": v}
		for k, x := range r.Context {
			ctx[k] = x
		}
		r.Context = ctx
		return r
	}
	switch x := b.(type) {
	case event.MessagePosted:
		x.Request = add(x.Request)
		return x
	case event.SlackCommandInvoked:
		x.Request = add(x.Request)
		return x
	case event.GitHubIssueCommentCreated:
		x.Request = add(x.Request)
		return x
	case event.JiraCommentCreated:
		x.Request = add(x.Request)
		return x
	case event.CronTick:
		x.Request = add(x.Request)
		return x
	}
	return b
}

// Every ingress body that carries a Request carries text a person wrote, so
// it is sensitive. A stop carries none.
func TestIngressSensitivity(t *testing.T) {
	for _, b := range ingressSamples() {
		e, _ := event.Lookup(b.Type())
		_, stop := b.(event.SlackSessionStopped)
		if e.Sensitive == stop {
			t.Errorf("%s: Sensitive = %v", b.Type(), e.Sensitive)
		}
	}
}

func TestGitHubTypeCoversEveryGitHubBody(t *testing.T) {
	for _, typ := range event.Types() {
		if event.Family(typ) != "github" {
			continue
		}
		segs := strings.Split(event.Base(typ), ".")
		if got, ok := event.GitHubType(segs[1], segs[2]); !ok || got != typ {
			t.Errorf("GitHubType(%q, %q) = %q, %v", segs[1], segs[2], got, ok)
		}
	}
	if _, ok := event.GitHubType("push", ""); ok {
		t.Error("GitHubType(push) is registered")
	}
}
