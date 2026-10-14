package slack_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/connectors/slack"
)

// invocation is a slash command request as Slack documents it.
func invocation(overrides map[string]string) string {
	form := url.Values{
		"token":        {"XXYYZZ"},
		"team_id":      {"T01ABCDEF"},
		"team_domain":  {"acme"},
		"channel_id":   {"C09CODE"},
		"channel_name": {"fix-flaky-login"},
		"user_id":      {"U02HUMAN"},
		"user_name":    {"alice"},
		"command":      {"/create-pr"},
		"text":         {"Fix the flaky login test"},
		"api_app_id":   {"A01APP"},
		"response_url": {"https://hooks.slack.com/commands/T01ABCDEF/1/abc"},
		"trigger_id":   {"13345224609.738474920.8088930838d88f008e0"},
	}
	for k, v := range overrides {
		if v == "" {
			form.Del(k)
			continue
		}
		form.Set(k, v)
	}
	return form.Encode()
}

func command(t *testing.T, body string) *http.Request {
	t.Helper()
	r := post(t, body)
	r.URL.Path = slack.CommandsPath
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

func TestASlashCommandBecomesATagForTheDefaultAgent(t *testing.T) {
	c, tags := start(t, slack.Config{DefaultAgent: "docs-bot"})

	w := httptest.NewRecorder()
	c.ServeHTTP(w, command(t, invocation(nil)))
	if w.Code != http.StatusOK || w.Body.Len() != 0 {
		t.Fatalf("status = %d body = %q, want an empty 200", w.Code, w.Body)
	}

	tag := <-tags
	if err := tag.Validate(); err != nil {
		t.Fatalf("tag is not valid: %v", err)
	}
	if tag.Agent != "docs-bot" {
		t.Errorf("agent = %q", tag.Agent)
	}
	if tag.Text != "/create-pr Fix the flaky login test" {
		t.Errorf("text = %q, want the invocation as a human would have typed it", tag.Text)
	}
	if got := tag.Source.String(); got != "slack://T01ABCDEF/C09CODE" {
		t.Errorf("source = %q, want the code channel with no thread", got)
	}
	if tag.Actor.ID != "U02HUMAN" {
		t.Errorf("actor = %q", tag.Actor.ID)
	}
	if tag.Meta["slack_command"] != "/create-pr" || tag.Meta["slack_event"] != "slash_command" {
		t.Errorf("meta = %v", tag.Meta)
	}
	if !tag.At.Equal(signedAt) {
		t.Errorf("at = %v, want the signed request time %v", tag.At, signedAt)
	}
}

func TestReplayingAnInvocationRaisesTheSameTag(t *testing.T) {
	c, tags := start(t, slack.Config{DefaultAgent: "docs-bot"})

	var ids []string
	for range 2 {
		c.ServeHTTP(httptest.NewRecorder(), command(t, invocation(nil)))
		ids = append(ids, (<-tags).ID)
	}
	if ids[0] != ids[1] {
		t.Errorf("ids = %v, want one idempotency key for one invocation", ids)
	}
}

func TestASlashCommandCanNameItsAgentInTheText(t *testing.T) {
	c, tags := start(t, slack.Config{
		Agents:       mention.NewNames("docs-bot", "release-bot"),
		DefaultAgent: "docs-bot",
	})

	c.ServeHTTP(httptest.NewRecorder(), command(t, invocation(map[string]string{"text": "@release-bot cut 1.2"})))

	tag := <-tags
	if tag.Agent != "release-bot" || tag.Text != "/create-pr cut 1.2" {
		t.Errorf("agent = %q text = %q", tag.Agent, tag.Text)
	}
}

func TestASlashCommandNobodyAnswersSaysSoToItsInvoker(t *testing.T) {
	c, tags := start(t, slack.Config{})

	w := httptest.NewRecorder()
	c.ServeHTTP(w, command(t, invocation(nil)))

	var reply map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatalf("body %q is not JSON: %v", w.Body, err)
	}
	if w.Code != http.StatusOK || reply["response_type"] != "ephemeral" || !strings.Contains(reply["text"], "/create-pr") {
		t.Errorf("status = %d reply = %v", w.Code, reply)
	}
	select {
	case tag := <-tags:
		t.Errorf("a command with no agent raised %+v", tag)
	default:
	}
}

func TestAnUnsignedSlashCommandIsRefusedBeforeItIsParsed(t *testing.T) {
	c, tags := start(t, slack.Config{DefaultAgent: "docs-bot"})

	cases := map[string]func(*http.Request){
		"no signature":    func(r *http.Request) { r.Header.Del("X-Slack-Signature") },
		"wrong signature": func(r *http.Request) { r.Header.Set("X-Slack-Signature", "v0="+strings.Repeat("0", 64)) },
		"tampered body": func(r *http.Request) {
			*r = *command(t, invocation(nil))
			r.Body = http.NoBody
		},
	}
	for name, tamper := range cases {
		t.Run(name, func(t *testing.T) {
			r := command(t, invocation(nil))
			tamper(r)
			w := httptest.NewRecorder()
			c.ServeHTTP(w, r)
			if w.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", w.Code)
			}
		})
	}
	select {
	case tag := <-tags:
		t.Errorf("an unauthenticated command raised %+v", tag)
	default:
	}
}

func TestMalformedInvocationsRaiseNoTag(t *testing.T) {
	c, tags := start(t, slack.Config{DefaultAgent: "docs-bot"})

	for _, missing := range []string{"team_id", "channel_id", "user_id", "trigger_id", "command"} {
		t.Run(missing, func(t *testing.T) {
			w := httptest.NewRecorder()
			c.ServeHTTP(w, command(t, invocation(map[string]string{missing: ""})))
			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", w.Code)
			}
		})
	}
	select {
	case tag := <-tags:
		t.Errorf("a malformed command raised %+v", tag)
	default:
	}
}
