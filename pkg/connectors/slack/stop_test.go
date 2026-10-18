package slack_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/urmzd/fiber/pkg/connectors/slack"
)

// sessionStopped is the delivery Slack documents for a stop on a thread
// session. A code channel sends the same event without thread_ts.
const sessionStopped = `{
  "team_id": "T01ABCDEF",
  "api_app_id": "A08APPID",
  "event": {
    "type": "agent_session_stopped",
    "user": "U0HUMAN1",
    "channel": "C02CHANNEL",
    "thread_ts": "1700000000.000100",
    "streaming_message_ts": [],
    "event_ts": "1700000095.000300"
  },
  "type": "event_callback",
  "event_id": "Ev08STOPPED",
  "event_time": 1700000095
}`

const codeChannelStopped = `{
  "team_id": "T01ABCDEF",
  "event": {"type": "agent_session_stopped", "user": "U0HUMAN1", "channel": "C09CODE", "streaming_message_ts": []},
  "type": "event_callback",
  "event_id": "Ev08STOPCODE"
}`

func stops(got *[]slack.Stop) func(context.Context, slack.Stop) error {
	return func(_ context.Context, s slack.Stop) error {
		*got = append(*got, s)
		return nil
	}
}

func TestAStopOnASessionReachesTheApplicationAndRaisesNoTag(t *testing.T) {
	cases := []struct {
		name, body, session string
	}{
		{"a thread session", sessionStopped, "slack://T01ABCDEF/C02CHANNEL?thread=1700000000.000100"},
		{"a code channel", codeChannelStopped, "slack://T01ABCDEF/C09CODE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []slack.Stop
			c, tags := start(t, slack.Config{Team: "T01ABCDEF", DefaultAgent: "docs-bot", Stopped: stops(&got)})

			w := httptest.NewRecorder()
			c.ServeHTTP(w, post(t, tc.body))

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", w.Code)
			}
			if len(got) != 1 || got[0].Session.String() != tc.session || got[0].User != "U0HUMAN1" {
				t.Fatalf("stops = %+v, want one for %s by U0HUMAN1", got, tc.session)
			}
			select {
			case tag := <-tags:
				t.Fatalf("a stop raised a tag: %+v", tag)
			default:
			}
		})
	}
}

func TestAStopThatFailsAsksSlackToRedeliver(t *testing.T) {
	c, _ := start(t, slack.Config{Stopped: func(context.Context, slack.Stop) error {
		return errors.New("store unavailable")
	}})

	w := httptest.NewRecorder()
	c.ServeHTTP(w, post(t, sessionStopped))

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 so Slack retries", w.Code)
	}
}

func TestAStopIsRefusedUnsignedIgnoredFromAnotherTeamAndHarmlessUnhandled(t *testing.T) {
	t.Run("unsigned", func(t *testing.T) {
		var got []slack.Stop
		c, _ := start(t, slack.Config{Stopped: stops(&got)})
		r := post(t, sessionStopped)
		r.Header.Set("X-Slack-Signature", "v0=00")

		w := httptest.NewRecorder()
		c.ServeHTTP(w, r)

		if w.Code == http.StatusOK || len(got) != 0 {
			t.Fatalf("status = %d, stops = %+v: an unsigned stop must not reach the application", w.Code, got)
		}
	})
	t.Run("another team", func(t *testing.T) {
		var got []slack.Stop
		c, _ := start(t, slack.Config{Team: "T09OTHER", Stopped: stops(&got)})

		w := httptest.NewRecorder()
		c.ServeHTTP(w, post(t, sessionStopped))

		if w.Code != http.StatusOK || len(got) != 0 {
			t.Fatalf("status = %d, stops = %+v: another team's stop is acknowledged and dropped", w.Code, got)
		}
	})
	t.Run("no handler", func(t *testing.T) {
		c, _ := start(t, slack.Config{})

		w := httptest.NewRecorder()
		c.ServeHTTP(w, post(t, sessionStopped))

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
	})
}
