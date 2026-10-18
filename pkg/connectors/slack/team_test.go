package slack_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/urmzd/fiber/pkg/connectors/slack"
	"github.com/urmzd/fiber/pkg/envelope"
)

func TestVerifiedMessagesStayWithinTheConfiguredTeam(t *testing.T) {
	for _, event := range []string{"app_mention", "message"} {
		for _, tc := range []struct {
			name, configured, delivered string
			accepted                    bool
		}{
			{"same team", "T01ABCDEF", "T01ABCDEF", true},
			{"other team", "T01ABCDEF", "T09OTHER", false},
			{"missing team", "T01ABCDEF", "", false},
			{"unbound connector", "", "T09OTHER", true},
		} {
			t.Run(event+"/"+tc.name, func(t *testing.T) {
				prepared := false
				c, tags := start(t, slack.Config{
					Team: tc.configured, DefaultAgent: "docs-bot",
					Prepare: func(_ context.Context, _ *envelope.Tag) error {
						prepared = true
						return nil
					},
				})
				body := strings.ReplaceAll(appMention, "T01ABCDEF", tc.delivered)
				body = strings.Replace(body, `"type": "app_mention"`, `"type": "`+event+`", "channel_type": "im"`, 1)
				w := httptest.NewRecorder()
				c.ServeHTTP(w, post(t, body))
				if w.Code != http.StatusOK || prepared != tc.accepted {
					t.Fatalf("status=%d prepared=%v, want 200/%v", w.Code, prepared, tc.accepted)
				}
				if tc.accepted {
					if tag := receive(t, tags); tag.Source.Workspace != tc.delivered {
						t.Fatalf("workspace=%q, want %q", tag.Source.Workspace, tc.delivered)
					}
				} else {
					select {
					case tag := <-tags:
						t.Fatalf("out-of-team delivery raised a tag: %+v", tag)
					default:
					}
				}
			})
		}
	}
}
