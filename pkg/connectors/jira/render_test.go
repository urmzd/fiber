package jira_test

import (
	"net/http/httptest"
	"testing"

	"github.com/urmzd/fiber/pkg/connectors/internal/rendertest"
	fevent "github.com/urmzd/fiber/pkg/event"
)

// Each Jira trigger renders through the event catalog as its tag renders
// today.
func TestRenderMatchesRequestText(t *testing.T) {
	c, tags := start(t, botConfig())
	for _, tc := range []struct {
		id, body, want string
	}{
		{"b8a5f0c2-1111-2222-3333-444455556666", commentCreated, fevent.TypeJiraCommentCreated},
		{"assign-1", assigned, fevent.TypeJiraIssueAssigned},
		{"created-1", issueCreated, fevent.TypeJiraIssueCreated},
	} {
		c.ServeHTTP(httptest.NewRecorder(), deliver(t, tc.id, tc.body))
		if typ := rendertest.Check(t, receive(t, tags).Envelope); typ != tc.want {
			t.Errorf("%s: type = %s, want %s", tc.id, typ, tc.want)
		}
	}
}
