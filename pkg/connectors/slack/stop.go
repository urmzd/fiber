package slack

import (
	"context"
	"net/http"

	"github.com/urmzd/mandatum/pkg/address"
)

// StopEvent is the Events API type Slack sends when a person presses stop on an
// agent session. Subscribing to it is what makes Slack show the button: without
// the subscription a processing session has a loading indicator and no way out.
const StopEvent = "agent_session_stopped"

// Stop is a person asking an agent session to stop.
type Stop struct {
	// Session addresses the session the same way Tag.Source addresses a
	// mention: the channel, with ?thread= for a thread session. A code channel
	// is its own session and carries no thread.
	Session address.Address
	// User is the Slack user id of whoever pressed stop.
	User string
}

// stopFrom reads a stop request out of a verified event. ok is false for an
// event that is not one, or that names no session this connector serves.
func (c *Connector) stopFrom(env eventEnvelope) (Stop, bool) {
	ev := env.Event
	if env.Type != "event_callback" || ev.Type != StopEvent {
		return Stop{}, false
	}
	if env.TeamID == "" || ev.Channel == "" || ev.User == "" {
		return Stop{}, false
	}
	if c.cfg.Team != "" && env.TeamID != c.cfg.Team {
		return Stop{}, false
	}
	if err := address.ValidWorkspace(env.TeamID); err != nil {
		return Stop{}, false
	}
	session := address.Address{Connector: Name, Workspace: env.TeamID, Path: []string{ev.Channel}}
	if ev.ThreadTS != "" {
		session = session.WithParam("thread", ev.ThreadTS)
	}
	return Stop{Session: session, User: ev.User}, true
}

// serveStop hands a stop request to the application.
//
// The connector knows sessions, not runs: which work a session stands for, and
// whether it can be stopped at all, is the application's to say. A failure is
// answered with a 500 so Slack redelivers; stopping is idempotent, so a retry
// of one that half-succeeded is safe.
func (c *Connector) serveStop(ctx context.Context, w http.ResponseWriter, stop Stop) {
	if c.cfg.Stopped == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	if err := c.cfg.Stopped(ctx, stop); err != nil {
		http.Error(w, "stop failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
