package slack

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/connectors/internal/inbound"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/signature"
)

// CommandsPath is the path suffix that receives slash command invocations. It
// is what an app's code_channels.slash_command_url ends in.
const CommandsPath = "/commands"

// serveCommand handles one slash command invocation, already authenticated.
//
// A command is a mention by another route: the human picked this app's command
// in a code channel, so the request is for an agent and there is no mention to
// read one from. The agent may still be named in the text; otherwise the
// default agent answers. The invocation is handed over as the text a human
// would have typed, "/create-pr fix the login test", so an agent needs no
// second input format to act on it.
func (c *Connector) serveCommand(w http.ResponseWriter, r *http.Request, body []byte) {
	form, err := url.ParseQuery(string(body))
	if err != nil {
		http.Error(w, "malformed payload", http.StatusBadRequest)
		return
	}
	tag, ok := c.tagFromCommand(form, r.Header)
	if !ok {
		http.Error(w, "malformed payload", http.StatusBadRequest)
		return
	}
	if tag.Agent == "" {
		// Slack shows this to the invoker alone. Silence would look like a
		// command that worked.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"response_type": "ephemeral",
			"text":          "No agent is configured to answer " + form.Get("command") + ".",
		})
		return
	}
	if c.cfg.Prepare != nil {
		if err := c.cfg.Prepare(r.Context(), &tag); err != nil {
			http.Error(w, "command refused", http.StatusForbidden)
			return
		}
	}
	if err := c.pipe.Send(r.Context(), tag); err != nil {
		http.Error(w, "not ready", inbound.SendStatus(err))
		return
	}
	// An empty 200 acknowledges without posting anything; the run's own message
	// is the answer.
	w.WriteHeader(http.StatusOK)
}

// tagFromCommand translates an invocation into a Tag. ok is false when the
// payload is not a usable invocation; a Tag with no Agent means it is one that
// nobody is configured to answer.
func (c *Connector) tagFromCommand(form url.Values, header http.Header) (envelope.Tag, bool) {
	team, channel, user := form.Get("team_id"), form.Get("channel_id"), form.Get("user_id")
	command, trigger := form.Get("command"), form.Get("trigger_id")
	if team == "" || channel == "" || user == "" || trigger == "" || !strings.HasPrefix(command, "/") {
		return envelope.Tag{}, false
	}
	if c.cfg.Team != "" && team != c.cfg.Team {
		return envelope.Tag{}, false
	}
	if err := address.ValidWorkspace(team); err != nil {
		return envelope.Tag{}, false
	}

	agent, args, _ := c.agentFor(strings.TrimSpace(form.Get("text")), nil)
	text := command
	if args != "" {
		text += " " + args
	}

	meta := map[string]string{
		"slack_team":    team,
		"slack_channel": channel,
		"slack_user":    user,
		"slack_event":   "slash_command",
		"slack_command": command,
	}
	if app := form.Get("api_app_id"); app != "" {
		meta["slack_app_id"] = app
	}

	return envelope.Tag{
		// trigger_id is unique per invocation, so a replayed request is the
		// same tag.
		ID:     "command:" + trigger,
		Agent:  agent,
		Origin: Name,
		// No thread: a code channel is the conversation, and its answers are
		// top-level messages.
		Source:  address.Address{Connector: Name, Workspace: team, Path: []string{channel}},
		Text:    text,
		Actor:   envelope.Actor{ID: user},
		Deliver: c.cfg.Deliver,
		Meta:    meta,
		At:      requestedAt(header),
	}, true
}

// requestedAt is when Slack sent the request. An invocation carries no
// timestamp of its own, and this header is covered by the signature the gate
// already checked.
func requestedAt(header http.Header) time.Time {
	sec, err := strconv.ParseInt(header.Get(signature.HeaderSlackTimestamp), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(sec, 0).UTC()
}
