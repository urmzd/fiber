package slack

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/connectors/mention"
	"github.com/urmzd/fiber/pkg/event"
)

// eventEnvelope is the outer body of an Events API request. Only the fields the
// connector acts on are decoded: an unknown field is not an error, because Slack
// adds them without notice and a strict decoder would turn that into an outage.
type eventEnvelope struct {
	Type string `json:"type"`
	// Challenge answers a url_verification request.
	Challenge string `json:"challenge"`
	// TeamID is the workspace, and becomes the address workspace.
	TeamID string `json:"team_id"`
	// EventID is Slack's own identifier for this delivery and becomes the
	// event's id.
	// Slack reuses it across retries of the same event, which is exactly the
	// property an idempotency key needs.
	EventID string `json:"event_id"`
	// EventTime is when Slack observed the event, in whole seconds.
	EventTime int64 `json:"event_time"`
	// APIAppID identifies the app the event was delivered to.
	APIAppID string `json:"api_app_id"`

	Event innerEvent `json:"event"`
}

// innerEvent is the event itself.
type innerEvent struct {
	Type        string `json:"type"`
	Subtype     string `json:"subtype"`
	User        string `json:"user"`
	BotID       string `json:"bot_id"`
	Text        string `json:"text"`
	TS          string `json:"ts"`
	EventTS     string `json:"event_ts"`
	ThreadTS    string `json:"thread_ts"`
	Channel     string `json:"channel"`
	ChannelType string `json:"channel_type"`
	// Files are the files posted with the message, in the order Slack lists
	// them.
	Files []File `json:"files"`
}

// eventFrom translates a Slack event into a message.posted.v1 event, or
// reports that this event publishes nothing.
//
// Everything that decides "nothing" is here rather than spread through the
// handler, because "which events do we ignore" is a security-adjacent question:
// the loop-prevention rule (never act on a bot's message) is the difference
// between a mesh and a runaway.
//
// The event names the agents the message mentions (Config.Bots) and chooses
// none of them: the members of its channel decide who answers.
func (c *Connector) eventFrom(env eventEnvelope) (event.Envelope, bool) {
	if env.Type != "event_callback" {
		return event.Envelope{}, false
	}
	// One app's signing secret can authenticate multiple installations. A
	// verified delivery still has to belong to this connector's workspace.
	if c.cfg.Team != "" && env.TeamID != c.cfg.Team {
		return event.Envelope{}, false
	}
	ev := env.Event

	switch ev.Type {
	case "app_mention":
		// A mention in a channel: always a candidate.
	case "message":
		// A direct message to the app is a mention by context: there is nobody
		// else in the conversation. In a channel, a plain message is not:
		// answering every message in a channel we are in is not what joining a
		// channel means.
		if ev.ChannelType != "im" {
			return event.Envelope{}, false
		}
		if ev.Subtype != "" && ev.Subtype != "file_share" {
			// Joins, leaves, edits and the rest are not requests. A file
			// shared with a message is one: its files ride on the event.
			return event.Envelope{}, false
		}
	default:
		return event.Envelope{}, false
	}

	// Loop prevention, and the reason it is unconditional: this connector's sink
	// posts into the same channels its trigger reads. A bot message that reached
	// the agent would let the mesh answer itself.
	if ev.BotID != "" || ev.User == "" {
		return event.Envelope{}, false
	}
	if env.TeamID == "" || ev.Channel == "" {
		return event.Envelope{}, false
	}
	if err := address.ValidWorkspace(env.TeamID); err != nil {
		return event.Envelope{}, false
	}

	text, mentioned := decode(ev.Text, c.cfg.Bots)
	mentions := unique(mentioned)
	for _, agent := range mentions {
		text = mention.Strip(text, agent)
	}

	// The thread the answer belongs in: the mention's own thread if it is in one,
	// otherwise the mention itself, so the answer opens a thread under it rather
	// than adding a line to the channel.
	thread := ev.ThreadTS
	if thread == "" {
		thread = ev.TS
	}
	channel := address.Address{
		Connector: Name,
		Workspace: env.TeamID,
		Path:      []string{ev.Channel},
	}
	if thread != "" {
		channel = channel.WithParam("thread", thread)
	}

	id := env.EventID
	if id == "" {
		// Slack always sends event_id for the Events API. If a proxy stripped
		// it, the channel and the event timestamp still identify the event
		// uniquely, which keeps redelivery idempotent.
		id = ev.Channel + ":" + firstNonEmpty(ev.EventTS, ev.TS)
	}

	body := event.MessagePosted{
		Request: event.Request{Text: text, Actor: event.Actor{ID: ev.User}, Mentions: mentions},
		Slack: &event.SlackRef{
			Team:        env.TeamID,
			Channel:     ev.Channel,
			User:        ev.User,
			Event:       ev.Type,
			TS:          ev.TS,
			ThreadTS:    thread,
			ChannelType: ev.ChannelType,
			AppID:       env.APIAppID,
		},
	}
	return event.Envelope{
		ID:      id,
		Source:  channel.String(),
		Type:    body.Type(),
		Subject: ev.Channel,
		Channel: channel.String(),
		Time:    observedAt(env),
		Tenant:  c.cfg.Tenant,
		Data:    body,
	}, true
}

// unique keeps the first of each name, in order.
func unique(names []string) []string {
	var out []string
	for _, n := range names {
		if !slices.Contains(out, n) {
			out = append(out, n)
		}
	}
	return out
}

// observedAt is when Slack saw the event. The message timestamp is preferred
// over the envelope's whole-second event_time because it has microsecond
// resolution and is the same value Slack orders the channel by.
func observedAt(env eventEnvelope) time.Time {
	if t, ok := parseTS(firstNonEmpty(env.Event.EventTS, env.Event.TS)); ok {
		return t
	}
	if env.EventTime > 0 {
		return time.Unix(env.EventTime, 0).UTC()
	}
	return time.Time{}
}

// parseTS reads a Slack timestamp, "<seconds>.<microseconds>".
func parseTS(ts string) (time.Time, bool) {
	if ts == "" {
		return time.Time{}, false
	}
	secs, frac, _ := strings.Cut(ts, ".")
	sec, err := strconv.ParseInt(secs, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	var nsec int64
	if frac != "" {
		micro, err := strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		// Slack's fraction is six digits; pad or truncate rather than assuming.
		for i := len(frac); i < 6; i++ {
			micro *= 10
		}
		nsec = micro * 1000
	}
	return time.Unix(sec, nsec).UTC(), true
}

// decode turns Slack's wire text into text a model can read, and reports which
// agents were mentioned, in the order they appear.
//
// Slack sends entity-escaped text with its own markup for anything that refers
// to something: <@U123> for a user, <#C123|general> for a channel,
// <https://x|label> for a link, <!here> for a broadcast. A model reading the raw
// form learns opaque ids and stray angle brackets, so each construct is reduced
// to what a human sees — except a mention of an agent, which is reduced to
// "@name" so that mention stripping can then remove it the same way it does on
// every other surface.
func decode(raw string, bots map[string]string) (text string, mentioned []string) {
	var b strings.Builder
	b.Grow(len(raw))

	for i := 0; i < len(raw); {
		if raw[i] != '<' {
			b.WriteByte(raw[i])
			i++
			continue
		}
		end := strings.IndexByte(raw[i:], '>')
		if end < 0 {
			// An unbalanced "<" is literal text, not markup.
			b.WriteString(raw[i:])
			break
		}
		token := raw[i+1 : i+end]
		i += end + 1

		body, label, hasLabel := strings.Cut(token, "|")
		switch {
		case strings.HasPrefix(body, "@"):
			id := body[1:]
			if agent, ok := bots[id]; ok {
				mentioned = append(mentioned, agent)
				b.WriteString("@" + agent)
				continue
			}
			if hasLabel && label != "" {
				b.WriteString("@" + label)
				continue
			}
			// An unresolved user id is still information: someone was named. It
			// is written as "@<id>", which no agent name set will match.
			b.WriteString("@" + id)
		case strings.HasPrefix(body, "#"):
			if hasLabel && label != "" {
				b.WriteString("#" + label)
				continue
			}
			b.WriteString("#" + body[1:])
		case strings.HasPrefix(body, "!"):
			// A broadcast: <!here>, <!channel>, <!subteam^S123|@team>.
			if hasLabel && label != "" {
				b.WriteString(label)
				continue
			}
			b.WriteString("@" + body[1:])
		default:
			// A link. The label is what the human saw; without one, the URL is.
			if hasLabel && label != "" {
				b.WriteString(label)
				continue
			}
			b.WriteString(body)
		}
	}

	return unescape(strings.TrimSpace(b.String())), mentioned
}

// unescape reverses the three entities Slack escapes on the way in. It runs
// after markup reduction so that a "&gt;" in the text cannot be mistaken for the
// end of a markup token.
func unescape(s string) string {
	if !strings.Contains(s, "&") {
		return s
	}
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	// Ampersand last: doing it first would turn "&amp;lt;" into "<".
	return strings.ReplaceAll(s, "&amp;", "&")
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
