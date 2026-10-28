package runtime

import (
	"net/url"
	"strings"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/event"
)

// TriggerOf reads a tag as the event a run answers: message.posted.v1 with
// the tag's text, actor, files and meta (as context, which renders the same
// request text a tag always did). The source is the tag's surface address,
// or "<origin>:<tenant>" for a tag with none, which is not an address, so the
// run has no surface and its thread is its own.
func TriggerOf(tag envelope.Tag) event.Envelope {
	req := event.Request{Text: tag.Text, Actor: tag.Actor, Attachments: tag.Attachments}
	if len(tag.Meta) > 0 {
		req.Context = tag.Meta
	}
	if tag.Agent != "" {
		req.Mentions = []string{tag.Agent}
	}
	source := tag.Source.String()
	if tag.Source.IsZero() {
		origin := tag.Origin
		if origin == "" {
			origin = "fiber"
		}
		source = origin + ":" + url.PathEscape(TenantOf(tag))
	}
	return event.Envelope{
		ID:      tag.ID,
		Source:  source,
		Type:    event.TypeMessagePosted,
		Channel: ChannelOf(tag),
		Time:    tag.At,
		Tenant:  tag.Tenant,
		Data:    event.MessagePosted{Request: req},
	}
}

// runID is the run this input belongs to.
func (in Input) runID() string {
	if in.Run != "" {
		return in.Run
	}
	return RunID(in.Trigger.Tenant, in.Trigger.ID)
}

// Tag is the trigger read back as the tag TriggerOf made it from. The thread
// journal, the surface reconciler and the answer rules still speak tags.
func (in Input) Tag() envelope.Tag {
	req, _ := event.RequestOf(in.Trigger.Data)
	t := envelope.Tag{
		ID:          in.Trigger.ID,
		Tenant:      in.Trigger.Tenant,
		Agent:       in.Subscriber,
		Text:        req.Text,
		Actor:       req.Actor,
		Meta:        req.Context,
		Attachments: req.Attachments,
		At:          in.Trigger.Time,
	}
	if a, err := address.Parse(in.Trigger.Source); err == nil {
		t.Source, t.Origin = a, a.Connector
	} else {
		t.Origin, _, _ = strings.Cut(in.Trigger.Source, ":")
	}
	return t
}
