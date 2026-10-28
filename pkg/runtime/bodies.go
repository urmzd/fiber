package runtime

import (
	"net/url"
	"strconv"
	"time"

	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/topic"
)

// DefaultTenant is the tenant a run's events carry when its tag names none.
// The bus requires a tenant on every event.
const DefaultTenant = "default"

// TenantOf is the tenant a tag's run publishes its events under.
func TenantOf(tag envelope.Tag) string { return busTenant(tag.Tenant) }

func busTenant(tenant string) string {
	if tenant == "" {
		return DefaultTenant
	}
	return tenant
}

// controlChannel is where requests of one verb for a run are published. A
// run id that could not be a run's is refused.
func controlChannel(verb, tenant, runID string) (string, error) {
	if _, err := topic.Run("probe", runID); err != nil {
		return "", err
	}
	return event.ControlChannel(busTenant(tenant), verb, runID), nil
}

// requestID names one stop or compact request, so the bus keeps each request
// once and a listener that reads it again after resubscribing knows it.
func requestID(verb, runID string, at time.Time) string {
	return verb + ":" + runID + ":" + strconv.FormatInt(at.UnixNano(), 10)
}

// ChannelOf is the channel a tag's run publishes its events on: the address
// the tag came from, or, for a trigger with no surface such as a schedule,
// <origin>://<tenant>/<origin>.
func ChannelOf(tag envelope.Tag) string {
	if !tag.Source.IsZero() {
		return tag.Source.String()
	}
	scheme := tag.Origin
	if scheme == "" {
		scheme = "fiber"
	}
	return scheme + "://" + url.PathEscape(TenantOf(tag)) + "/" + url.PathEscape(tag.Origin)
}

// The runtime keeps its durable records (questions, answers, usage) in the
// payload shapes the agent bridge produces. The event bodies have the same
// JSON, and these carry a record onto a body.

func eventQuestion(q *payload.Question) *event.Question {
	if q == nil {
		return nil
	}
	var out event.Question
	if recode(*q, &out) != nil {
		return nil
	}
	return &out
}

func eventAnswer(a *payload.Answer) *event.Answer {
	if a == nil {
		return nil
	}
	var out event.Answer
	if recode(*a, &out) != nil {
		return nil
	}
	return &out
}

func eventUsage(u *payload.RunUsage) *event.RunUsage {
	if u == nil {
		return nil
	}
	var out event.RunUsage
	if recode(*u, &out) != nil {
		return nil
	}
	return &out
}
