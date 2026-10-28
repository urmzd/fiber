package webhook

import (
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/event/legacy"
)

// Body is the JSON an outbound POST carries: one event, in the v1 wire shape
// receivers already read (a kind, a topic and an inline payload). The v2 body,
// the envelope itself, replaces it with the adapter rework.
type Body struct {
	Seq    uint64 `json:"seq"`
	Topic  string `json:"topic"`
	RunID  string `json:"run_id"`
	Tenant string `json:"tenant"`
	Agent  string `json:"agent"`
	Rev    int    `json:"rev"`
	Origin string `json:"origin"`
	Kind   string `json:"kind"`

	// Payload carries a JSON payload inline.
	Payload json.RawMessage `json:"payload,omitempty"`
	// PayloadB64 carries a payload that is not JSON. The two fields are
	// distinct rather than one polymorphic field so a consumer never has to
	// guess which encoding it received.
	PayloadB64 []byte `json:"payload_b64,omitempty"`

	At time.Time `json:"at"`
}

// BodyOf renders an event in the v1 wire shape. The kind is the old kind the
// type replaced, the topic is the old run topic, the revision is the pinned
// version read as a number, and the origin is the channel's scheme.
func BodyOf(e event.Envelope) Body {
	kind, ok := legacy.KindOf(e.Type)
	if !ok {
		kind = e.Type
	}
	rev, _ := strconv.Atoi(e.Version)
	b := Body{
		Seq:    e.Seq,
		Topic:  legacy.Topic(e.Agent, e.Run),
		RunID:  e.Run,
		Tenant: e.Tenant,
		Agent:  e.Agent,
		Rev:    rev,
		Origin: originOf(e.Channel),
		Kind:   kind,
		At:     e.Time.UTC(),
	}
	if e.Data != nil {
		if p, err := json.Marshal(e.Data); err == nil && string(p) != "null" {
			b.Payload = p
		}
	}
	return b
}

// originOf is the scheme of a channel: the surface the run was raised on.
func originOf(channel string) string {
	u, err := url.Parse(channel)
	if err != nil {
		return ""
	}
	return u.Scheme
}

// DeliveryID identifies one event exactly, and is what a receiver dedupes on.
//
// Run plus sequence number rather than a fresh uuid per attempt: the whole point
// is that a retry carries the SAME id as the attempt it repeats, so the receiver
// can recognise it. A uuid would be unique and useless.
func DeliveryID(e event.Envelope) string {
	if e.Run == "" || e.Seq == 0 {
		return e.ID
	}
	return e.Run + "/" + strconv.FormatUint(e.Seq, 10)
}
