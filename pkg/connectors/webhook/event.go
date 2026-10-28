package webhook

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/urmzd/fiber/pkg/event"
)

// SpecVersion is the CloudEvents version the body follows.
const SpecVersion = "1.0"

// BodyVersion is the version of the body shape, sent in HeaderVersion. The
// first body carried a kind, a topic and a payload; version 2 is the event
// envelope itself.
const BodyVersion = "2"

// Body is the JSON an outbound POST carries: one event envelope, with the
// CloudEvents specversion. Data is the typed body as inline JSON. A sensitive
// body (event.Entry.Sensitive) is left out unless the endpoint opts in, and
// Redacted says so.
type Body struct {
	SpecVersion string          `json:"specversion"`
	ID          string          `json:"id"`
	Source      string          `json:"source"`
	Type        string          `json:"type"`
	Subject     string          `json:"subject,omitempty"`
	Channel     string          `json:"channel"`
	Time        time.Time       `json:"time"`
	Hops        int             `json:"hops"`
	Tenant      string          `json:"tenant,omitempty"`
	Run         string          `json:"run,omitempty"`
	Seq         uint64          `json:"seq,omitempty"`
	Agent       string          `json:"agent,omitempty"`
	Version     string          `json:"version,omitempty"`
	Digest      string          `json:"digest,omitempty"`
	Data        json.RawMessage `json:"data,omitempty"`
	Redacted    bool            `json:"redacted,omitempty"`
}

// BodyOf renders an event as a body. With sensitive false, a body the
// catalog marks sensitive is left out.
func BodyOf(e event.Envelope, sensitive bool) (Body, error) {
	b := Body{
		SpecVersion: SpecVersion,
		ID:          e.ID,
		Source:      e.Source,
		Type:        e.Type,
		Subject:     e.Subject,
		Channel:     e.Channel,
		Time:        e.Time.UTC(),
		Hops:        e.Hops,
		Tenant:      e.Tenant,
		Run:         e.Run,
		Seq:         e.Seq,
		Agent:       e.Agent,
		Version:     e.Version,
		Digest:      e.Digest,
	}
	if entry, ok := event.Lookup(e.Type); ok && entry.Sensitive && !sensitive {
		b.Redacted = true
		return b, nil
	}
	if e.Data != nil {
		data, err := json.Marshal(e.Data)
		if err != nil {
			return Body{}, err
		}
		if string(data) != "null" {
			b.Data = data
		}
	}
	return b, nil
}

// DeliveryID identifies one event exactly, and is what a receiver dedupes on:
// "<run>/<seq>" for a run event, and the event id for any other.
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
