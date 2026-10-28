// Package envelope holds what is left of fiber's wire contract from before
// the event union: the old event kinds, the old Event, Filter and
// Subscription, and the actor a context acts for. A run is started by an
// event (pkg/event), never by a tag, and delivered by egress adapters that
// subscribe to channels (pkg/egress), never by routes.
package envelope

import (
	"errors"
	"time"

	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/topic"
)

// ErrInvalid reports a malformed envelope value. Callers match with errors.Is.
var ErrInvalid = errors.New("envelope: invalid")

// Kind classifies an Event. Kinds are dotted so that a filter can select a
// whole family ("delta") or one member ("delta.citation"). Every kind travels
// on the same topic and in the same stream; kind is a field, never an address.
type Kind string

// Event kinds.
const (
	// Streaming output from the agent as it works.
	KindText     Kind = "delta.text"
	KindThinking Kind = "delta.thinking"
	KindToolCall Kind = "delta.tool.call"
	KindToolDone Kind = "delta.tool.done"
	KindCitation Kind = "delta.citation"

	// Run lifecycle. A sink that only wants to know the work landed
	// subscribes to lifecycle.completed and nothing else.
	KindAccepted  Kind = "lifecycle.accepted"
	KindStarted   Kind = "lifecycle.started"
	KindParked    Kind = "lifecycle.parked"
	KindResumed   Kind = "lifecycle.resumed"
	KindCompleted Kind = "lifecycle.completed"
	KindFailed    Kind = "lifecycle.failed"
	// KindChildParked reports, on a parent run's stream, that a child run
	// it delegated to and is waiting on is parked on a question for a
	// person. The parent is not parked: its turn is waiting on the child.
	KindChildParked Kind = "lifecycle.child.parked"
	// KindStopped ends a run that was asked to stop before it finished. It
	// is terminal, like completed and failed, and it is neither: the run did
	// not finish its work and nothing went wrong. No completed or failed
	// event follows it.
	KindStopped Kind = "lifecycle.stopped"

	// Actions the agent took on a connector's surface: a comment posted, a
	// ticket transitioned. This is the "work got done" signal, distinct from
	// "the agent said something".
	KindActionTaken Kind = "action.taken"

	// What a run spent. They are accounting, not conversation, and a route
	// receives them only by asking: a route with no kind filter, or one that
	// selects "delta" or "lifecycle", never does. Select "usage" for both,
	// or one of them by name. A subscription with no filter sees them like
	// any other kind.
	//
	// KindUsageCall reports one model call as it completes: tokens, the
	// model, its cost where saige can price it, and its context window.
	KindUsageCall Kind = "usage.call"
	// KindUsageChild reports a child run's whole usage once it has ended,
	// so the run that delegated to it can count it.
	KindUsageChild Kind = "usage.child"

	// KindCompacted reports that a run's turn replaced the older part of its
	// history with a summary, because its input crossed the deployment's
	// budget or because a host asked. It is how a host shows that the agent
	// now works from a summary. Like usage it is about the run rather than
	// the conversation, and a route receives it only by selecting "context"
	// or the kind itself.
	KindCompacted Kind = "context.compacted"

	// Control-plane changes, on the same bus as the data so a dashboard sees
	// an agent get revised in the same stream as the runs that follow it.
	KindAgentCreated Kind = "control.created"
	KindAgentRevised Kind = "control.revised"
	KindAgentDeleted Kind = "control.deleted"

	// KindStop asks a run to stop. It is a request, not an outcome: it is
	// published to the run's control topic (see topic.Control), never to the
	// run's own, and the run answers it with lifecycle.stopped only if it was
	// still working when the request reached it.
	KindStop Kind = "control.stop"

	// KindCompact asks a run to compact its history before its next model
	// call. Like KindStop it is a request on the run's control topic, and it
	// is answered with context.compacted only if the turn compacted.
	KindCompact Kind = "control.compact"
)

// Family returns the leading component of a kind: "delta", "lifecycle",
// "action", "usage", "context", or "control".
func (k Kind) Family() string {
	for i := 0; i < len(k); i++ {
		if k[i] == '.' {
			return string(k[:i])
		}
	}
	return string(k)
}

// Matches reports whether k is selected by sel. A selector matches either the
// exact kind or its family, so "delta" selects every delta kind and
// "delta.citation" selects only citations.
func (k Kind) Matches(sel string) bool {
	if sel == "" || sel == string(k) {
		return true
	}
	return sel == k.Family()
}

// Actor is the human or system that raised a tag. It is event.Actor.
type Actor = event.Actor

// Event is one thing an agent produced. Every event, of every kind, travels
// the same path: published to its run's topic, seen by every prefix
// subscriber, and delivered to whichever routes want its kind.
type Event struct {
	// Seq orders events within a run, starting at 1. It is the resume
	// cursor: a consumer that reconnects asks for everything after the last
	// Seq it processed.
	Seq uint64 `json:"seq"`

	// Topic is where this event was published: always the most specific
	// form, agent:<name>:<run>.
	Topic topic.Topic `json:"topic"`

	// RunID is the durable run that produced the event.
	RunID string `json:"run_id"`

	// Tenant scopes the event for authorization on the read path.
	Tenant string `json:"tenant"`

	// Agent and Rev identify exactly what produced this event. Rev is the
	// pinned revision, so a consumer comparing two revisions of an agent
	// filters on this field rather than subscribing to two topics.
	Agent string `json:"agent"`
	Rev   int    `json:"rev"`

	// Origin is the trigger that raised the run.
	Origin string `json:"origin"`

	Kind Kind `json:"kind"`

	// Payload is the kind-specific body.
	Payload []byte `json:"payload,omitempty"`

	At time.Time `json:"at"`
}

// Filter narrows a subscription without changing the topic it listens to.
// The zero Filter selects everything on the topic.
//
// Filtering rather than topic-splitting is what keeps the namespace at
// "agent:<name>": a consumer watching a canary revision and a consumer
// watching every citation are both listening to the same topic, and neither
// has to discover a topic name that only exists once some agent has been
// revised that many times.
type Filter struct {
	// Kinds selects event kinds by exact match or family. Empty means all.
	Kinds []string `json:"kinds,omitempty"`
	// Rev selects one pinned agent revision. Zero means any.
	Rev int `json:"rev,omitempty"`
	// Origin selects the trigger that raised the run. Empty means any.
	Origin string `json:"origin,omitempty"`
	// RunID selects a single run. Empty means any. Subscribing to
	// agent:<name>:<run> is usually clearer, but this lets a consumer hold
	// one agent-level subscription and narrow it dynamically.
	RunID string `json:"run_id,omitempty"`
}

// Allows reports whether e passes the filter.
func (f Filter) Allows(e Event) bool {
	if f.Rev != 0 && e.Rev != f.Rev {
		return false
	}
	if f.Origin != "" && e.Origin != f.Origin {
		return false
	}
	if f.RunID != "" && e.RunID != f.RunID {
		return false
	}
	if len(f.Kinds) == 0 {
		return true
	}
	for _, sel := range f.Kinds {
		if e.Kind.Matches(sel) {
			return true
		}
	}
	return false
}

// Subscription is a topic plus a filter plus a resume position: everything
// the bus needs to serve one consumer.
type Subscription struct {
	Topic  topic.Topic `json:"topic"`
	Filter Filter      `json:"filter,omitempty"`
	// From resumes after this sequence number within each run. Zero starts
	// at the beginning of what the bus still holds.
	From uint64 `json:"from,omitempty"`
}

// Accepts reports whether this subscription should receive e.
func (s Subscription) Accepts(e Event) bool {
	if !s.Topic.Covers(e.Topic) {
		return false
	}
	if e.Seq <= s.From {
		return false
	}
	return s.Filter.Allows(e)
}
