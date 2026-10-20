// Package envelope is the contract every fiber client speaks, independent
// of transport. gRPC, SSE, and the in-process path all carry these types.
//
// Two directions:
//
//	Tag    a request to an agent, raised by a trigger
//	Event  something an agent produced, delivered to sinks and observers
//
// The asymmetry between triggers and sinks is deliberate. A trigger is
// something that can raise a tag (a Slack mention, a GitHub review request, a
// cron schedule). A sink is something an event can be delivered to (a Slack
// thread, a GitHub comment, an outbound webhook). Most connectors are both,
// but not all: cron only triggers, and an outbound webhook is only a sink.
// Because the two roles are independent, a tag raised in GitHub can deliver
// into Jira, and a schedule with no origin surface at all can deliver into
// both.
//
// Delivery is N:M and selective. One tag fans out to many sinks; one sink
// serves many agents. Each route chooses its own event kinds, so the GitHub
// connector can stream every delta into a comment it keeps editing while a
// separate webhook receives only the terminal event and learns nothing about
// the intermediate steps.
package envelope

import (
	"errors"
	"fmt"
	"time"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/attachment"
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

// Actor is the human or system that raised a tag.
type Actor struct {
	// ID is the connector-native identifier (a Slack user ID, a GitHub
	// login). Stable within a workspace.
	ID string `json:"id"`
	// Display is a human-readable name, best effort.
	Display string `json:"display,omitempty"`
	// Bot reports whether the actor is automation rather than a person.
	Bot bool `json:"bot,omitempty"`
	// User is the host's own account for the actor (the user id of the
	// product a person signed in to), when the surface knows it. It is an
	// authorization fact like ID: the server keeps it only from a credential
	// trusted to act for others.
	User string `json:"user,omitempty"`
}

// Route is one delivery instruction: where to send events, and which ones.
//
// Kinds is a set of selectors matched against Event.Kind. An empty Kinds
// means every kind except the usage and context families (see OptIn). This is what makes a
// single run serve both a connector rendering a live stream and a webhook
// that only wants the outcome.
type Route struct {
	Target address.Address `json:"target"`
	Kinds  []string        `json:"kinds,omitempty"`
}

// OptIn reports whether a route must name k's family or k itself to receive
// it. The usage family is opt-in: it was added after routes with no kind
// filter were already delivering to receivers that knew a fixed set of kinds,
// and an event per model call that such a receiver refuses, or fails on and
// has retried in the run's ordered delivery lane, would cost every route that
// never asked for it. As the root topic does not cover the control namespace,
// "everything" does not cover accounting. The context family is opt-in for the
// same reason: it arrived later, and it reports on the run's context rather
// than on the conversation a receiver renders.
func (k Kind) OptIn() bool {
	switch k.Family() {
	case "usage", "context":
		return true
	}
	return false
}

// Wants reports whether an event of kind k should be delivered on this route.
// An opt-in kind is delivered only to a route that names it or its family.
func (r Route) Wants(k Kind) bool {
	if len(r.Kinds) == 0 {
		return !k.OptIn()
	}
	for _, sel := range r.Kinds {
		if sel == "" && k.OptIn() {
			continue
		}
		if k.Matches(sel) {
			return true
		}
	}
	return false
}

// Validate checks that the route names a usable target.
func (r Route) Validate() error {
	if r.Target.IsZero() {
		return fmt.Errorf("%w: route has no target", ErrInvalid)
	}
	return nil
}

// Tag is a request to an agent. It is the canonical form every trigger
// translates into, and the only thing the core accepts as work.
type Tag struct {
	// ID is the trigger-native event identifier: a Slack event_id, a GitHub
	// delivery GUID, a schedule occurrence. It becomes the run's idempotency
	// key, so a webhook redelivery joins the existing run instead of starting
	// a second one.
	ID string `json:"id"`

	// Tenant scopes the tag. It is set by the server from the caller's
	// credential and is not trusted from the wire.
	Tenant string `json:"tenant"`

	// Agent is the name that was tagged.
	Agent string `json:"agent"`

	// Origin is the trigger that raised this tag ("slack", "github", "cron").
	Origin string `json:"origin"`

	// Source is where the tag came from, and the natural place to answer if
	// no route says otherwise. Zero for triggers with no addressable surface,
	// such as a schedule.
	Source address.Address `json:"source,omitempty"`

	// Text is the request, with the agent mention already stripped.
	Text string `json:"text"`

	// Actor raised the tag.
	Actor Actor `json:"actor,omitempty"`

	// Deliver lists every destination for this run's events. Empty means
	// answer in place, at Source.
	Deliver []Route `json:"deliver,omitempty"`

	// Meta carries trigger-specific context the agent's tools may need: a
	// pull request number, an issue title, a Jira status.
	Meta map[string]string `json:"meta,omitempty"`

	// Attachments are the files that arrived with the tag, as references
	// into the blob store. They are omitted when empty so that the durable
	// input of a run with no files is byte for byte what it always was.
	Attachments []attachment.Attachment `json:"attachments,omitempty"`

	// At is when the trigger observed the event.
	At time.Time `json:"at"`
}

// Validate checks the fields the core requires to start a run.
func (t Tag) Validate() error {
	switch {
	case t.ID == "":
		return fmt.Errorf("%w: tag has no id, which is its idempotency key", ErrInvalid)
	case t.Agent == "":
		return fmt.Errorf("%w: tag names no agent", ErrInvalid)
	case t.Origin == "":
		return fmt.Errorf("%w: tag has no origin", ErrInvalid)
	}
	for i, r := range t.Deliver {
		if err := r.Validate(); err != nil {
			return fmt.Errorf("%w (route %d)", err, i)
		}
	}
	if err := attachment.ValidateAll(t.Attachments); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}

// Routes returns the tag's delivery routes, falling back to answering in
// place at Source when none were given. A tag with neither routes nor a
// source produces no routes: the run still executes and still publishes to
// the bus, it simply has nowhere to be rendered.
func (t Tag) Routes() []Route {
	if len(t.Deliver) > 0 {
		return t.Deliver
	}
	if t.Source.IsZero() {
		return nil
	}
	return []Route{{Target: t.Source}}
}

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
