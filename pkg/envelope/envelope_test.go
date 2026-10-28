package envelope_test

import (
	"testing"

	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/topic"
)

func TestKindFamilyAndMatches(t *testing.T) {
	cases := []struct {
		kind   envelope.Kind
		family string
		sel    string
		want   bool
	}{
		{envelope.KindText, "delta", "delta", true},
		{envelope.KindCitation, "delta", "delta", true},
		{envelope.KindCitation, "delta", "delta.citation", true},
		{envelope.KindCitation, "delta", "delta.text", false},
		{envelope.KindCompleted, "lifecycle", "delta", false},
		{envelope.KindCompleted, "lifecycle", "lifecycle", true},
		{envelope.KindCompleted, "lifecycle", "lifecycle.completed", true},
		{envelope.KindActionTaken, "action", "action", true},
		{envelope.KindText, "delta", "", true},
		// Usage is its own family, so a route that asked for the
		// conversation or the outcome is never sent accounting.
		{envelope.KindUsageCall, "usage", "usage", true},
		{envelope.KindUsageChild, "usage", "usage", true},
		{envelope.KindUsageCall, "usage", "delta", false},
		{envelope.KindUsageCall, "usage", "lifecycle", false},
		{envelope.KindUsageChild, "usage", "usage.call", false},
		{envelope.KindCompacted, "context", "context", true},
		{envelope.KindCompacted, "context", "lifecycle", false},
	}
	for _, tc := range cases {
		if got := tc.kind.Family(); got != tc.family {
			t.Errorf("%s.Family() = %q, want %q", tc.kind, got, tc.family)
		}
		if got := tc.kind.Matches(tc.sel); got != tc.want {
			t.Errorf("%s.Matches(%q) = %v, want %v", tc.kind, tc.sel, got, tc.want)
		}
	}
}

func newEvent(seq uint64, agent, runID string, rev int, origin string, kind envelope.Kind) envelope.Event {
	return envelope.Event{
		Seq:    seq,
		Topic:  topic.MustParse("agent:" + agent + ":" + runID),
		RunID:  runID,
		Agent:  agent,
		Rev:    rev,
		Origin: origin,
		Kind:   kind,
	}
}

func TestFilterZeroAllowsEverything(t *testing.T) {
	var f envelope.Filter
	for _, k := range []envelope.Kind{envelope.KindText, envelope.KindCompleted, envelope.KindActionTaken} {
		if !f.Allows(newEvent(1, "docs-bot", "run1", 3, "slack", k)) {
			t.Errorf("zero Filter rejected %s", k)
		}
	}
}

func TestFilterNarrowsWithoutChangingTopic(t *testing.T) {
	rev7 := newEvent(1, "docs-bot", "run1", 7, "slack", envelope.KindText)
	rev6 := newEvent(1, "docs-bot", "run2", 6, "slack", envelope.KindText)

	canary := envelope.Filter{Rev: 7}
	if !canary.Allows(rev7) {
		t.Error("canary filter rejected rev 7")
	}
	if canary.Allows(rev6) {
		t.Error("canary filter accepted rev 6")
	}

	citations := envelope.Filter{Kinds: []string{"delta.citation"}}
	if citations.Allows(rev7) {
		t.Error("citation filter accepted a text delta")
	}
	if !citations.Allows(newEvent(2, "docs-bot", "run1", 7, "slack", envelope.KindCitation)) {
		t.Error("citation filter rejected a citation")
	}

	fromGitHub := envelope.Filter{Origin: "github"}
	if fromGitHub.Allows(rev7) {
		t.Error("origin filter accepted a slack-origin event")
	}

	oneRun := envelope.Filter{RunID: "run1"}
	if !oneRun.Allows(rev7) || oneRun.Allows(rev6) {
		t.Error("run filter did not isolate run1")
	}
}

// Listening to an agent means receiving every run on one subscription, with
// From acting as the resume cursor.
func TestSubscriptionAcceptsAndResumes(t *testing.T) {
	sub := envelope.Subscription{Topic: topic.MustParse("agent:docs-bot")}

	for _, runID := range []string{"run1", "run2"} {
		if !sub.Accepts(newEvent(1, "docs-bot", runID, 1, "slack", envelope.KindText)) {
			t.Errorf("agent subscription missed %s", runID)
		}
	}
	if sub.Accepts(newEvent(1, "triage-bot", "run9", 1, "slack", envelope.KindText)) {
		t.Error("agent subscription accepted another agent's event")
	}

	resumed := envelope.Subscription{Topic: topic.MustParse("agent:docs-bot"), From: 5}
	if resumed.Accepts(newEvent(5, "docs-bot", "run1", 1, "slack", envelope.KindText)) {
		t.Error("resume cursor redelivered the last processed event")
	}
	if !resumed.Accepts(newEvent(6, "docs-bot", "run1", 1, "slack", envelope.KindText)) {
		t.Error("resume cursor dropped the next event")
	}
}
