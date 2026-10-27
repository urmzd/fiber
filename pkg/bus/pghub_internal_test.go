package bus

import (
	"testing"

	"github.com/urmzd/fiber/pkg/topic"
)

// A publish wakes the readers of its run, its agent and the root, and no one
// else, so a replica with many open streams runs one query per publish that
// concerns it rather than one per stream.
func TestPgHubWakesOnlyCoveringReaders(t *testing.T) {
	h := &pgHub{}
	// listen is never started: count starts above zero so subscribe does
	// not spawn a LISTEN session without a pool.
	h.count = 1

	sub := func(s string) <-chan struct{} {
		ch, _ := h.subscribe(topic.MustParse(s).String())
		return ch
	}
	run1 := sub("agent:docs-bot:run_1")
	run2 := sub("agent:docs-bot:run_2")
	agent := sub("agent:docs-bot")
	other := sub("agent:triage")
	root := sub("agent")

	h.wake("agent:docs-bot:run_1")

	want := map[string]struct {
		ch   <-chan struct{}
		woke bool
	}{
		"run_1":  {run1, true},
		"run_2":  {run2, false},
		"agent":  {agent, true},
		"triage": {other, false},
		"root":   {root, true},
	}
	for name, w := range want {
		got := len(w.ch) == 1
		if got != w.woke {
			t.Errorf("%s woken = %v, want %v", name, got, w.woke)
		}
	}
}

// A payload the hub cannot read wakes everyone: the payload only saves
// queries, it never decides what a reader sees.
func TestPgHubUnreadablePayloadWakesAll(t *testing.T) {
	h := &pgHub{count: 1}
	a, _ := h.subscribe("agent:docs-bot:run_1")
	b, _ := h.subscribe("agent:triage")

	h.wake("not a topic")

	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("woken = %d, %d; want both", len(a), len(b))
	}
}

func TestPgHubUnsubscribeStopsListening(t *testing.T) {
	h := &pgHub{}
	stopped := false
	h.cancel = func() { stopped = true }
	h.count = 1
	_, un := h.subscribe("agent")
	h.count-- // undo the seeded count; this subscription is now the only one
	un()
	un() // idempotent
	if h.count != 0 || len(h.subs) != 0 || !stopped {
		t.Fatalf("count=%d subs=%d stopped=%v", h.count, len(h.subs), stopped)
	}
}
