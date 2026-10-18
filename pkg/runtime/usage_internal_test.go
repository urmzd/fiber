package runtime

import (
	"testing"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/envelope"
)

// The relay a delegation runs is the same at every level, so a call made two
// delegations down arrives on the origin's stream exactly once, two levels
// deep, and nothing else a sub-task streams arrives at all. The real sandbox
// refuses a sub-task's own delegation by policy today; this holds the relay to
// any depth regardless.
func TestASubTaskOfASubTaskIsRelayedTwoLevelsDeepOnce(t *testing.T) {
	var origin []agentrt.Chunk
	toOrigin := func(c agentrt.Chunk) error {
		origin = append(origin, c)
		return nil
	}

	// origin <- child's sink <- grandchild's sink
	childSink := make(chan agentrt.Chunk, 4)
	stopChild := relay(childSink, toOrigin)
	grandchildSink := make(chan agentrt.Chunk, 4)
	stopGrandchild := relay(grandchildSink, func(c agentrt.Chunk) error {
		childSink <- c
		return nil
	})

	call, err := payload.Encode(payload.UsageCall{AccountingID: "r/a1/helper.sub-1/assistant.sub-2/1", Agent: "assistant", PromptTokens: 30})
	if err != nil {
		t.Fatal(err)
	}
	text, err := payload.Encode(payload.Text{Text: "Three fail."})
	if err != nil {
		t.Fatal(err)
	}
	grandchildSink <- agentrt.Chunk{Kind: envelope.KindText, Payload: text}
	grandchildSink <- agentrt.Chunk{Kind: envelope.KindUsageCall, Payload: call}
	stopGrandchild()
	stopChild()

	if len(origin) != 1 || origin[0].Kind != envelope.KindUsageCall {
		t.Fatalf("the origin received %d chunks, want the one usage.call", len(origin))
	}
	got, err := payload.Decode[payload.UsageCall](envelope.Event{Kind: origin[0].Kind, Payload: origin[0].Payload})
	if err != nil {
		t.Fatal(err)
	}
	if got.Depth != 2 || got.AccountingID != "r/a1/helper.sub-1/assistant.sub-2/1" || got.PromptTokens != 30 {
		t.Fatalf("relayed call = %+v, want it two levels deep and otherwise unchanged", got)
	}
}
