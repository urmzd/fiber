package agentrt_test

import (
	"bytes"
	"testing"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/event/legacy"
)

// Every old kind converts to its type and back with the same bytes.
func TestChunksRoundTripThroughBodies(t *testing.T) {
	t.Parallel()
	for _, kind := range legacy.KeptKinds() {
		typ, err := legacy.TypeOf(kind)
		if err != nil {
			t.Fatal(err)
		}
		body, err := event.Decode(typ, nil)
		if err != nil {
			t.Fatal(err)
		}
		c, ok := agentrt.ChunkOf(body)
		if !ok || string(c.Kind) != kind {
			t.Fatalf("%s: chunk kind %q, %v", typ, c.Kind, ok)
		}
		back, err := agentrt.BodyOf(c)
		if err != nil || back.Type() != typ {
			t.Fatalf("%s: back = %v, %v", typ, back, err)
		}
	}
	if _, ok := agentrt.ChunkOf(event.MessagePosted{}); ok {
		t.Fatal("an ingress body has no old kind, yet converted")
	}
}

// A body marshals to the bytes its old payload did.
func TestABodyKeepsItsPayloadBytes(t *testing.T) {
	t.Parallel()
	c := agentrt.MustChunk(event.ToolCall{ID: "c1", Name: "t", Phase: event.PhaseExecuting, Arguments: []byte(`{"a":1}`)})
	old, err := payload.Encode(payload.ToolCall{ID: "c1", Name: "t", Phase: payload.PhaseExecuting, Arguments: []byte(`{"a":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(c.Payload, old) {
		t.Fatalf("payload = %s, want %s", c.Payload, old)
	}
}
