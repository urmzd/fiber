package server

import (
	"encoding/json"
	"testing"
	"time"

	mandatumv1 "github.com/urmzd/fiber/gen/mandatum/v1"
	"github.com/urmzd/fiber/pkg/envelope"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// actor.user has to survive every hop a tag makes: wire to envelope at
// accept, envelope to JSON in the run ledger, and back to the wire when the
// run is read. A field dropped on any of them is a host that cannot tell
// whose request a run was.
func TestActorUserSurvivesTheRoundTrip(t *testing.T) {
	in := &mandatumv1.Tag{
		Id: "t1", Agent: "docs-bot", Origin: "web", Text: "hi",
		Actor: &mandatumv1.Actor{Id: "U1", Display: "Alice", User: "user-42"},
	}
	for name, codec := range map[string]struct {
		marshal   func(proto.Message) ([]byte, error)
		unmarshal func([]byte, proto.Message) error
	}{
		"binary": {proto.Marshal, proto.Unmarshal},
		"json":   {protojson.Marshal, protojson.Unmarshal},
	} {
		b, err := codec.marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		var wire mandatumv1.Tag
		if err := codec.unmarshal(b, &wire); err != nil {
			t.Fatal(err)
		}
		tag, err := tagFromProto(&wire)
		if err != nil {
			t.Fatalf("%s: tagFromProto: %v", name, err)
		}
		tag.At = time.Unix(1, 0)
		want := envelope.Actor{ID: "U1", Display: "Alice", User: "user-42"}
		if tag.Actor != want {
			t.Fatalf("%s: wire to envelope: %+v, want %+v", name, tag.Actor, want)
		}
		stored, err := json.Marshal(tag)
		if err != nil {
			t.Fatal(err)
		}
		var back envelope.Tag
		if err := json.Unmarshal(stored, &back); err != nil {
			t.Fatal(err)
		}
		if back.Actor != want {
			t.Fatalf("%s: envelope through JSON: %+v, want %+v", name, back.Actor, want)
		}
		if got := tagToProto(back).GetActor(); got.GetUser() != "user-42" || got.GetId() != "U1" {
			t.Fatalf("%s: envelope to wire: %+v", name, got)
		}
	}
}
