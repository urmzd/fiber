// Package rendertest checks that a connector's tags, read as events, render
// the exact request text the tags render today.
package rendertest

import (
	"encoding/json"
	"testing"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/event/legacy"
)

// Check reads tag as an event, renders it through the catalog and fails t
// unless the bytes equal agentrt.RequestText of the tag. It returns the
// event's type.
func Check(t testing.TB, tag envelope.Tag) string {
	t.Helper()
	raw, err := json.Marshal(tag)
	if err != nil {
		t.Fatalf("rendertest: marshal tag: %v", err)
	}
	tr, err := legacy.DecodeTag(raw)
	if err != nil {
		t.Fatalf("rendertest: decode tag %s: %v", tag.ID, err)
	}
	e, ok := event.Lookup(tr.Event.Type)
	if !ok {
		t.Fatalf("rendertest: %s is not registered", tr.Event.Type)
	}
	got, err := e.Render(tr.Event)
	if err != nil {
		t.Fatalf("rendertest: render %s: %v", tr.Event.Type, err)
	}
	if want := agentrt.RequestText(tag.Text, tag.Meta); got != want {
		t.Errorf("rendertest: %s renders\n%q\nwant\n%q", tr.Event.Type, got, want)
	}
	return tr.Event.Type
}
