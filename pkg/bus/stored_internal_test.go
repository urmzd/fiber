package bus

import (
	"testing"

	"github.com/urmzd/fiber/pkg/event"
)

// bad is a stored run.delta.text.v1 whose body does not decode.
const bad = `{"id":"x","source":"fiber://run/r","type":"run.delta.text.v1","channel":"slack://T01/C02","time":"2026-01-01T00:00:00Z","hops":0,"tenant":"acme","run":"r","seq":1,"data":{"text":5}}`

// A subscriber that did not select a broken event's type never sees its
// decode error: the header is matched before the body is decoded.
func TestReadStoredMatchesHeaderBeforeBody(t *testing.T) {
	other := Subscription{Tenant: "acme", Channels: []string{"slack://"}, Types: []string{"message"}}
	if _, keep, err := readStored([]byte(bad), other); err != nil || keep {
		t.Fatalf("readStored for another type = keep %v, err %v; want skipped with no error", keep, err)
	}
	runs := Subscription{Tenant: "acme", Channels: []string{"slack://"}, Types: []string{"run"}}
	if _, keep, err := readStored([]byte(bad), runs); err == nil || keep {
		t.Fatalf("readStored for run = keep %v, err %v; want a decode error", keep, err)
	}
	good := `{"id":"y","source":"fiber://run/r","type":"run.delta.text.v1","channel":"slack://T01/C02","time":"2026-01-01T00:00:00Z","hops":0,"tenant":"acme","run":"r","seq":2,"data":{"text":"hi"}}`
	e, keep, err := readStored([]byte(good), runs)
	if err != nil || !keep {
		t.Fatalf("readStored good = keep %v, err %v", keep, err)
	}
	if txt, ok := e.Data.(event.Text); !ok || txt.Text != "hi" {
		t.Fatalf("data = %#v", e.Data)
	}
}
