package fiber

import (
	"encoding/json"

	"github.com/urmzd/fiber/pkg/event"
)

// bodyJSON is an event's body as JSON, for a test that looks for text in it.
func bodyJSON(e event.Envelope) string {
	b, err := json.Marshal(e.Data)
	if err != nil {
		panic(err)
	}
	return string(b)
}
