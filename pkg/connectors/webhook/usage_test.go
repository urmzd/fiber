package webhook_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/connectors/webhook"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/router"
	"github.com/urmzd/fiber/pkg/topic"
)

// An existing webhook route with no kind filter keeps receiving exactly what
// it did before usage existed: no POST per model call. One that asks for
// usage gets it, signed and inline like every other event.
func TestAWebhookIsSentUsageOnlyWhenItsRouteAsksForIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		kinds []string
		want  []string
	}{
		{"no kind filter", nil, []string{"delta.text", "lifecycle.completed"}},
		{"usage selected", []string{"usage", "lifecycle.completed"}, []string{"usage.call", "lifecycle.completed"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := newReceiver(t, http.StatusOK)
			reg := connector.NewRegistry()
			if err := reg.Register(connectorAt(t, rec.URL, nil)); err != nil {
				t.Fatal(err)
			}
			b := bus.NewMemory()
			r := router.New(b, reg)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- r.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil {
					t.Errorf("router shutdown: %v", err)
				}
			})

			if err := r.Register("run_1", []envelope.Route{{Target: target, Kinds: tc.kinds}}); err != nil {
				t.Fatal(err)
			}
			tp, err := topic.Run("docs-bot", "run_1")
			if err != nil {
				t.Fatal(err)
			}
			bodies := map[envelope.Kind]string{
				envelope.KindText:      `{"text":"hi"}`,
				envelope.KindUsageCall: `{"accounting_id":"run_1/a1/1","prompt_tokens":1200}`,
				envelope.KindCompleted: `{"text":"hi"}`,
			}
			for i, kind := range []envelope.Kind{envelope.KindText, envelope.KindUsageCall, envelope.KindCompleted} {
				if err := b.Publish(ctx, envelope.Event{
					Seq: uint64(i + 1), Topic: tp, RunID: "run_1", Tenant: "acme", Agent: "docs-bot",
					Kind: kind, Payload: []byte(bodies[kind]), At: time.Now(),
				}); err != nil {
					t.Fatal(err)
				}
			}
			deadline := time.Now().Add(10 * time.Second)
			for rec.count() < len(tc.want) && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			time.Sleep(200 * time.Millisecond)
			var got []string
			for _, req := range rec.all() {
				var body webhook.Body
				if err := json.Unmarshal(req.body, &body); err != nil {
					t.Fatal(err)
				}
				got = append(got, body.Kind)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("the receiver got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("the receiver got %v, want %v", got, tc.want)
				}
			}
		})
	}
}
