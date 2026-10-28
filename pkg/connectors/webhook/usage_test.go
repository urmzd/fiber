package webhook_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/connectors/webhook"
	"github.com/urmzd/fiber/pkg/egress"
	"github.com/urmzd/fiber/pkg/event"
)

// An endpoint with no type filter receives what it always did: no POST per
// model call. One that asks for usage gets it, signed and inline like every
// other event.
func TestAWebhookIsSentUsageOnlyWhenItsEndpointAsksForIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		types []string
		want  []string
	}{
		{"no type filter", nil, []string{event.TypeText, event.TypeCompleted}},
		{"usage selected", []string{"run.usage", "run.completed"}, []string{event.TypeUsageCall, event.TypeCompleted}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := newReceiver(t, http.StatusOK)
			reg := connector.NewRegistry()
			if err := reg.Register(connectorAt(t, rec.URL, func(cfg *webhook.Config) {
				ep := cfg.Endpoints["acme"]
				ep.Types = tc.types
				cfg.Endpoints["acme"] = ep
			})); err != nil {
				t.Fatal(err)
			}
			b := bus.NewMemory()
			out := egress.New(b, reg)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- out.Run(ctx, "acme") }()
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil {
					t.Errorf("egress shutdown: %v", err)
				}
			})

			for i, body := range []event.Body{
				event.Text{Text: "hi"},
				event.UsageCall{AccountingID: "run_1/a1/1", PromptTokens: 1200},
				event.Completed{Text: "hi"},
			} {
				seq := uint64(i + 1)
				if err := b.Publish(ctx, event.Envelope{
					ID: fmt.Sprintf("run_1:%d", seq), Source: "fiber://run/run_1", Type: body.Type(),
					Channel: "cron://acme/nightly", Tenant: "acme", Run: "run_1", Seq: seq,
					Agent: "docs-bot", Time: time.Now().UTC(), Data: body,
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
				got = append(got, body.Type)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("the receiver got %v, want %v", got, tc.want)
			}
		})
	}
}
