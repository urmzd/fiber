package envelope_test

import (
	"context"
	"testing"

	"github.com/urmzd/fiber/pkg/envelope"
)

// Work acts for the actor its context was given and for nobody otherwise,
// and a zero actor clears an outer one, so work started for nobody cannot
// inherit the person some enclosing context acted for.
func TestWorkActsForTheActorItsContextWasGivenAndAZeroActorClearsIt(t *testing.T) {
	ana := envelope.Actor{ID: "ana", User: "u-ana"}
	for _, tc := range []struct {
		name string
		ctx  context.Context
		want envelope.Actor
		ok   bool
	}{
		{"nothing set", context.Background(), envelope.Actor{}, false},
		{"set", envelope.WithActor(context.Background(), ana), ana, true},
		{"only the host's account", envelope.WithActor(context.Background(), envelope.Actor{User: "u-ana"}), envelope.Actor{User: "u-ana"}, true},
		{"cleared by a zero actor", envelope.WithActor(envelope.WithActor(context.Background(), ana), envelope.Actor{}), envelope.Actor{}, false},
		{"a display name alone names nobody", envelope.WithActor(context.Background(), envelope.Actor{Display: "Ana"}), envelope.Actor{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := envelope.ActorFrom(tc.ctx)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("ActorFrom = %+v, %v; want %+v, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}
