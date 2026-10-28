package envelope

import (
	"context"

	"github.com/urmzd/fiber/pkg/event"
)

// WithActor returns ctx carrying a as the actor the work under it acts for.
// It is event.WithActor: see there.
//
// pkg/runtime sets it for every turn from the tag on the run's durable record,
// which is the one account of who raised a run that a model cannot rewrite.
func WithActor(ctx context.Context, a Actor) context.Context { return event.WithActor(ctx, a) }

// ActorFrom returns the actor ctx acts for. ok is false when it acts for
// nobody. It is event.ActorFrom.
func ActorFrom(ctx context.Context) (Actor, bool) { return event.ActorFrom(ctx) }
