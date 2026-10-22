package envelope

import "context"

// actorKey is the context key the actor a piece of work acts for travels
// under. It is unexported, so only WithActor sets it.
type actorKey struct{}

// WithActor returns ctx carrying a as the actor the work under it acts for.
//
// pkg/runtime sets it for every turn from the tag on the run's durable record,
// which is the one account of who raised a run that a model cannot rewrite:
// nothing a turn says, no tool argument and no trigger metadata reaches it.
// Code that acts on a person's behalf (a tool source opening a session with
// that person's own credential) reads it with ActorFrom. A zero actor clears
// whatever ctx carried, so work that acts for nobody cannot inherit someone.
func WithActor(ctx context.Context, a Actor) context.Context {
	return context.WithValue(ctx, actorKey{}, a)
}

// ActorFrom returns the actor ctx acts for. ok is false when it acts for
// nobody: no actor was set, or the one set names no one (no ID and no User).
func ActorFrom(ctx context.Context) (a Actor, ok bool) {
	a, _ = ctx.Value(actorKey{}).(Actor)
	if a.ID == "" && a.User == "" {
		return Actor{}, false
	}
	return a, true
}
