package mcptools

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"

	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// ErrNoPersonCredential reports a person-scoped provider reached for someone
// with no credential of their own for it: the person a run acts for has not
// connected one, the run acts for nobody, or nothing is configured to look one
// up. Nothing is opened. A shared credential is never used instead, and
// neither is anyone else's.
var ErrNoPersonCredential = errors.New("mcptools: no credential of the person's own")

// PersonCredential is one person's credential for one provider, as a
// PersonResolver found it: Ref, a reference resolved through the session's
// Resolver the way a shared CredentialRef is, or Secret, the credential
// itself, for a resolver that holds it. Exactly one is set; the zero value is
// no credential.
type PersonCredential struct {
	Ref    string
	Secret Secret
}

// Empty reports whether c holds no credential.
func (c PersonCredential) Empty() bool { return c.Ref == "" && c.Secret.Empty() }

// String names the reference, which is not secret, or prints Redacted.
func (c PersonCredential) String() string {
	if c.Ref != "" {
		return c.Ref
	}
	return Redacted
}

// GoString is String, so %#v cannot reach the secret either.
func (c PersonCredential) GoString() string { return c.String() }

// same reports whether a and b are one credential: the same reference, or
// the same secret. It runs on every expansion of a person's group, so the
// comparison does not depend on where two secrets first differ.
func (c PersonCredential) same(o PersonCredential) bool {
	return c.Ref == o.Ref && subtle.ConstantTimeCompare([]byte(c.Secret.Reveal()), []byte(o.Secret.Reveal())) == 1
}

// resolve turns c into the secret a session sends, through r for a
// reference.
func (c PersonCredential) resolve(ctx context.Context, r Resolver, provider string) (Secret, error) {
	switch {
	case c.Ref != "" && !c.Secret.Empty():
		return Secret{}, fmt.Errorf("%w: provider %q: a person's credential is a reference or a secret, not both", ErrCredential, provider)
	case !c.Secret.Empty():
		return c.Secret, nil
	case c.Ref == "":
		return Secret{}, fmt.Errorf("%w: provider %q is person-scoped and no credential of the person's own was given", ErrNoPersonCredential, provider)
	case r == nil:
		return Secret{}, fmt.Errorf("%w: provider %q: a person's credential names %q and no resolver is configured", ErrCredential, provider, c.Ref)
	}
	s, err := r.Resolve(ctx, c.Ref)
	if err != nil {
		return Secret{}, fmt.Errorf("%w: provider %q: resolve a person's %q: %w", ErrCredential, provider, c.Ref, err)
	}
	if s.Empty() {
		return Secret{}, fmt.Errorf("%w: provider %q: a person's %q resolved to nothing", ErrCredential, provider, c.Ref)
	}
	return s, nil
}

// PersonResolver finds the credential the person a run acts for holds for a
// person-scoped provider, so that person's session is opened with their own.
//
// actor is the actor on the run's durable record (pkg/runtime puts it on the
// turn's context, see event.ActorFrom), never anything a model or a
// request body said. The resolver decides who that actor is: an actor's ID is
// connector-native and two surfaces may share one, while User is the host's
// own account for them, which is what a host that keeps credentials per
// account looks up. It returns that person's credential and nobody else's.
//
// A person with no credential for p is ErrNoPersonCredential (wrapped as the
// resolver likes) or the zero PersonCredential. Either expands the person's
// group to nothing, and the model is told they have not connected one. Any
// other error means the credential could not be looked up now; the group is
// unavailable for the turn and the operator is told why.
type PersonResolver interface {
	ResolvePerson(ctx context.Context, tenant string, p toolprovider.Provider, actor event.Actor) (PersonCredential, error)
}

// PersonResolverFunc adapts a function into a PersonResolver.
type PersonResolverFunc func(ctx context.Context, tenant string, p toolprovider.Provider, actor event.Actor) (PersonCredential, error)

// ResolvePerson implements PersonResolver.
func (f PersonResolverFunc) ResolvePerson(ctx context.Context, tenant string, p toolprovider.Provider, actor event.Actor) (PersonCredential, error) {
	return f(ctx, tenant, p, actor)
}

// personKey is who a person's session belongs to. Two actors that differ in
// any of these are two people as far as sessions go, so one person's session
// is never handed to another; a display name is not identity.
type personKey struct {
	id, user string
	bot      bool
}

func keyOf(a event.Actor) personKey { return personKey{id: a.ID, user: a.User, bot: a.Bot} }

// personCtxKey carries the credential Grants dials a person's session with
// through Dial, which takes only a context and a record, to Connect.
type personCtxKey struct{}

func withPersonCredential(ctx context.Context, c PersonCredential) context.Context {
	return context.WithValue(ctx, personCtxKey{}, c)
}

func personCredentialFrom(ctx context.Context) (PersonCredential, bool) {
	c, ok := ctx.Value(personCtxKey{}).(PersonCredential)
	return c, ok && !c.Empty()
}

// noPersonCredential is ErrNoPersonCredential for one provider and one turn,
// with a reason for the operator and a note the model may be told
// (native.ModelNote), which names neither the actor nor anything about the
// deployment.
type noPersonCredential struct {
	provider string
	reason   string
	note     string
}

func (e *noPersonCredential) Error() string {
	return fmt.Sprintf("%s: provider %q takes the credential of the person a run acts for, and %s", ErrNoPersonCredential, e.provider, e.reason)
}

func (e *noPersonCredential) Unwrap() error { return ErrNoPersonCredential }

// ModelNote is what the model is told about the group it does not have.
func (e *noPersonCredential) ModelNote() string { return e.note }

func actingForNobody(provider string) error {
	return &noPersonCredential{provider: provider,
		reason: "this run acts for nobody",
		note:   "These tools use the credential of the person a request is made for, and this request was made for no one."}
}

func noPersonResolver(provider string) error {
	return &noPersonCredential{provider: provider,
		reason: "this deployment has no PersonResolver to look one up",
		note:   "These tools use a credential of the person you are acting for, and this deployment cannot look one up."}
}

func notConnected(provider string, a event.Actor) error {
	who := fmt.Sprintf("actor %q", a.ID)
	if a.User != "" {
		who += fmt.Sprintf(" (user %q)", a.User)
	}
	return &noPersonCredential{provider: provider,
		reason: who + " has not connected one",
		note: fmt.Sprintf("These tools use a credential of the person you are acting for, and they have not connected one for %s. "+
			"They can connect one and ask again.", provider)}
}
