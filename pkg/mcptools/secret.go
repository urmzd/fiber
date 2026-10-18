package mcptools

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Redacted is what a Secret prints as, everywhere.
const Redacted = "[REDACTED]"

// Secret is a resolved credential. Every way of printing it prints Redacted:
// String, GoString, every fmt verb, slog, JSON and text marshaling. The only
// way to the value is Reveal, which this package calls in exactly two places,
// the HTTP header and the stdio environment.
//
// The value sits behind a pointer so that a Secret held in an unexported
// field of some other struct, which fmt prints without consulting its
// methods, shows as an address rather than as the value.
type Secret struct{ v *string }

// NewSecret wraps a credential value.
func NewSecret(value string) Secret { return Secret{v: &value} }

// Reveal returns the credential. Call it only where the value leaves the
// process to the server it is for.
func (s Secret) Reveal() string {
	if s.v == nil {
		return ""
	}
	return *s.v
}

// Empty reports whether there is no credential.
func (s Secret) Empty() bool { return s.Reveal() == "" }

func (Secret) String() string               { return Redacted }
func (Secret) GoString() string             { return Redacted }
func (Secret) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte(Redacted)) }
func (Secret) LogValue() slog.Value         { return slog.StringValue(Redacted) }
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + Redacted + `"`), nil }
func (Secret) MarshalText() ([]byte, error) { return []byte(Redacted), nil }

// Resolver turns a Provider's CredentialRef into the credential it names.
// It is called once per Connect, never stored, and its errors are reported
// with the reference, which is not secret.
type Resolver interface {
	Resolve(ctx context.Context, ref string) (Secret, error)
}

// ResolverFunc adapts a function into a Resolver.
type ResolverFunc func(ctx context.Context, ref string) (Secret, error)

// Resolve implements Resolver.
func (f ResolverFunc) Resolve(ctx context.Context, ref string) (Secret, error) { return f(ctx, ref) }

// EnvResolver resolves references of the form "env:NAME" from this process's
// environment. Other schemes are refused, so a deployment that also uses a
// vault composes resolvers rather than having one guess.
func EnvResolver() Resolver {
	return ResolverFunc(func(_ context.Context, ref string) (Secret, error) {
		name, ok := strings.CutPrefix(ref, "env:")
		if !ok || name == "" {
			return Secret{}, fmt.Errorf("%w: %q is not an env: reference", ErrCredential, ref)
		}
		v, ok := os.LookupEnv(name)
		if !ok || v == "" {
			return Secret{}, fmt.Errorf("%w: %q is not set", ErrCredential, ref)
		}
		return NewSecret(v), nil
	})
}

// redactor removes a credential's value from text that crosses out of this
// package: errors, tool results and log records. It exists because the
// server is not trusted to keep the secret out of what it sends back, and
// neither is the HTTP stack to keep it out of what it reports.
type redactor struct{ secret Secret }

func (r redactor) text(s string) string {
	v := r.secret.Reveal()
	if v == "" {
		return s
	}
	return strings.ReplaceAll(s, v, Redacted)
}

// err returns an error whose message is redacted. It does not unwrap to the
// original, which could be printed; it answers errors.Is for it instead, so
// callers can still match context.Canceled or a sentinel.
func (r redactor) err(err error) error {
	if err == nil {
		return nil
	}
	return &redactedError{msg: r.text(err.Error()), inner: err}
}

type redactedError struct {
	msg   string
	inner error
}

func (e *redactedError) Error() string              { return e.msg }
func (e *redactedError) Is(target error) bool       { return errors.Is(e.inner, target) }
func (e *redactedError) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(e.msg)) }

// redactingHandler scrubs a credential from every record a logger emits:
// its message and every string attribute, inside groups too.
type redactingHandler struct {
	next slog.Handler
	r    redactor
}

func (h redactingHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

func (h redactingHandler) Handle(ctx context.Context, rec slog.Record) error {
	out := slog.NewRecord(rec.Time, rec.Level, h.r.text(rec.Message), rec.PC)
	rec.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(h.attr(a))
		return true
	})
	return h.next.Handle(ctx, out)
}

func (h redactingHandler) WithAttrs(as []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, len(as))
	for i, a := range as {
		clean[i] = h.attr(a)
	}
	return redactingHandler{next: h.next.WithAttrs(clean), r: h.r}
}

func (h redactingHandler) WithGroup(name string) slog.Handler {
	return redactingHandler{next: h.next.WithGroup(name), r: h.r}
}

func (h redactingHandler) attr(a slog.Attr) slog.Attr {
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindString:
		return slog.String(a.Key, h.r.text(v.String()))
	case slog.KindGroup:
		group := v.Group()
		clean := make([]any, len(group))
		for i, g := range group {
			clean[i] = h.attr(g)
		}
		return slog.Group(a.Key, clean...)
	case slog.KindAny:
		if err, ok := v.Any().(error); ok {
			return slog.String(a.Key, h.r.text(err.Error()))
		}
		return slog.String(a.Key, h.r.text(fmt.Sprint(v.Any())))
	default:
		return slog.Attr{Key: a.Key, Value: v}
	}
}
