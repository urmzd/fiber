package mcptools

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/toolprovider"
	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

// ErrClosed reports an expansion after Grants.Close.
var ErrClosed = errors.New("mcptools: grants closed")

// Dial opens a session to a provider. Dialer is the default; a test or an
// embedding host supplies its own, which must hand ctx to Connect: that is
// how a person's credential reaches a person-scoped session, and a Dial that
// drops it opens nothing for that person rather than something else.
type Dial func(ctx context.Context, p toolprovider.Provider) (*Session, error)

// Dialer returns the Dial that calls Connect with opts.
func Dialer(opts ...Option) Dial {
	return func(ctx context.Context, p toolprovider.Provider) (*Session, error) {
		return Connect(ctx, p, opts...)
	}
}

// Grants expands an agent's MCP group grants against a toolprovider.Store. A
// grant is "mcp:<provider>" (read tools) or "mcp:<provider>:write" (read and
// write tools), parsed by definition.ParseMCPGrant. A definition lists the
// server under tools.mcp and its pinned Policy decides the class
// (Policy.MCPWrite); definition.Parse compiles the two into the grant, so the
// write class reaches here from the policy and from nothing else. It expands, in one tenant,
// to the tools of the provider's current accepted snapshot that the server
// still offers unchanged and the grant's class permits, each named by
// toolprovider.QualifiedName. Held-back tools never appear, and every member
// is a Toolset callable, so the class is checked again on each call: a read
// grant cannot reach a write tool by any path.
//
// It is the store-backed source of tool groups: pkg/runtime's
// WithToolGroupSource takes a *Grants and hands what it expands to
// pkg/turn/native as a native.ToolGroup named by the grant, whose members the
// runner registers. This package does not import native.
//
// Sessions are kept open per tenant and provider, for the provider record's
// version, so a stdio server is not started per turn, and a record that
// changes (a new endpoint, a rotated credential reference) gets a new session
// on the next expansion. A session that fails is dropped and dialled once
// more.
//
// # Per-person credentials
//
// A person-scoped provider (toolprovider.ScopePerson) is expanded for the
// person the run acts for: the actor pkg/runtime puts on the turn's context
// from the run's record (event.ActorFrom). Each expansion asks the
// PersonResolver (WithPersonResolver) for that person's credential and
// expands on a session of their own, kept per provider record version and
// person and opened with exactly that credential; a credential the resolver
// answers differently (rotated, or replaced) gets a new session. When the
// run acts for nobody, no resolver is configured, or the person has no
// credential, the grant is not expanded: Expand reports
// ErrNoPersonCredential, which a runtime drops as an unavailable group, the
// operator log names the provider and the person, and the model is told (as
// an native.ModelNote) that the person has not connected one. The shared
// credential is never used, and no person's session is ever handed to
// another. At most WithPersonSessions person sessions are kept open; past
// that the one expanded longest ago is closed.
type Grants struct {
	store   toolprovider.Store
	dial    Dial
	persons PersonResolver
	limit   int

	mu       sync.Mutex
	sessions map[sessionKey]*held
	tick     uint64
	closed   bool
}

type sessionKey struct {
	tenant, provider string
	// person is zero for a shared provider's one session.
	person personKey
}

// held is an open session and what it was opened for.
type held struct {
	s *Session
	// cred is the person's credential a person-scoped session was opened
	// with; zero for a shared provider.
	cred PersonCredential
	// used is when it was last expanded, in Grants ticks.
	used uint64
}

// DefaultPersonSessions is how many person sessions Grants keeps open by
// default. Each is a connection, and for a stdio provider a process.
const DefaultPersonSessions = 128

// GrantsOption configures Grants.
type GrantsOption func(*Grants)

// WithPersonResolver sets how a person-scoped provider finds the credential
// of the person a run acts for. Without one, every person-scoped grant
// expands to nothing, and never to the shared credential.
func WithPersonResolver(r PersonResolver) GrantsOption {
	return func(g *Grants) { g.persons = r }
}

// WithPersonSessions bounds how many person sessions are kept open at once
// (default DefaultPersonSessions). Past it, the one expanded longest ago is
// closed; a turn still holding its tools fails its next call and says why.
func WithPersonSessions(n int) GrantsOption {
	return func(g *Grants) {
		if n > 0 {
			g.limit = n
		}
	}
}

// NewGrants expands grants against store, opening sessions with dial (nil
// takes Dialer() with no options, which reaches no provider that names a
// credential).
func NewGrants(store toolprovider.Store, dial Dial, opts ...GrantsOption) *Grants {
	if dial == nil {
		dial = Dialer()
	}
	g := &Grants{store: store, dial: dial, limit: DefaultPersonSessions, sessions: map[sessionKey]*held{}}
	for _, o := range opts {
		o(g)
	}
	return g
}

// person is who a person-scoped expansion is for, and their credential.
type person struct {
	key  personKey
	cred PersonCredential
}

// personFor resolves the credential of the person ctx acts for on p. Every
// way of having none is ErrNoPersonCredential, with a note for the model.
func (g *Grants) personFor(ctx context.Context, p toolprovider.Provider) (*person, error) {
	actor, ok := event.ActorFrom(ctx)
	if !ok {
		return nil, actingForNobody(p.Name)
	}
	if g.persons == nil {
		return nil, noPersonResolver(p.Name)
	}
	cred, err := g.persons.ResolvePerson(ctx, p.Tenant, p, actor)
	switch {
	case errors.Is(err, ErrNoPersonCredential) || (err == nil && cred.Empty()):
		return nil, notConnected(p.Name, actor)
	case err != nil:
		return nil, fmt.Errorf("%w: provider %q: look up the credential of actor %q: %w", ErrCredential, p.Name, actor.ID, err)
	case cred.Ref != "" && !cred.Secret.Empty():
		return nil, fmt.Errorf("%w: provider %q: the credential of actor %q is a reference and a secret; want one", ErrCredential, p.Name, actor.ID)
	}
	return &person{key: keyOf(actor), cred: cred}, nil
}

// Expand returns the tools grant stands for in tenant. ok is false for a name
// that is not an MCP group grant on a name a provider can have, which the
// caller resolves some other way. An error means the grant could not be
// expanded right now (no such provider in this tenant, the server
// unreachable).
func (g *Grants) Expand(ctx context.Context, tenant, grant string) (tools []saigetypes.Tool, ok bool, err error) {
	parsed, ok, err := definition.ParseMCPGrant(grant)
	if !ok || err != nil || !toolprovider.ValidName(parsed.Provider) {
		return nil, false, nil
	}
	p, err := g.store.GetProvider(ctx, tenant, parsed.Provider)
	if err != nil {
		return nil, true, fmt.Errorf("mcptools: grant %q: %w", grant, err)
	}
	snap, err := g.store.Snapshot(ctx, tenant, parsed.Provider, 0)
	if err != nil {
		return nil, true, fmt.Errorf("mcptools: grant %q: %w", grant, err)
	}
	class := toolprovider.GrantRead
	if parsed.Write {
		class = toolprovider.GrantWrite
	}
	g.sweep(p)
	var who *person
	if p.Scope() == toolprovider.ScopePerson {
		if who, err = g.personFor(ctx, p); err != nil {
			return nil, true, fmt.Errorf("mcptools: grant %q: %w", grant, err)
		}
	}
	ts, err := g.toolset(ctx, p, who, snap, class)
	if err != nil {
		return nil, true, fmt.Errorf("mcptools: grant %q: %w", grant, err)
	}
	return ts.All(), true, nil
}

// toolset builds the grant's view on a cached session, redialling once if the
// cached one has failed.
func (g *Grants) toolset(ctx context.Context, p toolprovider.Provider, who *person, snap toolprovider.Snapshot, class toolprovider.Grant) (*Toolset, error) {
	s, fresh, err := g.session(ctx, p, who)
	if err != nil {
		return nil, err
	}
	ts, err := s.Toolset(ctx, snap, class)
	if err == nil || fresh || ctx.Err() != nil {
		return ts, err
	}
	g.evict(keyFor(p, who), s)
	s, _, err = g.session(ctx, p, who)
	if err != nil {
		return nil, err
	}
	return s.Toolset(ctx, snap, class)
}

func keyFor(p toolprovider.Provider, who *person) sessionKey {
	k := sessionKey{tenant: p.Tenant, provider: p.Name}
	if who != nil {
		k.person = who.key
	}
	return k
}

// current reports whether h may serve p for who: the record's version, and
// for a person the very credential they hold now.
func (h *held) current(p toolprovider.Provider, who *person) bool {
	if h == nil || h.s.provider.Version != p.Version {
		return false
	}
	return who == nil || h.cred.same(who.cred)
}

// session returns the open session for p's current version (and for who,
// with their current credential), dialling one if there is none. fresh
// reports a session dialled by this call.
func (g *Grants) session(ctx context.Context, p toolprovider.Provider, who *person) (*Session, bool, error) {
	k := keyFor(p, who)
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil, false, ErrClosed
	}
	if cur := g.sessions[k]; cur.current(p, who) {
		g.tick++
		cur.used = g.tick
		g.mu.Unlock()
		return cur.s, false, nil
	}
	g.mu.Unlock()

	// The session outlives the turn that opened it, so it is not bound to
	// that turn's cancellation. A person's session is dialled with their
	// credential and with nothing else.
	dctx := context.WithoutCancel(ctx)
	h := &held{}
	if who != nil {
		dctx = withPersonCredential(dctx, who.cred)
		h.cred = who.cred
	}
	s, err := g.dial(dctx, p)
	if err != nil {
		return nil, false, err
	}
	if who != nil && !s.person.same(who.cred) {
		// A Dial that opened the session with something other than what it
		// was handed. Whatever it holds is not this person's.
		_ = s.Close()
		return nil, false, fmt.Errorf("%w: provider %q: the session was not opened with the credential of the person it is for", ErrCredential, p.Name)
	}
	h.s = s

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		_ = s.Close()
		return nil, false, ErrClosed
	}
	g.tick++
	if won := g.sessions[k]; won.current(p, who) {
		// Another turn dialled the same version first; keep one.
		_ = s.Close()
		won.used = g.tick
		return won.s, false, nil
	}
	if old := g.sessions[k]; old != nil {
		// A superseded version of the record, or a credential the person
		// no longer holds. A turn still holding its tools fails its next
		// call, which lists first, and says why.
		_ = old.s.Close()
	}
	h.used = g.tick
	g.sessions[k] = h
	if who != nil {
		g.trim(k)
	}
	return s, true, nil
}

// sweep closes every session of p's provider opened from an older version of
// its record, whoever it is for. A record moved to per-person credentials
// leaves no shared session open behind it, and one moved back leaves no
// person's.
func (g *Grants) sweep(p toolprovider.Provider) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k, h := range g.sessions {
		if k.tenant == p.Tenant && k.provider == p.Name && h.s.provider.Version < p.Version {
			_ = h.s.Close()
			delete(g.sessions, k)
		}
	}
}

// trim closes the person sessions expanded longest ago until at most limit
// remain, sparing keep. It is called with g.mu held.
func (g *Grants) trim(keep sessionKey) {
	for {
		var (
			oldest sessionKey
			found  bool
			n      int
		)
		for k, h := range g.sessions {
			if k.person == (personKey{}) {
				continue
			}
			n++
			if k != keep && (!found || h.used < g.sessions[oldest].used) {
				oldest, found = k, true
			}
		}
		if n <= g.limit || !found {
			return
		}
		_ = g.sessions[oldest].s.Close()
		delete(g.sessions, oldest)
	}
}

func (g *Grants) evict(k sessionKey, s *Session) {
	g.mu.Lock()
	if h := g.sessions[k]; h != nil && h.s == s {
		delete(g.sessions, k)
	}
	g.mu.Unlock()
	_ = s.Close()
}

// Close ends every open session. Expand fails with ErrClosed afterwards.
func (g *Grants) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	var err error
	for k, h := range g.sessions {
		err = errors.Join(err, h.s.Close())
		delete(g.sessions, k)
	}
	return err
}
