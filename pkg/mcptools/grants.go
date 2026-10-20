package mcptools

import (
	"context"
	"errors"
	"fmt"
	"sync"

	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

// ErrClosed reports an expansion after Grants.Close.
var ErrClosed = errors.New("mcptools: grants closed")

// Dial opens a session to a provider. Dialer is the default; a test or an
// embedding host supplies its own.
type Dial func(ctx context.Context, p toolprovider.Provider) (*Session, error)

// Dialer returns the Dial that calls Connect with opts.
func Dialer(opts ...Option) Dial {
	return func(ctx context.Context, p toolprovider.Provider) (*Session, error) {
		return Connect(ctx, p, opts...)
	}
}

// Grants expands an agent's MCP group grants against a toolprovider.Store. A
// grant is "mcp:<provider>" (read tools) or "mcp:<provider>:write" (read and
// write tools), parsed by agentspec.ParseMCPGrant. It expands, in one tenant,
// to the tools of the provider's current accepted snapshot that the server
// still offers unchanged and the grant's class permits, each named by
// toolprovider.QualifiedName. Held-back tools never appear, and every member
// is a Toolset callable, so the class is checked again on each call: a read
// grant cannot reach a write tool by any path.
//
// It is the store-backed source of tool groups: pkg/runtime's
// WithToolGroupSource takes a *Grants and hands what it expands to
// pkg/agentrt as an agentrt.ToolGroup named by the grant, whose members the
// runner registers. This package does not import agentrt.
//
// Sessions are kept open per tenant and provider, for the provider record's
// version, so a stdio server is not started per turn, and a record that
// changes (a new endpoint, a rotated credential reference) gets a new session
// on the next expansion. A session that fails is dropped and dialled once
// more.
type Grants struct {
	store toolprovider.Store
	dial  Dial

	mu       sync.Mutex
	sessions map[sessionKey]*Session
	closed   bool
}

type sessionKey struct {
	tenant, provider string
}

// NewGrants expands grants against store, opening sessions with dial (nil
// takes Dialer() with no options, which reaches no provider that names a
// credential).
func NewGrants(store toolprovider.Store, dial Dial) *Grants {
	if dial == nil {
		dial = Dialer()
	}
	return &Grants{store: store, dial: dial, sessions: map[sessionKey]*Session{}}
}

// Expand returns the tools grant stands for in tenant. ok is false for a name
// that is not an MCP group grant on a name a provider can have, which the
// caller resolves some other way. An error means the grant could not be
// expanded right now (no such provider in this tenant, the server
// unreachable).
func (g *Grants) Expand(ctx context.Context, tenant, grant string) (tools []saigetypes.Tool, ok bool, err error) {
	parsed, ok, err := agentspec.ParseMCPGrant(grant)
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
	ts, err := g.toolset(ctx, p, snap, class)
	if err != nil {
		return nil, true, fmt.Errorf("mcptools: grant %q: %w", grant, err)
	}
	return ts.All(), true, nil
}

// toolset builds the grant's view on a cached session, redialling once if the
// cached one has failed.
func (g *Grants) toolset(ctx context.Context, p toolprovider.Provider, snap toolprovider.Snapshot, class toolprovider.Grant) (*Toolset, error) {
	s, fresh, err := g.session(ctx, p)
	if err != nil {
		return nil, err
	}
	ts, err := s.Toolset(ctx, snap, class)
	if err == nil || fresh || ctx.Err() != nil {
		return ts, err
	}
	g.evict(p, s)
	s, _, err = g.session(ctx, p)
	if err != nil {
		return nil, err
	}
	return s.Toolset(ctx, snap, class)
}

// session returns the open session for p's current version, dialling one if
// there is none. fresh reports a session dialled by this call.
func (g *Grants) session(ctx context.Context, p toolprovider.Provider) (*Session, bool, error) {
	k := sessionKey{p.Tenant, p.Name}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil, false, ErrClosed
	}
	if cur := g.sessions[k]; cur != nil && cur.provider.Version == p.Version {
		g.mu.Unlock()
		return cur, false, nil
	}
	g.mu.Unlock()

	// The session outlives the turn that opened it, so it is not bound to
	// that turn's cancellation.
	s, err := g.dial(context.WithoutCancel(ctx), p)
	if err != nil {
		return nil, false, err
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		_ = s.Close()
		return nil, false, ErrClosed
	}
	if won := g.sessions[k]; won != nil && won.provider.Version == p.Version {
		// Another turn dialled the same version first; keep one.
		_ = s.Close()
		return won, false, nil
	}
	if old := g.sessions[k]; old != nil {
		// A superseded version of the record. A turn still holding its
		// tools fails its next call, which lists first, and says why.
		_ = old.Close()
	}
	g.sessions[k] = s
	return s, true, nil
}

func (g *Grants) evict(p toolprovider.Provider, s *Session) {
	k := sessionKey{p.Tenant, p.Name}
	g.mu.Lock()
	if g.sessions[k] == s {
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
	for k, s := range g.sessions {
		err = errors.Join(err, s.Close())
		delete(g.sessions, k)
	}
	return err
}
