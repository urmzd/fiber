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

// Dial opens a session to a provider. Dialer is the default; a test or an
// embedding host supplies its own.
type Dial func(ctx context.Context, p toolprovider.Provider) (*Session, error)

// Dialer returns the Dial that calls Connect with opts.
func Dialer(opts ...Option) Dial {
	return func(ctx context.Context, p toolprovider.Provider) (*Session, error) {
		return Connect(ctx, p, opts...)
	}
}

// Grants expands an agent's MCP provider grants into the tools they stand
// for. A grant is "mcp:<provider>" (read tools) or "mcp:<provider>:write"
// (read and write tools), parsed by agentspec.ParseMCPGrant; it expands to
// the provider's tools in its current accepted snapshot that the server
// still offers unchanged and the grant's class permits, named
// "<provider>_<tool>". Held-back tools never appear, and the class is
// enforced again by each tool at call time (see Toolset).
//
// Sessions are kept open per tenant, provider and provider version, so a
// stdio server is not started per turn, and a provider record that changes
// (a new endpoint, a rotated credential reference) gets a new session on the
// next expansion. A session that fails is dropped and dialled once more.
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

// Expand returns the tools grant stands for in tenant. ok is false for a
// name that is not an MCP grant, which the caller resolves as an ordinary
// tool. An error means the grant could not be expanded right now (no such
// provider, the server unreachable); the caller decides whether that costs
// the run or only the tools.
func (g *Grants) Expand(ctx context.Context, tenant, grant string) ([]saigetypes.Tool, bool, error) {
	parsed, ok, err := agentspec.ParseMCPGrant(grant)
	if !ok {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
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

// toolset builds the grant's view on a cached session, redialling once if
// the cached one has failed.
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
		return nil, false, errors.New("mcptools: grants closed")
	}
	cur := g.sessions[k]
	if cur != nil && cur.provider.Version == p.Version {
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
		return nil, false, errors.New("mcptools: grants closed")
	}
	if won := g.sessions[k]; won != nil && won.provider.Version == p.Version {
		// Another turn dialled the same version first; keep one.
		_ = s.Close()
		return won, false, nil
	}
	if old := g.sessions[k]; old != nil {
		// A superseded version of the record. Turns already holding its
		// tools fail their next call, which lists first, and say why.
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

// Close ends every open session. Expand fails afterwards.
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
