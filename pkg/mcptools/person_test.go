package mcptools_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/mcptools"
	"github.com/urmzd/fiber/pkg/toolprovider"
	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

// The credentials in play. Each must reach the server only for its owner,
// and none may appear anywhere else.
const (
	anaToken    = "ana-own-token-do-not-print"
	bobToken    = "bob-own-token-do-not-print"
	sharedToken = "shared-token-never-a-fallback"
	adminToken  = "admin-review-token"
)

var (
	ana   = event.Actor{ID: "U-ana", User: "u-ana", Display: "Ana"}
	bob   = event.Actor{ID: "U-bob", User: "u-bob", Display: "Bob"}
	carol = event.Actor{ID: "U-carol", User: "u-carol"}
)

// authLog serves f over streamable HTTP and records the Authorization header
// of every request that does work, in order. A session ending itself (the
// DELETE a streamable HTTP client sends on Close) is recorded apart: it is
// the session that held a credential saying goodbye with it, not a use of it.
type authLog struct {
	mu     sync.Mutex
	seen   []string
	closes []string
}

func (l *authLog) add(method, h string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if method == http.MethodDelete {
		l.closes = append(l.closes, h)
		return
	}
	l.seen = append(l.seen, h)
}

func (l *authLog) closed() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.closes)
}

func (l *authLog) last() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.seen) == 0 {
		return ""
	}
	return l.seen[len(l.seen)-1]
}

func (l *authLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.seen)
}

func serveLogged(t *testing.T, f *fake) (string, *authLog) {
	t.Helper()
	log := &authLog{}
	h := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return f.srv }, nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(r.Method, r.Header.Get("Authorization"))
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/mcp", log
}

// refs resolves the references this file's providers and people name, and
// records every one it is asked for.
type refs struct {
	mu    sync.Mutex
	asked []string
}

func (r *refs) Resolve(_ context.Context, ref string) (mcptools.Secret, error) {
	r.mu.Lock()
	r.asked = append(r.asked, ref)
	r.mu.Unlock()
	switch ref {
	case "vault:shared":
		return mcptools.NewSecret(sharedToken), nil
	case "vault:people/u-bob":
		return mcptools.NewSecret(bobToken), nil
	}
	return mcptools.Secret{}, fmt.Errorf("no such reference %q", ref)
}

func (r *refs) askedFor() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.asked)
}

// people is a host's per-account credentials: ana's is held as a secret,
// bob's as a reference, carol has none. It records who it was asked about.
type people struct {
	mu    sync.Mutex
	asked []event.Actor
	creds map[string]mcptools.PersonCredential
}

func newPeople() *people {
	return &people{creds: map[string]mcptools.PersonCredential{
		"u-ana": {Secret: mcptools.NewSecret(anaToken)},
		"u-bob": {Ref: "vault:people/u-bob"},
	}}
}

func (p *people) ResolvePerson(_ context.Context, tenant string, prov toolprovider.Provider, a event.Actor) (mcptools.PersonCredential, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.asked = append(p.asked, a)
	if tenant != prov.Tenant || prov.Scope() != toolprovider.ScopePerson {
		return mcptools.PersonCredential{}, fmt.Errorf("asked about %s/%s (%s)", tenant, prov.Name, prov.Scope())
	}
	c, ok := p.creds[a.User]
	if !ok {
		return mcptools.PersonCredential{}, fmt.Errorf("%w: %s", mcptools.ErrNoPersonCredential, a.User)
	}
	return c, nil
}

func (p *people) set(user string, c mcptools.PersonCredential) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.creds[user] = c
}

// personWorld is a person-scoped provider "fake" served over HTTP, with
// search and echo_auth accepted as read by an admin reviewing it with a
// credential of their own.
type personWorld struct {
	store  toolprovider.Store
	auth   *authLog
	refs   *refs
	people *people
	dials  atomic.Int32
	grants *mcptools.Grants
	logs   strings.Builder
	logMu  sync.Mutex
}

func newPersonWorld(t *testing.T, opts ...mcptools.GrantsOption) *personWorld {
	t.Helper()
	ctx := context.Background()
	w := &personWorld{store: toolprovider.NewMemory(), refs: &refs{}, people: newPeople()}
	f := newFake()
	url, auth := serveLogged(t, f)
	w.auth = auth
	p, err := w.store.PutProvider(ctx, toolprovider.Provider{Tenant: tenant, Name: "fake",
		Transport: toolprovider.TransportHTTP, Endpoint: url, CredentialScope: toolprovider.ScopePerson})
	if err != nil {
		t.Fatal(err)
	}
	review, err := mcptools.Connect(ctx, p, mcptools.WithPersonCredential(mcptools.PersonCredential{Secret: mcptools.NewSecret(adminToken)}))
	if err != nil {
		t.Fatalf("an admin reviewing with their own credential: %v", err)
	}
	accept(t, w.store, review, 0, map[string]toolprovider.Class{"search": toolprovider.ClassRead, "echo_auth": toolprovider.ClassRead})
	_ = review.Close()

	dial := mcptools.Dialer(mcptools.WithResolver(w.refs), mcptools.WithLogger(slogTo(&w.logs, &w.logMu)))
	counted := func(ctx context.Context, p toolprovider.Provider) (*mcptools.Session, error) {
		w.dials.Add(1)
		return dial(ctx, p)
	}
	w.grants = mcptools.NewGrants(w.store, counted, append([]mcptools.GrantsOption{mcptools.WithPersonResolver(w.people)}, opts...)...)
	t.Cleanup(func() { _ = w.grants.Close() })
	return w
}

func as(a event.Actor) context.Context { return event.WithActor(context.Background(), a) }

func member(t *testing.T, tools []saigetypes.Tool, name string) saigetypes.Tool {
	t.Helper()
	for _, tool := range tools {
		if tool.Definition().Name == name {
			return tool
		}
	}
	t.Fatalf("%s is not among %v", name, names(tools))
	return nil
}

// echo calls echo_auth through tools and returns what the server received
// with the call.
func (w *personWorld) echo(t *testing.T, tools []saigetypes.Tool) (header, result string) {
	t.Helper()
	out, err := member(t, tools, "fake__echo_auth").Execute(context.Background(), nil)
	if err != nil {
		t.Fatalf("echo_auth: %v", err)
	}
	return w.auth.last(), out
}

// Two people on one person-scoped provider each reach it with their own
// credential, on a session of their own: ana's tools carry ana's token even
// after bob's session opened, a second turn reuses the person's session, and
// what the server echoes back is redacted for the person it echoes.
func TestEachPersonReachesAPersonScopedProviderWithTheirOwnCredentialAndSession(t *testing.T) {
	w := newPersonWorld(t)

	anaTools, ok, err := w.grants.Expand(as(ana), tenant, "mcp:fake")
	if !ok || err != nil {
		t.Fatalf("Expand as ana = ok %v, %v", ok, err)
	}
	if got := names(anaTools); !slices.Equal(got, []string{"fake__echo_auth", "fake__search"}) {
		t.Fatalf("ana's group = %v", got)
	}
	for _, tool := range anaTools {
		if p, ok := tool.(interface{ ActsForPerson() bool }); !ok || !p.ActsForPerson() {
			t.Errorf("%s does not say it acts as the person, so an approver could edit it", tool.Definition().Name)
		}
	}
	bobTools, _, err := w.grants.Expand(as(bob), tenant, "mcp:fake")
	if err != nil {
		t.Fatalf("Expand as bob: %v", err)
	}

	header, out := w.echo(t, anaTools)
	if header != "Bearer "+anaToken {
		t.Errorf("ana's call reached the server with %q, want her own token", header)
	}
	if strings.Contains(out, anaToken) || !strings.Contains(out, mcptools.Redacted) {
		t.Errorf("ana's echoed credential reached the model as %q", out)
	}
	header, out = w.echo(t, bobTools)
	if header != "Bearer "+bobToken {
		t.Errorf("bob's call reached the server with %q, want his own token", header)
	}
	if strings.Contains(out, bobToken) {
		t.Errorf("bob's echoed credential reached the model as %q", out)
	}
	if header, _ := w.echo(t, anaTools); header != "Bearer "+anaToken {
		t.Errorf("after bob's session opened, ana's call carried %q", header)
	}

	if _, _, err := w.grants.Expand(as(ana), tenant, "mcp:fake:write"); err != nil {
		t.Fatal(err)
	}
	if got := w.dials.Load(); got != 2 {
		t.Errorf("two people and three expansions dialled %d sessions, want one each", got)
	}
	for _, h := range w.auth.all() {
		switch h {
		case "Bearer " + anaToken, "Bearer " + bobToken, "Bearer " + adminToken:
		default:
			t.Errorf("the server received %q", h)
		}
	}
	if slices.Contains(w.refs.askedFor(), "vault:shared") {
		t.Error("the shared reference was resolved for a person-scoped provider")
	}
	w.logMu.Lock()
	defer w.logMu.Unlock()
	for _, secret := range []string{anaToken, bobToken} {
		if strings.Contains(w.logs.String(), secret) {
			t.Errorf("a person's credential reached the log:\n%s", w.logs.String())
		}
	}
}

// A person with no credential gets no tools, and is told so: the expansion
// is ErrNoPersonCredential with a note the model may read, no session is
// dialled for them, and nobody else's credential, the provider's old shared
// one included, is used instead. The same holds for a run that acts for
// nobody and for a deployment with no way to look a credential up.
func TestAPersonWithNoCredentialGetsNoToolsAndNeverTheSharedCredential(t *testing.T) {
	ctx := context.Background()
	w := newPersonWorld(t)

	// The provider was shared before an admin moved it to per-person
	// credentials; its shared session must not outlive the move.
	p, err := w.store.GetProvider(ctx, tenant, "fake")
	if err != nil {
		t.Fatal(err)
	}
	p.CredentialScope, p.CredentialRef = toolprovider.ScopeShared, "vault:shared"
	if p, err = w.store.PutProvider(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.grants.Expand(ctx, tenant, "mcp:fake"); err != nil {
		t.Fatalf("Expand while shared: %v", err)
	}
	if w.auth.last() != "Bearer "+sharedToken {
		t.Fatalf("the shared provider was reached with %q", w.auth.last())
	}
	p.CredentialScope, p.CredentialRef = toolprovider.ScopePerson, ""
	if _, err := w.store.PutProvider(ctx, p); err != nil {
		t.Fatal(err)
	}
	seenBefore := len(w.auth.all())
	dialsBefore := w.dials.Load()

	for _, tc := range []struct {
		name  string
		ctx   context.Context
		g     *mcptools.Grants
		note  string
		names string
	}{
		{"a person who has not connected one", as(carol), w.grants, "they have not connected one for fake", `actor "U-carol" (user "u-carol") has not connected one`},
		{"a run that acts for nobody", ctx, w.grants, "made for no one", "this run acts for nobody"},
		{"a deployment with no person resolver", as(ana), mcptools.NewGrants(w.store, mcptools.Dialer(mcptools.WithResolver(w.refs))), "cannot look one up", "no PersonResolver"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tools, ok, err := tc.g.Expand(tc.ctx, tenant, "mcp:fake:write")
			if !ok || !errors.Is(err, mcptools.ErrNoPersonCredential) || len(tools) != 0 {
				t.Fatalf("Expand = %v, ok %v, %v; want no tools and ErrNoPersonCredential", names(tools), ok, err)
			}
			var noted interface{ ModelNote() string }
			if !errors.As(err, &noted) || !strings.Contains(noted.ModelNote(), tc.note) {
				t.Fatalf("the model is not told why: %v", err)
			}
			if strings.Contains(noted.ModelNote(), "U-carol") || strings.Contains(noted.ModelNote(), "u-carol") {
				t.Errorf("the model's note names the actor: %q", noted.ModelNote())
			}
			if !strings.Contains(err.Error(), tc.names) {
				t.Errorf("the operator's reason %q does not say %q", err, tc.names)
			}
		})
	}

	if got := w.dials.Load(); got != dialsBefore {
		t.Errorf("%d sessions were dialled for people with no credential", got-dialsBefore)
	}
	if got := w.auth.all()[seenBefore:]; len(got) != 0 {
		t.Errorf("the server was reached %d times after the move, with %v", len(got), got)
	}
	// The shared session did not outlive the move: it ended itself.
	if got := w.auth.closed(); !slices.Contains(got, "Bearer "+sharedToken) {
		t.Errorf("the shared session was never closed after the move (closes: %v)", got)
	}
	for _, ref := range w.refs.askedFor()[1:] {
		if ref == "vault:shared" {
			t.Error("the shared reference was resolved after the provider became person-scoped")
		}
	}
	// ana still reaches it, with her own, and the shared session is gone.
	tools, _, err := w.grants.Expand(as(ana), tenant, "mcp:fake")
	if err != nil {
		t.Fatal(err)
	}
	if header, _ := w.echo(t, tools); header != "Bearer "+anaToken {
		t.Errorf("ana reached the moved provider with %q", header)
	}
	for _, h := range w.auth.all()[seenBefore:] {
		if h == "Bearer "+sharedToken {
			t.Fatal("the shared credential reached the server after the provider became person-scoped")
		}
	}
}

// A person-scoped provider is never connected without a person's own
// credential: not with nothing, not with a shared reference smuggled onto a
// record the store would refuse, and not by a Dial that swaps in someone
// else's. And a person's credential is never given to a shared provider.
func TestAPersonScopedProviderIsNeverConnectedWithoutThatPersonsCredential(t *testing.T) {
	ctx := context.Background()
	r := &refs{}
	person := toolprovider.Provider{Tenant: tenant, Name: "fake", Transport: toolprovider.TransportHTTP,
		Endpoint: "http://127.0.0.1:1/mcp", CredentialScope: toolprovider.ScopePerson, Version: 1}

	if _, err := mcptools.Connect(ctx, person, mcptools.WithResolver(r)); !errors.Is(err, mcptools.ErrNoPersonCredential) {
		t.Errorf("Connect with no person's credential = %v, want ErrNoPersonCredential", err)
	}
	smuggled := person
	smuggled.CredentialRef = "vault:shared"
	if _, err := mcptools.Connect(ctx, smuggled, mcptools.WithResolver(r)); !errors.Is(err, toolprovider.ErrInvalid) {
		t.Errorf("Connect to a person-scoped record naming a shared reference = %v, want it refused", err)
	}
	both := mcptools.PersonCredential{Ref: "vault:people/u-bob", Secret: mcptools.NewSecret(anaToken)}
	if _, err := mcptools.Connect(ctx, person, mcptools.WithResolver(r), mcptools.WithPersonCredential(both)); !errors.Is(err, mcptools.ErrCredential) {
		t.Errorf("Connect with a reference and a secret = %v, want ErrCredential", err)
	}
	shared := person
	shared.CredentialScope, shared.CredentialRef = toolprovider.ScopeShared, "vault:shared"
	if _, err := mcptools.Connect(ctx, shared, mcptools.WithResolver(r),
		mcptools.WithPersonCredential(mcptools.PersonCredential{Secret: mcptools.NewSecret(anaToken)})); !errors.Is(err, mcptools.ErrCredential) {
		t.Errorf("Connect of a shared provider with a person's credential = %v, want ErrCredential", err)
	}
	if got := r.askedFor(); len(got) != 0 {
		t.Errorf("refused connections resolved %v", got)
	}

	// A Dial that opens the session with a credential other than the one it
	// was handed is caught and its session closed.
	w := newPersonWorld(t)
	swap := func(ctx context.Context, p toolprovider.Provider) (*mcptools.Session, error) {
		return mcptools.Connect(ctx, p, mcptools.WithResolver(w.refs),
			mcptools.WithPersonCredential(mcptools.PersonCredential{Ref: "vault:people/u-bob"}))
	}
	g := mcptools.NewGrants(w.store, swap, mcptools.WithPersonResolver(w.people))
	t.Cleanup(func() { _ = g.Close() })
	if tools, _, err := g.Expand(as(ana), tenant, "mcp:fake"); !errors.Is(err, mcptools.ErrCredential) || len(tools) != 0 {
		t.Fatalf("ana's expansion on a session opened with bob's credential = %v, %v; want it refused", names(tools), err)
	}
}

// A person's session is kept for the credential they hold: one the resolver
// answers differently (rotated, or replaced) gets a session of its own, and a
// person who disconnects theirs gets no tools on the next turn.
func TestAPersonsSessionFollowsTheCredentialTheyHoldNow(t *testing.T) {
	w := newPersonWorld(t)
	if _, _, err := w.grants.Expand(as(ana), tenant, "mcp:fake"); err != nil {
		t.Fatal(err)
	}
	const rotated = "ana-rotated-token"
	w.people.set("u-ana", mcptools.PersonCredential{Secret: mcptools.NewSecret(rotated)})
	tools, _, err := w.grants.Expand(as(ana), tenant, "mcp:fake")
	if err != nil {
		t.Fatal(err)
	}
	if header, _ := w.echo(t, tools); header != "Bearer "+rotated {
		t.Errorf("after rotation ana's call carried %q", header)
	}
	if w.dials.Load() != 2 {
		t.Errorf("dials = %d, want a new session for the rotated credential", w.dials.Load())
	}
	w.people.set("u-ana", mcptools.PersonCredential{})
	if tools, _, err := w.grants.Expand(as(ana), tenant, "mcp:fake"); !errors.Is(err, mcptools.ErrNoPersonCredential) || len(tools) != 0 {
		t.Fatalf("after ana disconnected: %v, %v", names(tools), err)
	}
}

// Person sessions are bounded: past the limit, the one expanded longest ago
// is closed, and its person is dialled afresh on their next turn.
func TestPersonSessionsAreBounded(t *testing.T) {
	w := newPersonWorld(t, mcptools.WithPersonSessions(1))
	for _, a := range []event.Actor{ana, bob, ana} {
		if _, _, err := w.grants.Expand(as(a), tenant, "mcp:fake"); err != nil {
			t.Fatal(err)
		}
	}
	if w.dials.Load() != 3 {
		t.Errorf("dials = %d, want ana's session closed for bob's and dialled again", w.dials.Load())
	}
}

// A person's credential prints as its reference or as Redacted, never as the
// secret, through every verb.
func TestAPersonCredentialNeverPrintsItsSecret(t *testing.T) {
	c := mcptools.PersonCredential{Secret: mcptools.NewSecret(anaToken)}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x"} {
		for _, v := range []any{c, &c, []mcptools.PersonCredential{c}, struct{ C mcptools.PersonCredential }{c}} {
			if got := fmt.Sprintf(verb, v); strings.Contains(got, anaToken) {
				t.Errorf("%s printed %q", verb, got)
			}
		}
	}
	if got := (mcptools.PersonCredential{Ref: "vault:people/u-bob"}).String(); got != "vault:people/u-bob" {
		t.Errorf("a reference prints as %q", got)
	}
}
