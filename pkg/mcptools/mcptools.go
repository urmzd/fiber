// Package mcptools connects to the MCP servers pkg/toolprovider records and
// turns the tools an admin accepted into callables an agent can use.
//
// It answers one question: given a provider and its accepted snapshot, which
// tools may this caller invoke right now, and what happens when it does. What
// a provider is and which tools were accepted belong to pkg/toolprovider;
// which agent holds which grant belongs to the agent's spec and to
// pkg/turn/native, which this package does not import.
//
// # Only accepted tools are callable
//
// A Session lists what the server offers and compares it with the accepted
// snapshot (toolprovider.Compare). A tool is callable only when the server
// offers it exactly as accepted: same name, same description, same schema
// hash. A new tool, or one whose description or schema changed, is held back
// as pending until an admin accepts it again. The check is repeated at call
// time: every call lists the server's tools first and refuses if the tool is
// gone or described differently, so a tool that changed under a live session
// is not called with the old definition shown to the model. This costs one
// round trip per call. It cannot stop a server that changes what a tool does
// without changing how it describes it; nothing on the client can, and that
// is what the admin's trust in the provider covers.
//
// # Read and write at call time
//
// A Toolset is built for one toolprovider.Grant. Lookup refuses a tool the
// grant does not permit, All leaves it out, and Execute checks again before
// it sends anything, so a read-only grant cannot reach a write tool by any
// path. A tool's class is the admin's declaration in the snapshot; the
// server's readOnlyHint is never consulted.
//
// # Credentials
//
// A provider's CredentialRef is resolved through a Resolver when Connect is
// called, and the value is held only by the HTTP round tripper (as a bearer
// token, sent only to the endpoint's own origin and never across a redirect
// to another host, port or plain HTTP) or the stdio command's environment (in CredentialEnv). Every error,
// tool result and log record that leaves this package is scrubbed of the
// value, and Secret prints as Redacted through every fmt verb, slog and JSON.
// A stdio server inherits only PATH and HOME from this process, not its
// whole environment, so it cannot read credentials meant for something else.
//
// # Whose credential
//
// A shared provider (toolprovider.ScopeShared, the default) is reached with
// its CredentialRef, for everyone. A person-scoped one (ScopePerson) is
// reached with the credential of one person, and is never opened without it:
// Connect refuses it with ErrNoPersonCredential unless it is given that
// person's PersonCredential, and it never reads the record's CredentialRef,
// which such a record cannot have. Grants finds each person's credential
// through a PersonResolver, for the actor the run acts for, and keeps a
// session per person; an admin reviews such a provider with a credential of
// their own (WithPersonCredential). A person with none gets no tools, never
// the shared credential and never anyone else's.
//
// # The seam with pkg/turn/native
//
// native resolves a spec's tool names through its Tools interface,
// Lookup(name string) (saige types.Tool, error), and runs what it gets back.
// *Toolset has exactly that method, so the wiring is
//
//	ts, err := session.Toolset(ctx, snapshot, grant)
//	var tools native.Tools = ts
//
// and each callable is a plain saige types.Tool whose Definition carries the
// qualified name (toolprovider.QualifiedName, "<provider>__<tool>"), the
// accepted description and schema, and a Capability of read or write.
// Execute returns a Go error for a refusal (ErrHeldBack, ErrDenied) and for a
// tool that reported failure, which saige records as the call's error and
// shows the model; the turn continues.
//
// # Provider grants
//
// A spec grants a whole provider as "mcp:<name>" (its read tools) or
// "mcp:<name>:write" (read and write). Grants expands such a grant against
// the store, for one tenant, into the members of that tool group, under the
// grant's class, on a session kept per provider record version, and for a
// person-scoped provider per person as well. pkg/runtime's
// WithToolGroupSource takes a *Grants and hands the members to pkg/turn/native
// as a native.ToolGroup, so this package stays independent of native; the
// person a turn acts for reaches it on the context (event.ActorFrom), and a
// person with no credential is reported with a note native shows the model
// (native.ModelNote, which this package satisfies by shape).
package mcptools

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/urmzd/fiber/pkg/toolprovider"
)

// Sentinels, matched with errors.Is.
var (
	// ErrCredential reports a credential reference that could not be
	// resolved, or a provider that names one with no Resolver configured.
	ErrCredential = errors.New("mcptools: credential")

	// ErrHeldBack reports a tool that is not callable because it was never
	// accepted, or because the server changed it since it was.
	ErrHeldBack = errors.New("mcptools: tool held back pending acceptance")

	// ErrRedirectDowngrade reports a provider served over HTTPS that
	// redirected to plain HTTP. The redirect is not followed.
	ErrRedirectDowngrade = errors.New("mcptools: refused a redirect from https to http")

	// ErrDenied reports a call the grant does not permit, such as a write
	// tool under a read-only grant.
	ErrDenied = errors.New("mcptools: denied by grant")

	// ErrToolFailed reports a tool that ran and reported failure.
	ErrToolFailed = errors.New("mcptools: tool failed")

	// ErrUnsupportedSchema reports an accepted tool whose input schema
	// cannot be offered to a model as written: not an object, or using a
	// construct saige cannot represent (a union of several types, allOf, a
	// $ref that is not local, names no definition, or is recursive). The
	// error names where in the schema and why.
	ErrUnsupportedSchema = errors.New("mcptools: unsupported input schema")
)

// Option configures Connect.
type Option func(*config)

type config struct {
	resolver   Resolver
	logger     *slog.Logger
	httpClient *http.Client
	transport  mcp.Transport
	env        []string
	person     *PersonCredential
}

// WithResolver sets how CredentialRef is resolved. Without one, a provider
// that names a credential fails to connect with ErrCredential.
func WithResolver(r Resolver) Option { return func(c *config) { c.resolver = r } }

// WithLogger sets the logger for this session and the MCP client under it.
// Every record is scrubbed of the credential before it reaches the handler.
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// WithHTTPClient sets the base client for HTTP providers. Its transport is
// wrapped to add the credential; the client passed in is not modified.
func WithHTTPClient(h *http.Client) Option { return func(c *config) { c.httpClient = h } }

// WithEnv adds KEY=VALUE pairs to a stdio server's environment, on top of
// PATH, HOME and the credential.
func WithEnv(kv ...string) Option { return func(c *config) { c.env = append(c.env, kv...) } }

// WithTransport replaces the transport the provider record describes. It is
// for tests and for embedding a server in process; no credential is applied.
// A person-scoped provider still needs a person's credential to connect, and
// its value is still redacted from everything the session returns.
func WithTransport(t mcp.Transport) Option { return func(c *config) { c.transport = t } }

// WithPersonCredential opens the session with one person's own credential,
// which a person-scoped provider requires: it is how an admin reviews such a
// provider's tools (Session.Diff) with a credential of their own. Grants
// passes each person's for them. It is refused for a shared provider, whose
// credential is its CredentialRef.
func WithPersonCredential(cred PersonCredential) Option {
	return func(c *config) { c.person = &cred }
}

// Session is a live connection to one provider.
type Session struct {
	provider toolprovider.Provider
	cs       *mcp.ClientSession
	red      redactor
	logger   *slog.Logger
	// person is the credential a person-scoped session was opened with, as
	// it was given: Grants checks it is the one it asked for.
	person PersonCredential
}

// Connect resolves p's credential and opens an MCP session to it.
//
// For a person-scoped provider the credential is the person's: the one
// WithPersonCredential gives, or the one Grants dials that person's session
// with, through ctx (so a Dial must pass its ctx on). Without one Connect
// opens nothing and reports ErrNoPersonCredential.
func Connect(ctx context.Context, p toolprovider.Provider, opts ...Option) (*Session, error) {
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("mcptools: %w", err)
	}
	var c config
	for _, o := range opts {
		o(&c)
	}
	secret, person, err := c.credential(ctx, p)
	if err != nil {
		return nil, err
	}
	red := redactor{secret: secret}
	base := c.logger
	if base == nil {
		base = slog.New(slog.DiscardHandler)
	}
	logger := slog.New(redactingHandler{next: base.Handler(), r: red}).With("provider", p.Name)

	s := &Session{provider: p, red: red, logger: logger, person: person}
	transport := c.transport
	if transport == nil {
		transport = buildTransport(p, secret, c)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "fiber", Version: "v0"}, &mcp.ClientOptions{Logger: logger})
	cs, err := client.Connect(ctx, transport, nil)
	if err != nil {
		return nil, red.err(fmt.Errorf("mcptools: connect %q: %w", p.Name, err))
	}
	s.cs = cs
	logger.Debug("mcptools: connected", "transport", string(p.Transport), "credential", secret)
	return s, nil
}

// credential is the secret a session to p is opened with, and for a
// person-scoped provider the person's credential it came from.
func (c config) credential(ctx context.Context, p toolprovider.Provider) (Secret, PersonCredential, error) {
	if p.Scope() == toolprovider.ScopePerson {
		// The record's CredentialRef is never read here: Validate refuses
		// one on a person-scoped provider, and a session opened with it
		// would be the shared fallback this scope exists to rule out.
		var cred PersonCredential
		switch from, ok := personCredentialFrom(ctx); {
		case c.person != nil:
			cred = *c.person
		case ok:
			cred = from
		}
		secret, err := cred.resolve(ctx, c.resolver, p.Name)
		if err != nil {
			return Secret{}, PersonCredential{}, err
		}
		return secret, cred, nil
	}
	if c.person != nil {
		return Secret{}, PersonCredential{}, fmt.Errorf("%w: provider %q is shared; a person's credential is for a person-scoped provider", ErrCredential, p.Name)
	}
	if p.CredentialRef == "" || c.transport != nil {
		return Secret{}, PersonCredential{}, nil
	}
	if c.resolver == nil {
		return Secret{}, PersonCredential{}, fmt.Errorf("%w: provider %q names %q and no resolver is configured", ErrCredential, p.Name, p.CredentialRef)
	}
	s, err := c.resolver.Resolve(ctx, p.CredentialRef)
	if err != nil {
		return Secret{}, PersonCredential{}, fmt.Errorf("%w: provider %q: resolve %q: %w", ErrCredential, p.Name, p.CredentialRef, err)
	}
	if s.Empty() {
		return Secret{}, PersonCredential{}, fmt.Errorf("%w: provider %q: %q resolved to nothing", ErrCredential, p.Name, p.CredentialRef)
	}
	return s, PersonCredential{}, nil
}

func buildTransport(p toolprovider.Provider, secret Secret, c config) mcp.Transport {
	if p.Transport == toolprovider.TransportStdio {
		cmd := exec.Command(p.Endpoint, p.Args...)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
		cmd.Env = append(cmd.Env, c.env...)
		if !secret.Empty() {
			cmd.Env = append(cmd.Env, p.CredentialEnv+"="+secret.Reveal())
		}
		return &mcp.CommandTransport{Command: cmd}
	}
	base := http.DefaultClient
	if c.httpClient != nil {
		base = c.httpClient
	}
	client := *base
	if !secret.Empty() {
		next := client.Transport
		if next == nil {
			next = http.DefaultTransport
		}
		origin, ok := originOf(p.Endpoint)
		client.Transport = bearer{next: next, secret: secret, origin: origin, ok: ok}
		client.CheckRedirect = noDowngrade(client.CheckRedirect)
	}
	return &mcp.StreamableClientTransport{Endpoint: p.Endpoint, HTTPClient: &client}
}

// bearer adds the credential to requests bound for the provider's own origin
// (scheme, host and port of its endpoint) and to nothing else. A redirect to
// another host, another port or plain HTTP gets no Authorization header, so a
// server, or anyone who can make it redirect, cannot send the credential
// somewhere the admin did not configure.
type bearer struct {
	next   http.RoundTripper
	secret Secret
	origin string
	ok     bool
}

func (b bearer) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Del("Authorization")
	if o, ok := originOf(req.URL.String()); b.ok && ok && o == b.origin {
		req.Header.Set("Authorization", "Bearer "+b.secret.Reveal())
	}
	return b.next.RoundTrip(req)
}

// originOf returns raw's scheme://host:port with the scheme and host lowered
// and the default port filled in, so equal origins compare equal as strings.
func originOf(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", false
	}
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	if port == "" {
		switch scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		default:
			return "", false
		}
	}
	return scheme + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port), true
}

// noDowngrade refuses a redirect from HTTPS to HTTP, then defers to next (or
// to net/http's default limit of 10 redirects when next is nil).
func noDowngrade(next func(*http.Request, []*http.Request) error) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		for _, v := range via {
			if strings.EqualFold(v.URL.Scheme, "https") && !strings.EqualFold(req.URL.Scheme, "https") {
				return ErrRedirectDowngrade
			}
		}
		if next != nil {
			return next(req, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
}

// Provider returns the record this session was opened from.
func (s *Session) Provider() toolprovider.Provider { return s.provider }

// Close ends the session and, for stdio, the server process.
func (s *Session) Close() error { return s.red.err(s.cs.Close()) }

// Offered lists the tools the server offers now, as toolprovider tools with
// canonical schemas and hashes and no class.
//
// Every error an exported method returns passes through the session's
// redactor at one point, so text a server supplied (a tool name, a schema, a
// failure message) can never carry the credential out.
func (s *Session) Offered(ctx context.Context) ([]toolprovider.Tool, error) {
	out, err := s.offered(ctx)
	return out, s.red.err(err)
}

func (s *Session) offered(ctx context.Context) ([]toolprovider.Tool, error) {
	var out []toolprovider.Tool
	for t, err := range s.cs.Tools(ctx, nil) {
		if err != nil {
			return nil, fmt.Errorf("mcptools: list tools of %q: %w", s.provider.Name, err)
		}
		schema, err := marshalSchema(t.InputSchema)
		if err != nil {
			return nil, fmt.Errorf("mcptools: tool %q of %q: %w", t.Name, s.provider.Name, err)
		}
		tool, err := toolprovider.NewTool(t.Name, t.Description, schema)
		if err != nil {
			return nil, fmt.Errorf("mcptools: provider %q: %w", s.provider.Name, err)
		}
		out = append(out, tool)
	}
	return out, nil
}

// Diff lists what the server offers and compares it with accepted, which is
// what an admin reviews before calling toolprovider.Store.Accept.
func (s *Session) Diff(ctx context.Context, accepted toolprovider.Snapshot) (toolprovider.Diff, error) {
	d, err := s.diff(ctx, accepted)
	return d, s.red.err(err)
}

func (s *Session) diff(ctx context.Context, accepted toolprovider.Snapshot) (toolprovider.Diff, error) {
	offered, err := s.offered(ctx)
	if err != nil {
		return toolprovider.Diff{}, err
	}
	return toolprovider.Compare(accepted, offered), nil
}

// current returns the server's offered form of name as of now. It lists on
// every call rather than trusting an earlier list or waiting for the server's
// list_changed notification, which is sent asynchronously (the Go SDK delays
// it on purpose) and so can arrive after a call to the changed tool.
func (s *Session) current(ctx context.Context, name string) (toolprovider.Tool, bool, error) {
	offered, err := s.offered(ctx)
	if err != nil {
		return toolprovider.Tool{}, false, err
	}
	for _, t := range offered {
		if t.Name == name {
			return t, true, nil
		}
	}
	return toolprovider.Tool{}, false, nil
}
