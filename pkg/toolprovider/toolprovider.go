// Package toolprovider is the control plane for external tool servers: the
// record of which MCP servers a tenant may reach, and the versioned list of
// tools an admin has accepted from each.
//
// It answers three questions and refuses to answer anything else: how do I
// reach provider X, which of its tools has an admin accepted (now, or as of
// snapshot N), and how does what the server offers today differ from that.
// Talking to a server belongs to pkg/mcptools; resolving the secret a provider
// names belongs to whoever deploys it; deciding which agent may use which
// provider belongs to the agent's grants. This package holds records and
// compares them.
//
// # A server's tool list is input, not configuration
//
// An MCP server describes its own tools and may change that description at
// any time: a new tool, a reworded description, a schema that takes one more
// argument. Each of those changes what a model is told it can do, so none of
// them takes effect on its own. An admin accepts a tool list explicitly, and
// what was accepted is a Snapshot: immutable, numbered, and kept. A tool the
// server offers that is not in the current snapshot, or that differs from its
// accepted form in name, description or schema hash, is pending, and pending
// tools are never callable. Diff computes the three-way comparison an admin
// reviews; Accept records the decision.
//
// Snapshots are numbered, like definition versions and for the same reason: a
// run can pin "provider X as of snapshot 4" and every attempt then sees the
// same tool definitions, byte for byte, which keeps both the replay and the
// prompt cache stable. Accept is an optimistic concurrency check on that
// number, so two admins reviewing the same diff cannot silently overwrite
// each other.
//
// # Read and write
//
// Every accepted tool has a Class, read or write, and the default is write: a
// tool is read only when an admin declared it read. The server's own
// readOnlyHint is not trusted, because it is the server describing itself.
// A Grant says which classes a caller may invoke, and its zero value is read
// only, so both defaults fail closed. Class is enforced at call time by
// pkg/mcptools, not only when a tool list is built.
//
// # Credentials
//
// A Provider carries a CredentialRef, a reference such as "env:GITHUB_TOKEN"
// or "op://vault/item/field", and never the secret. The reference is resolved
// when a connection is opened, so a stored record, a log line or an error can
// name which credential was used without being able to reveal it. Endpoints
// with credentials embedded in them (https://user:pass@host) are refused for
// the same reason.
//
// Whose credential that is depends on the provider's CredentialScope. A
// shared provider (the default) is reached with its one CredentialRef, for
// every run. A person-scoped provider is reached with the credential of the
// person a run acts for, which pkg/mcptools resolves for that person when it
// opens their session. Such a provider has no CredentialRef at all: a shared
// credential is never a fallback for a person who has none, and a record that
// names one is refused rather than kept unused.
//
// # Tenancy
//
// The tenant is an explicit argument on every method, as in pkg/definition. Two
// tenants may each have a provider called github; they share a name and
// nothing else.
//
// # Backends
//
// Memory is the reference implementation and a complete system. Postgres keeps
// the same contract and is held to it by the same suite when
// $FIBER_POSTGRES_TEST_URL is set. There is no delete: removing every tool
// from a provider is an Accept of an empty list, which keeps snapshot numbers
// from ever being reused.
package toolprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Sentinels every implementation reports, matched with errors.Is.
var (
	// ErrNotFound reports a provider, or a snapshot of one, this tenant does
	// not have.
	ErrNotFound = errors.New("toolprovider: not found")

	// ErrExists reports a create against a name that already exists.
	ErrExists = errors.New("toolprovider: already exists")

	// ErrConflict reports a failed optimistic concurrency check: the caller
	// edited a version that is no longer current.
	ErrConflict = errors.New("toolprovider: conflict")

	// ErrInvalid reports a record or an argument the store will not accept.
	ErrInvalid = errors.New("toolprovider: invalid")
)

// Transport is how a provider is reached.
type Transport string

const (
	// TransportStdio runs Endpoint as a local command and speaks MCP over its
	// stdin and stdout.
	TransportStdio Transport = "stdio"
	// TransportHTTP speaks MCP's streamable HTTP transport to Endpoint.
	TransportHTTP Transport = "http"
)

// CredentialScope is whose credential a provider is reached with.
type CredentialScope string

const (
	// ScopeShared reaches the provider with its own CredentialRef, the same
	// for every run. It is the default: the empty scope is shared.
	ScopeShared CredentialScope = "shared"
	// ScopePerson reaches the provider with the credential of the person a
	// run acts for, and with nothing when that person has none.
	ScopePerson CredentialScope = "person"
)

// Provider is how to reach one MCP server. It is versioned: PutProvider
// takes the version the caller read and returns the next one.
type Provider struct {
	Tenant    string    `json:"tenant"`
	Name      string    `json:"name"`
	Transport Transport `json:"transport"`
	// Endpoint is a URL for TransportHTTP and a command for TransportStdio.
	Endpoint string `json:"endpoint"`
	// Args are the command's arguments, for TransportStdio only.
	Args []string `json:"args,omitempty"`
	// CredentialScope is whose credential the provider is reached with:
	// ScopeShared (the default, and what empty means) or ScopePerson. A
	// store records the empty scope as ScopeShared.
	CredentialScope CredentialScope `json:"credential_scope,omitempty"`
	// CredentialRef names the shared credential to resolve at connect time.
	// It is a reference, never the secret, and may be empty. A
	// person-scoped provider has none.
	CredentialRef string `json:"credential_ref,omitempty"`
	// CredentialEnv is the environment variable a stdio server receives the
	// resolved credential in. Required for stdio when CredentialRef is set,
	// and for every person-scoped stdio provider; HTTP providers receive it
	// as a bearer token.
	CredentialEnv string    `json:"credential_env,omitempty"`
	Version       uint64    `json:"version"`
	UpdatedBy     string    `json:"updated_by,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

var (
	providerName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	toolName     = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	envName      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// MaxQualifiedName is the longest tool name a model API accepts, and so the
// longest QualifiedName may produce.
const MaxQualifiedName = 64

// QualifiedName is the name a model sees for a provider's tool. The separator
// is two underscores, which a provider name cannot contain, so the split is
// unambiguous.
func QualifiedName(provider, tool string) string { return provider + "__" + tool }

// Scope returns the provider's effective credential scope: ScopeShared
// unless it is ScopePerson. Validate refuses any other value, so an unknown
// scope never reaches a connection.
func (p Provider) Scope() CredentialScope {
	if p.CredentialScope == ScopePerson {
		return ScopePerson
	}
	return ScopeShared
}

// ValidName reports whether name is one a provider may have: lowercase
// letters, digits and hyphens, starting with a letter, at most 32 characters.
func ValidName(name string) bool { return providerName.MatchString(name) }

// Validate reports whether p is a record a store will accept.
func (p Provider) Validate() error {
	if p.Tenant == "" {
		return fmt.Errorf("%w: provider has no tenant", ErrInvalid)
	}
	if !providerName.MatchString(p.Name) {
		return fmt.Errorf("%w: provider name %q must match %s", ErrInvalid, p.Name, providerName)
	}
	switch p.CredentialScope {
	case "", ScopeShared:
	case ScopePerson:
		if p.CredentialRef != "" {
			// Kept unused, it would be one edit away from a fallback.
			return fmt.Errorf("%w: provider %q is person-scoped and takes each person's own credential; it cannot name a shared CredentialRef", ErrInvalid, p.Name)
		}
		if p.Transport == TransportStdio && p.CredentialEnv == "" {
			return fmt.Errorf("%w: provider %q: a person-scoped stdio provider needs CredentialEnv for each person's credential", ErrInvalid, p.Name)
		}
	default:
		return fmt.Errorf("%w: provider %q: credential scope %q, want shared or person", ErrInvalid, p.Name, p.CredentialScope)
	}
	switch p.Transport {
	case TransportHTTP:
		u, err := url.Parse(p.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("%w: provider %q: endpoint must be an http or https URL", ErrInvalid, p.Name)
		}
		if u.User != nil {
			// Not echoed: the point is that it may hold a secret.
			return fmt.Errorf("%w: provider %q: endpoint carries credentials; use CredentialRef", ErrInvalid, p.Name)
		}
		if len(p.Args) > 0 {
			return fmt.Errorf("%w: provider %q: args apply to stdio only", ErrInvalid, p.Name)
		}
	case TransportStdio:
		if strings.TrimSpace(p.Endpoint) == "" {
			return fmt.Errorf("%w: provider %q: stdio needs a command", ErrInvalid, p.Name)
		}
		if p.CredentialRef != "" && p.CredentialEnv == "" {
			return fmt.Errorf("%w: provider %q: stdio with a credential needs CredentialEnv", ErrInvalid, p.Name)
		}
	default:
		return fmt.Errorf("%w: provider %q: transport %q, want stdio or http", ErrInvalid, p.Name, p.Transport)
	}
	if p.CredentialEnv != "" && !envName.MatchString(p.CredentialEnv) {
		return fmt.Errorf("%w: provider %q: credential env %q is not a variable name", ErrInvalid, p.Name, p.CredentialEnv)
	}
	return nil
}

// stored is p as a store records it: the scope spelled out, so a record
// written without one reads back as the shared provider it is.
func (p Provider) stored() Provider {
	p = p.clone()
	p.CredentialScope = p.Scope()
	return p
}

func (p Provider) clone() Provider {
	if len(p.Args) == 0 {
		p.Args = nil
	} else {
		p.Args = slices.Clone(p.Args)
	}
	return p
}

// Class is what a tool can do. Anything that is not ClassRead is a write.
type Class string

const (
	// ClassRead reads state and changes nothing.
	ClassRead Class = "read"
	// ClassWrite may change state. It is the default.
	ClassWrite Class = "write"
)

// Effective maps every value but ClassRead to ClassWrite, so an undeclared
// or misspelled class is treated as the more dangerous one.
func (c Class) Effective() Class {
	if c == ClassRead {
		return ClassRead
	}
	return ClassWrite
}

// Grant is the class of tool a caller may invoke. Its zero value is read only.
type Grant string

const (
	// GrantRead admits read tools only.
	GrantRead Grant = "read"
	// GrantWrite admits read and write tools.
	GrantWrite Grant = "write"
)

// Permits reports whether g admits a call to a tool of class c. Anything but
// GrantWrite is read only, so an unset grant cannot reach a write tool.
func (g Grant) Permits(c Class) bool {
	return g == GrantWrite || c.Effective() == ClassRead
}

// Tool is one tool as a server offers it or as an admin accepted it.
//
// Name, Description and SchemaHash are the tool's identity for review: if
// any of them differs from the accepted form, the tool is pending again.
// InputSchema is kept byte for byte as accepted, because it is shown to the
// model and a byte that moves invalidates the prompt cache. Class is the
// admin's declaration and is empty on an offered tool.
type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
	SchemaHash  string          `json:"schema_hash"`
	Class       Class           `json:"class,omitempty"`
}

// NewTool builds a Tool from a server's description, canonicalizing the
// schema and computing its hash. schema may be empty, which means "no
// arguments".
func NewTool(name, description string, schema json.RawMessage) (Tool, error) {
	canon, sum, err := canonicalSchema(schema)
	if err != nil {
		return Tool{}, fmt.Errorf("%w: tool %q: %w", ErrInvalid, name, err)
	}
	return Tool{Name: name, Description: description, InputSchema: canon, SchemaHash: sum}, nil
}

// canonicalSchema re-encodes a JSON schema with sorted keys and no
// insignificant whitespace, so two servers that serialize the same schema
// differently hash the same.
func canonicalSchema(raw json.RawMessage) (json.RawMessage, string, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, "", fmt.Errorf("schema is not JSON: %w", err)
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, "", errors.New("schema is not a JSON object")
	}
	canon, err := json.Marshal(v)
	if err != nil {
		return nil, "", fmt.Errorf("schema: %w", err)
	}
	sum := sha256.Sum256(canon)
	return canon, hex.EncodeToString(sum[:]), nil
}

// same reports whether two tools are the same for review purposes.
func (t Tool) same(o Tool) bool {
	return t.Name == o.Name && t.Description == o.Description && t.SchemaHash == o.SchemaHash
}

func (t Tool) clone() Tool {
	t.InputSchema = bytes.Clone(t.InputSchema)
	return t
}

// Snapshot is an accepted tool list. Version 0 is the snapshot every provider
// has before anything was accepted: empty, so everything is pending.
type Snapshot struct {
	Tenant     string    `json:"tenant"`
	Provider   string    `json:"provider"`
	Version    uint64    `json:"version"`
	Tools      []Tool    `json:"tools"`
	AcceptedBy string    `json:"accepted_by,omitempty"`
	AcceptedAt time.Time `json:"accepted_at"`
}

// Tool returns the accepted tool called name.
func (s Snapshot) Tool(name string) (Tool, bool) {
	for _, t := range s.Tools {
		if t.Name == name {
			return t, true
		}
	}
	return Tool{}, false
}

func (s Snapshot) clone() Snapshot {
	tools := make([]Tool, len(s.Tools))
	for i, t := range s.Tools {
		tools[i] = t.clone()
	}
	s.Tools = tools
	return s
}

// Change is a tool the server still offers in a different form than the one
// accepted.
type Change struct {
	Accepted Tool `json:"accepted"`
	Offered  Tool `json:"offered"`
}

// Diff compares what a server offers now with an accepted snapshot.
type Diff struct {
	// Unchanged are accepted tools the server offers identically. Each is
	// the accepted form, Class included, and only these are callable.
	Unchanged []Tool `json:"unchanged"`
	// Added are offered tools that were never accepted. Pending.
	Added []Tool `json:"added"`
	// Changed are accepted tools the server now describes differently.
	// Pending, and held back until accepted again.
	Changed []Change `json:"changed"`
	// Removed are accepted tools the server no longer offers.
	Removed []Tool `json:"removed"`
}

// Pending reports the offered tools that need a decision: the added ones and
// the new form of the changed ones.
func (d Diff) Pending() []Tool {
	out := slices.Clone(d.Added)
	for _, c := range d.Changed {
		out = append(out, c.Offered)
	}
	return out
}

// Compare computes the Diff between an accepted snapshot and an offered
// tool list. Every slice is sorted by name.
func Compare(accepted Snapshot, offered []Tool) Diff {
	d := Diff{Unchanged: []Tool{}, Added: []Tool{}, Changed: []Change{}, Removed: []Tool{}}
	seen := make(map[string]bool, len(offered))
	for _, o := range offered {
		seen[o.Name] = true
		a, ok := accepted.Tool(o.Name)
		switch {
		case !ok:
			d.Added = append(d.Added, o.clone())
		case a.same(o):
			d.Unchanged = append(d.Unchanged, a.clone())
		default:
			d.Changed = append(d.Changed, Change{Accepted: a.clone(), Offered: o.clone()})
		}
	}
	for _, a := range accepted.Tools {
		if !seen[a.Name] {
			d.Removed = append(d.Removed, a.clone())
		}
	}
	byName := func(a, b Tool) int { return strings.Compare(a.Name, b.Name) }
	slices.SortFunc(d.Unchanged, byName)
	slices.SortFunc(d.Added, byName)
	slices.SortFunc(d.Removed, byName)
	slices.SortFunc(d.Changed, func(a, b Change) int { return strings.Compare(a.Offered.Name, b.Offered.Name) })
	return d
}

// prepareTools validates and normalizes a tool list for Accept: names valid
// and unique, schema canonical and its hash recomputed rather than trusted,
// class declared or defaulted to write.
func prepareTools(provider string, tools []Tool) ([]Tool, error) {
	out := make([]Tool, 0, len(tools))
	seen := make(map[string]bool, len(tools))
	for _, t := range tools {
		if !toolName.MatchString(t.Name) {
			return nil, fmt.Errorf("%w: tool name %q must match %s", ErrInvalid, t.Name, toolName)
		}
		if q := QualifiedName(provider, t.Name); len(q) > MaxQualifiedName {
			return nil, fmt.Errorf("%w: tool %q: qualified name %q is longer than %d", ErrInvalid, t.Name, q, MaxQualifiedName)
		}
		if seen[t.Name] {
			return nil, fmt.Errorf("%w: tool %q accepted twice", ErrInvalid, t.Name)
		}
		seen[t.Name] = true
		switch t.Class {
		case "":
			t.Class = ClassWrite
		case ClassRead, ClassWrite:
		default:
			return nil, fmt.Errorf("%w: tool %q: class %q, want read or write", ErrInvalid, t.Name, t.Class)
		}
		canon, sum, err := canonicalSchema(t.InputSchema)
		if err != nil {
			return nil, fmt.Errorf("%w: tool %q: %w", ErrInvalid, t.Name, err)
		}
		t.InputSchema, t.SchemaHash = canon, sum
		out = append(out, t)
	}
	slices.SortFunc(out, func(a, b Tool) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// Store holds provider records and their accepted snapshots.
type Store interface {
	// PutProvider creates or updates a provider. p.Version is the version
	// the caller read: zero creates (ErrExists if the name is taken), any
	// other value updates (ErrNotFound, or ErrConflict if it is stale). It
	// returns the stored record with its new version.
	PutProvider(ctx context.Context, p Provider) (Provider, error)

	// GetProvider returns a provider's current record.
	GetProvider(ctx context.Context, tenant, name string) (Provider, error)

	// ListProviders returns a tenant's providers sorted by name.
	ListProviders(ctx context.Context, tenant string) ([]Provider, error)

	// Accept records tools as the provider's accepted list, replacing the
	// previous one, and returns the new snapshot. base is the snapshot
	// version the admin reviewed; ErrConflict if it is not the current one.
	// Each tool's Class defaults to write and its SchemaHash is recomputed.
	Accept(ctx context.Context, tenant, provider string, base uint64, tools []Tool, by string) (Snapshot, error)

	// Snapshot returns the accepted snapshot at version, or the current one
	// when version is zero. A provider with nothing accepted yet has the
	// empty snapshot 0. A version that was never written is ErrNotFound.
	Snapshot(ctx context.Context, tenant, provider string, version uint64) (Snapshot, error)
}

// Option configures a store.
type Option func(*settings)

type settings struct{ now func() time.Time }

// WithClock sets the clock that stamps UpdatedAt and AcceptedAt.
func WithClock(now func() time.Time) Option { return func(s *settings) { s.now = now } }

func newSettings(opts []Option) settings {
	s := settings{now: time.Now}
	for _, o := range opts {
		o(&s)
	}
	return s
}

func (s settings) stamp() time.Time { return s.now().UTC().Truncate(time.Microsecond) }

func checkName(ctx context.Context, tenant, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tenant == "" {
		return fmt.Errorf("%w: no tenant", ErrInvalid)
	}
	if !providerName.MatchString(name) {
		return fmt.Errorf("%w: provider name %q must match %s", ErrInvalid, name, providerName)
	}
	return nil
}
