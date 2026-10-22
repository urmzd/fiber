// Package toolprovidertest is the conformance suite every toolprovider.Store
// must pass: versioned provider records with stale edits refused, tenant
// isolation, validation, a credential scope that reads back as written (and a
// person-scoped provider that can name no shared credential), nothing
// accepted until an admin accepts it, and numbered, immutable, pinnable
// snapshots whose schema hashes the store recomputes. Memory and Postgres
// both run it; a new backend is a Factory.
package toolprovidertest

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/toolprovider"
)

const (
	tenantA = "acme"
	tenantB = "globex"
)

func github(tenant string) toolprovider.Provider {
	return toolprovider.Provider{
		Tenant: tenant, Name: "github", Transport: toolprovider.TransportHTTP,
		Endpoint: "https://mcp.example.com/github", CredentialRef: "env:GITHUB_TOKEN", UpdatedBy: "admin",
	}
}

func tool(t *testing.T, name, desc, schema string) toolprovider.Tool {
	t.Helper()
	out, err := toolprovider.NewTool(name, desc, json.RawMessage(schema))
	if err != nil {
		t.Fatalf("NewTool(%q): %v", name, err)
	}
	return out
}

func mustPut(t *testing.T, s toolprovider.Store, p toolprovider.Provider) toolprovider.Provider {
	t.Helper()
	got, err := s.PutProvider(context.Background(), p)
	if err != nil {
		t.Fatalf("PutProvider(%s): %v", p.Name, err)
	}
	return got
}

// Factory returns a fresh, empty store built with opts. It must isolate each
// call. The suite passes a clock option so stamps are deterministic.
type Factory func(t *testing.T, opts ...toolprovider.Option) toolprovider.Store

// Run exercises a Store implementation against the full contract.
func Run(t *testing.T, factory Factory) {
	newStore := func(t *testing.T) toolprovider.Store {
		t.Helper()
		var mu sync.Mutex
		at := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
		return factory(t, toolprovider.WithClock(func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			at = at.Add(time.Second)
			return at
		}))
	}

	t.Run("AProviderRecordIsVersionedAndAStaleEditIsRefused", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)

		v1 := mustPut(t, s, github(tenantA))
		if v1.Version != 1 {
			t.Fatalf("created at version %d, want 1", v1.Version)
		}
		if _, err := s.PutProvider(ctx, github(tenantA)); !errors.Is(err, toolprovider.ErrExists) {
			t.Fatalf("second create: %v, want ErrExists", err)
		}

		edit := v1
		edit.Endpoint = "https://mcp.example.com/github/v2"
		v2 := mustPut(t, s, edit)
		if v2.Version != 2 || v2.Endpoint != edit.Endpoint {
			t.Fatalf("update = %+v", v2)
		}
		if _, err := s.PutProvider(ctx, edit); !errors.Is(err, toolprovider.ErrConflict) {
			t.Fatalf("edit of stale version 1: %v, want ErrConflict", err)
		}
		missing := github(tenantA)
		missing.Name, missing.Version = "jira", 1
		if _, err := s.PutProvider(ctx, missing); !errors.Is(err, toolprovider.ErrNotFound) {
			t.Fatalf("update of a missing provider: %v, want ErrNotFound", err)
		}

		got, err := s.GetProvider(ctx, tenantA, "github")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, v2) {
			t.Fatalf("GetProvider = %+v, want %+v", got, v2)
		}
	})

	t.Run("ATenantSeesOnlyItsOwnProviders", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)
		mustPut(t, s, github(tenantA))
		stdio := toolprovider.Provider{
			Tenant: tenantA, Name: "files", Transport: toolprovider.TransportStdio,
			Endpoint: "mcp-files", Args: []string{"--root", "/srv"},
		}
		mustPut(t, s, stdio)

		if _, err := s.GetProvider(ctx, tenantB, "github"); !errors.Is(err, toolprovider.ErrNotFound) {
			t.Fatalf("another tenant's provider: %v, want ErrNotFound", err)
		}
		if _, err := s.Snapshot(ctx, tenantB, "github", 0); !errors.Is(err, toolprovider.ErrNotFound) {
			t.Fatalf("another tenant's snapshot: %v, want ErrNotFound", err)
		}
		if _, err := s.Accept(ctx, tenantB, "github", 0, nil, "mallory"); !errors.Is(err, toolprovider.ErrNotFound) {
			t.Fatalf("accept on another tenant's provider: %v, want ErrNotFound", err)
		}
		list, err := s.ListProviders(ctx, tenantA)
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 2 || list[0].Name != "files" || list[1].Name != "github" {
			t.Fatalf("ListProviders = %+v", list)
		}
		if !reflect.DeepEqual(list[0].Args, stdio.Args) {
			t.Fatalf("args read back as %#v", list[0].Args)
		}
		other, err := s.ListProviders(ctx, tenantB)
		if err != nil || len(other) != 0 {
			t.Fatalf("ListProviders(other tenant) = %+v, %v", other, err)
		}
	})

	t.Run("InvalidProvidersAreRefused", func(t *testing.T) {
		cases := map[string]func(*toolprovider.Provider){
			"no tenant":                    func(p *toolprovider.Provider) { p.Tenant = "" },
			"name with underscores":        func(p *toolprovider.Provider) { p.Name = "git_hub" },
			"unknown transport":            func(p *toolprovider.Provider) { p.Transport = "sse" },
			"endpoint with userinfo":       func(p *toolprovider.Provider) { p.Endpoint = "https://u:hunter2@mcp.example.com" },
			"non-http endpoint":            func(p *toolprovider.Provider) { p.Endpoint = "ftp://mcp.example.com" },
			"args on http":                 func(p *toolprovider.Provider) { p.Args = []string{"x"} },
			"stdio credential without env": func(p *toolprovider.Provider) { p.Transport, p.Endpoint = toolprovider.TransportStdio, "mcp-github" },
			"unknown credential scope":     func(p *toolprovider.Provider) { p.CredentialScope = "team" },
			"person scope with a shared credential": func(p *toolprovider.Provider) {
				p.CredentialScope = toolprovider.ScopePerson
			},
			"person-scoped stdio without env": func(p *toolprovider.Provider) {
				p.CredentialScope, p.CredentialRef = toolprovider.ScopePerson, ""
				p.Transport, p.Endpoint = toolprovider.TransportStdio, "mcp-github"
			},
		}
		for name, mutate := range cases {
			t.Run(name, func(t *testing.T) {
				p := github(tenantA)
				mutate(&p)
				_, err := newStore(t).PutProvider(context.Background(), p)
				if !errors.Is(err, toolprovider.ErrInvalid) {
					t.Fatalf("PutProvider: %v, want ErrInvalid", err)
				}
				if name == "endpoint with userinfo" && err != nil && strings.Contains(err.Error(), "hunter2") {
					t.Fatalf("the refusal echoed the embedded secret: %v", err)
				}
			})
		}
	})

	t.Run("ACredentialScopeReadsBackAsWrittenAndDefaultsToShared", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)

		shared := mustPut(t, s, github(tenantA))
		if shared.CredentialScope != toolprovider.ScopeShared || shared.Scope() != toolprovider.ScopeShared {
			t.Fatalf("a provider written with no scope = %q, want it recorded as shared", shared.CredentialScope)
		}
		person := toolprovider.Provider{
			Tenant: tenantA, Name: "files", Transport: toolprovider.TransportStdio, Endpoint: "mcp-files",
			CredentialScope: toolprovider.ScopePerson, CredentialEnv: "FILES_TOKEN", UpdatedBy: "admin",
		}
		person = mustPut(t, s, person)
		if person.CredentialScope != toolprovider.ScopePerson {
			t.Fatalf("person-scoped provider stored as %q", person.CredentialScope)
		}
		got, err := s.GetProvider(ctx, tenantA, "files")
		if err != nil || !reflect.DeepEqual(got, person) {
			t.Fatalf("GetProvider = %+v, %v; want %+v", got, err, person)
		}

		// Moving a provider to per-person credentials is an edit like any
		// other, and it drops the shared credential with it: a record that
		// kept one would be one edit away from a fallback.
		moved := shared
		moved.CredentialScope = toolprovider.ScopePerson
		if _, err := s.PutProvider(ctx, moved); !errors.Is(err, toolprovider.ErrInvalid) {
			t.Fatalf("person scope keeping the shared credential: %v, want ErrInvalid", err)
		}
		moved.CredentialRef = ""
		moved = mustPut(t, s, moved)
		if moved.Version != 2 || moved.Scope() != toolprovider.ScopePerson || moved.CredentialRef != "" {
			t.Fatalf("moved to person scope = %+v", moved)
		}
		list, err := s.ListProviders(ctx, tenantA)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range list {
			if p.CredentialScope != toolprovider.ScopePerson {
				t.Errorf("ListProviders: %q has scope %q, want person", p.Name, p.CredentialScope)
			}
		}
	})

	t.Run("NothingIsAcceptedUntilAnAdminAcceptsIt", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)
		mustPut(t, s, github(tenantA))

		empty, err := s.Snapshot(ctx, tenantA, "github", 0)
		if err != nil {
			t.Fatal(err)
		}
		if empty.Version != 0 || len(empty.Tools) != 0 {
			t.Fatalf("snapshot before any accept = %+v, want the empty snapshot 0", empty)
		}
		offered := []toolprovider.Tool{tool(t, "search", "Search issues", `{"type":"object"}`)}
		if d := toolprovider.Compare(empty, offered); len(d.Added) != 1 || len(d.Unchanged) != 0 {
			t.Fatalf("diff against nothing accepted = %+v, want everything pending", d)
		}
	})

	t.Run("AcceptedSnapshotsAreNumberedImmutableAndPinnable", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)
		mustPut(t, s, github(tenantA))

		search := tool(t, "search", "Search issues", `{"type":"object","properties":{"q":{"type":"string"}}}`)
		search.Class = toolprovider.ClassRead
		create := tool(t, "create_issue", "Open an issue", `{"type":"object"}`)

		s1, err := s.Accept(ctx, tenantA, "github", 0, []toolprovider.Tool{search, create}, "admin")
		if err != nil {
			t.Fatalf("Accept: %v", err)
		}
		if s1.Version != 1 || s1.AcceptedBy != "admin" || s1.AcceptedAt.IsZero() {
			t.Fatalf("snapshot 1 = %+v", s1)
		}
		if got := s1.Tools[0]; got.Name != "create_issue" || got.Class != toolprovider.ClassWrite {
			t.Fatalf("an undeclared tool was accepted as %+v, want class write", got)
		}

		if _, err := s.Accept(ctx, tenantA, "github", 0, []toolprovider.Tool{search}, "other-admin"); !errors.Is(err, toolprovider.ErrConflict) {
			t.Fatalf("accept on a stale base: %v, want ErrConflict", err)
		}
		s2, err := s.Accept(ctx, tenantA, "github", 1, []toolprovider.Tool{search}, "admin")
		if err != nil || s2.Version != 2 {
			t.Fatalf("second accept = %+v, %v", s2, err)
		}

		pinned, err := s.Snapshot(ctx, tenantA, "github", 1)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(pinned, s1) {
			t.Fatalf("snapshot 1 changed after snapshot 2 was accepted:\n got %+v\nwant %+v", pinned, s1)
		}
		current, err := s.Snapshot(ctx, tenantA, "github", 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(current, s2) {
			t.Fatalf("current snapshot = %+v, want %+v", current, s2)
		}
		if _, err := s.Snapshot(ctx, tenantA, "github", 3); !errors.Is(err, toolprovider.ErrNotFound) {
			t.Fatalf("snapshot 3: %v, want ErrNotFound", err)
		}
	})

	t.Run("AcceptRecomputesTheSchemaHashAndRefusesBadLists", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)
		mustPut(t, s, github(tenantA))

		forged := tool(t, "search", "", `{"type":"object"}`)
		forged.SchemaHash = "0000"
		snap, err := s.Accept(ctx, tenantA, "github", 0, []toolprovider.Tool{forged}, "admin")
		if err != nil {
			t.Fatal(err)
		}
		if snap.Tools[0].SchemaHash != tool(t, "search", "", `{"type":"object"}`).SchemaHash {
			t.Fatalf("Accept kept a caller-supplied schema hash %q", snap.Tools[0].SchemaHash)
		}

		bad := map[string][]toolprovider.Tool{
			"duplicate name": {tool(t, "a", "", ""), tool(t, "a", "", "")},
			"invalid name":   {tool(t, "a.b", "", "")},
			"unknown class":  {{Name: "a", Class: "readonly"}},
			"too long":       {tool(t, strings.Repeat("x", 60), "", "")},
			"array schema":   {{Name: "a", InputSchema: json.RawMessage(`[]`)}},
		}
		for name, tools := range bad {
			if _, err := s.Accept(ctx, tenantA, "github", 1, tools, "admin"); !errors.Is(err, toolprovider.ErrInvalid) {
				t.Errorf("%s: %v, want ErrInvalid", name, err)
			}
		}
	})
}
