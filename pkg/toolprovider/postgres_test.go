package toolprovider_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/fiber/pkg/toolprovider"
)

// postgresEnv names the Postgres the Store suite runs against. Without it the
// suite runs against Memory; with it, Postgres is held to the same contract.
const postgresEnv = "FIBER_POSTGRES_TEST_URL"

var schemas atomic.Uint64

// newPostgres returns a migrated store in a schema of its own, dropped when
// the test ends.
func newPostgres(t *testing.T, url string, opts ...toolprovider.Option) *toolprovider.Postgres {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("fibertest_toolprovider_%d_%d", time.Now().UnixNano(), schemas.Add(1))

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse $%s: %v", postgresEnv, err)
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		pool.Close()
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+ident+" CASCADE")
		pool.Close()
	})

	store := toolprovider.NewPostgres(pool, opts...)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pools.Store(store, pool)
	return store
}

var pools sync.Map

func poolOf(t *testing.T, store *toolprovider.Postgres) *pgxpool.Pool {
	t.Helper()
	pool, ok := pools.Load(store)
	if !ok {
		t.Fatal("store was not built by newPostgres")
	}
	return pool.(*pgxpool.Pool)
}

func postgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("set %s to run the Postgres toolprovider tests", postgresEnv)
	}
	return url
}

// Migrate runs from every replica at startup, so running it again, and from
// several places at once, is a no-op rather than an error.
func TestPostgresMigrateIsIdempotentAndConcurrent(t *testing.T) {
	store := newPostgres(t, postgresURL(t))
	errs := make(chan error, 8)
	for range cap(errs) {
		go func() { errs <- store.Migrate(context.Background()) }()
	}
	for range cap(errs) {
		if err := <-errs; err != nil {
			t.Fatalf("Migrate: %v", err)
		}
	}
}

// A second store on the same database, which is what a restarted process is,
// reads the providers and snapshots the first wrote, and an accepted schema
// reads back byte for byte: it is shown to the model, and a moved byte is a
// prompt cache miss.
func TestPostgresSnapshotsSurviveANewStoreByteForByte(t *testing.T) {
	ctx := context.Background()
	first := newPostgres(t, postgresURL(t))
	p := mustPut(t, first, github(tenantA))
	search := tool(t, "search", "Search <issues> & PRs", `{"type":"object","properties":{"z":{"type":"string"},"a":{"type":"integer","minimum":1e2}}}`)
	search.Class = toolprovider.ClassRead
	wrote, err := first.Accept(ctx, tenantA, "github", 0, []toolprovider.Tool{search}, "admin")
	if err != nil {
		t.Fatal(err)
	}

	second := toolprovider.NewPostgres(poolOf(t, first))
	got, err := second.Snapshot(ctx, tenantA, "github", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, wrote) {
		t.Fatalf("snapshot changed across stores:\n got %+v\nwant %+v", got, wrote)
	}
	if string(got.Tools[0].InputSchema) != string(wrote.Tools[0].InputSchema) {
		t.Fatalf("schema bytes moved: %s vs %s", got.Tools[0].InputSchema, wrote.Tools[0].InputSchema)
	}
	if !json.Valid(got.Tools[0].InputSchema) {
		t.Fatalf("schema read back invalid: %s", got.Tools[0].InputSchema)
	}
	rec, err := second.GetProvider(ctx, tenantA, "github")
	if err != nil || !reflect.DeepEqual(rec, p) {
		t.Fatalf("provider across stores = %+v, %v; want %+v", rec, err, p)
	}
}

// Two admins accepting the same base at once: exactly one wins, the other is
// told it conflicted, and no snapshot number is written twice.
func TestPostgresConcurrentAcceptsSerializeOnTheProvider(t *testing.T) {
	ctx := context.Background()
	store := newPostgres(t, postgresURL(t))
	mustPut(t, store, github(tenantA))
	const n = 8
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.Accept(ctx, tenantA, "github", 0, []toolprovider.Tool{tool(t, fmt.Sprintf("t%d", i), "", "")}, "admin"); err == nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d concurrent accepts on base 0 succeeded, want 1", wins.Load())
	}
}

// A database whose providers table predates credential scopes gains the
// column on Migrate, and every provider already in it is the shared provider
// it always was, its shared credential intact.
func TestPostgresAProviderFromBeforeCredentialScopesIsShared(t *testing.T) {
	ctx := context.Background()
	store := newPostgres(t, postgresURL(t))
	pool := poolOf(t, store)
	if _, err := pool.Exec(ctx, `DROP TABLE fiber_tool_snapshots; DROP TABLE fiber_tool_providers;
		CREATE TABLE fiber_tool_providers (
			tenant text NOT NULL, name text NOT NULL,
			transport text NOT NULL CHECK (transport IN ('stdio', 'http')),
			endpoint text NOT NULL, args jsonb NOT NULL DEFAULT '[]'::jsonb,
			credential_ref text NOT NULL, credential_env text NOT NULL,
			version bigint NOT NULL CHECK (version >= 1),
			updated_by text NOT NULL, updated_at timestamptz NOT NULL,
			PRIMARY KEY (tenant, name));
		INSERT INTO fiber_tool_providers VALUES
			('acme', 'github', 'http', 'https://mcp.example.com/github', '[]', 'env:GITHUB_TOKEN', '', 1, 'admin', now());`); err != nil {
		t.Fatalf("build the old table: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	got, err := store.GetProvider(ctx, tenantA, "github")
	if err != nil {
		t.Fatal(err)
	}
	if got.CredentialScope != toolprovider.ScopeShared || got.CredentialRef != "env:GITHUB_TOKEN" || got.Version != 1 {
		t.Fatalf("an old provider read back as %+v, want the shared provider it was", got)
	}
	if _, err := pool.Exec(ctx, `UPDATE fiber_tool_providers SET credential_scope = 'team'`); err == nil {
		t.Fatal("the column took a scope the store does not know")
	}
}
