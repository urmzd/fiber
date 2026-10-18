package registry_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/fiber/pkg/registry"
)

// postgresEnv names the Postgres the Store suite runs against. Without it the
// suite runs against Memory, so it stays runnable with no infrastructure; with
// it, the Postgres backend is held to exactly the same contract.
const postgresEnv = "FIBER_POSTGRES_TEST_URL"

// schemas numbers each store the suite builds, so tests sharing one database
// cannot see each other's agents.
var schemas atomic.Uint64

// newPostgres returns a migrated store in a schema of its own, dropped when the
// test ends. The pool is capped well under Postgres's default connection limit
// because the package's tests run in parallel with every other package's.
func newPostgres(t *testing.T, url string, opts ...registry.Option) *registry.Postgres {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("fibertest_%d_%d", time.Now().UnixNano(), schemas.Add(1))

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

	store := registry.NewPostgres(pool, opts...)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pools.Store(store, pool)
	return store
}

// pools remembers the pool behind each store the suite built, for the tests
// that need to reach around the Store interface.
var pools sync.Map

func poolOf(t *testing.T, store *registry.Postgres) *pgxpool.Pool {
	t.Helper()
	pool, ok := pools.Load(store)
	if !ok {
		t.Fatal("store was not built by newPostgres")
	}
	return pool.(*pgxpool.Pool)
}

// Migrate runs from every replica at startup, so running it again, and running
// it from several places at once, has to be a no-op rather than an error.
func TestPostgresMigrateIsIdempotentAndConcurrent(t *testing.T) {
	url := postgresURL(t)
	store := newPostgres(t, url)

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

// The reason this backend exists: a second store on the same database, which
// is what a restarted process is, reads what the first one wrote.
func TestPostgresDefinitionsSurviveANewStore(t *testing.T) {
	url := postgresURL(t)
	first := newPostgres(t, url)
	created := mustCreate(t, first, tenantA, "docs-bot", "v1")
	mustRevise(t, first, tenantA, "docs-bot", "v2", 1)

	second := registry.NewPostgres(poolOf(t, first))
	got, err := second.Get(context.Background(), tenantA, "docs-bot", 1)
	if err != nil {
		t.Fatalf("Get(rev 1) from a new store: %v", err)
	}
	if got.Hash != created.Hash || !got.CreatedAt.Equal(created.CreatedAt) {
		t.Errorf("revision 1 changed across stores: got %+v, want %+v", got, created)
	}
	latest, err := second.Get(context.Background(), tenantA, "docs-bot", 0)
	if err != nil || latest.Rev != 2 {
		t.Errorf("Get(latest) from a new store = rev %d, %v; want rev 2", latest.Rev, err)
	}
}

// A revision is the bytes a run replays against. A row edited behind the
// store's back must fail to read rather than be served as that revision.
func TestPostgresRefusesARevisionThatNoLongerMatchesItsHash(t *testing.T) {
	url := postgresURL(t)
	store := newPostgres(t, url)
	mustCreate(t, store, tenantA, "docs-bot", "v1")

	if _, err := poolOf(t, store).Exec(context.Background(),
		`UPDATE mandatum_agent_revisions SET spec = jsonb_set(spec, '{system_prompt}', '"tampered"')`); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	if _, err := store.Get(context.Background(), tenantA, "docs-bot", 1); err == nil {
		t.Fatal("Get served a revision whose content no longer hashes to its recorded hash")
	}
}

func postgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("set %s to run the Postgres registry tests", postgresEnv)
	}
	return url
}
