package thread_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/mandatum/pkg/thread"
)

// postgresEnv names the Postgres the Store suite runs against. Without it the
// suite runs against Memory, so it stays runnable with no infrastructure; with
// it, the Postgres backend is held to exactly the same contract.
const postgresEnv = "MANDATUM_POSTGRES_TEST_URL"

// schemas numbers each store the suite builds, so tests sharing one database
// cannot see each other's threads.
var schemas atomic.Uint64

// newPostgres returns a migrated store in a schema of its own, dropped when the
// test ends. The pool is capped well under Postgres's default connection limit
// because the package's tests run in parallel with every other package's.
func newPostgres(t *testing.T, url string, opts ...thread.Option) *thread.Postgres {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("mandatumtest_thread_%d_%d", time.Now().UnixNano(), schemas.Add(1))

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

	store := thread.NewPostgres(pool, opts...)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pools.Store(store, pool)
	return store
}

// pools remembers the pool behind each store the suite built, for the tests
// that need to reach around the Store interface.
var pools sync.Map

func poolOf(t *testing.T, store *thread.Postgres) *pgxpool.Pool {
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
// is what a restarted process is, reads the conversation the first one wrote
// and continues its numbering.
func TestPostgresConversationsSurviveANewStore(t *testing.T) {
	url := postgresURL(t)
	first := newPostgres(t, url)
	key := slackThread("docs-bot")
	wrote := mustAppend(t, first, key, user("run-1", "question"), assistant("run-1", "answer"))

	second := thread.NewPostgres(poolOf(t, first))
	got, err := second.History(context.Background(), key, 2)
	if err != nil {
		t.Fatalf("History from a new store: %v", err)
	}
	if !reflect.DeepEqual(got, wrote) {
		t.Fatalf("the conversation changed across stores:\n got %+v\nwant %+v", got, wrote)
	}
	// A retry from the new process is still recognized, and a new run
	// continues from where the old process stopped.
	mustAppend(t, second, key, user("run-1", "question"), assistant("run-1", "answer"))
	next := mustAppend(t, second, key, user("run-2", "next"))
	if next[0].Seq != 3 {
		t.Fatalf("a new store continued at seq %d, want 3", next[0].Seq)
	}
}

// The journal is append-only by construction, and the constraints are the
// backstop for a writer that bypasses the store: a second entry for one run
// and role, or for one surface message, cannot be inserted behind its back.
func TestPostgresConstraintsRefuseADuplicateWrittenBehindTheStore(t *testing.T) {
	url := postgresURL(t)
	store := newPostgres(t, url)
	key := slackThread("docs-bot")
	q := user("run-1", "question")
	q.SurfaceID = "1699.002"
	mustAppend(t, store, key, q)

	insert := func(seq int, run, surfaceID string) error {
		_, err := poolOf(t, store).Exec(context.Background(),
			`INSERT INTO mandatum_thread_entries
			   (tenant, agent, surface, seq, role, body, actor_id, actor_display, actor_bot, run_id, surface_id, at)
			 VALUES ($1, $2, $3, $4, 'user', 'forged', '', '', false, $5, $6, now())`,
			key.Tenant, key.Agent, key.Surface, seq, run, surfaceID)
		return err
	}
	if err := insert(1, "run-x", ""); err == nil {
		t.Error("a second entry at an existing sequence was accepted")
	}
	if err := insert(2, "run-1", ""); err == nil {
		t.Error("a second user entry for one run was accepted")
	}
	if err := insert(2, "", "1699.002"); err == nil {
		t.Error("a second entry for one surface message was accepted")
	}
}

func postgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("set %s to run the Postgres thread tests", postgresEnv)
	}
	return url
}
