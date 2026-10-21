package durable_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/fiber/pkg/durable"
)

// postgresEnv names the Postgres these tests run against. Without it they
// skip: a schema has no in-memory twin to fall back on.
const postgresEnv = "FIBER_POSTGRES_TEST_URL"

// schemas numbers each schema the tests build, so tests sharing one database
// cannot see each other's tables.
var schemas atomic.Uint64

func postgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("set %s to run the durable schema tests", postgresEnv)
	}
	return url
}

// newPool returns a pool confined to a schema of its own, dropped when the test
// ends. It does not migrate: the tests below are about what Migrate does. The
// pool is capped well under Postgres's default connection limit because the
// package's tests run in parallel with every other package's.
func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := postgresURL(t)
	ctx := context.Background()
	schema := fmt.Sprintf("fibertest_durable_%d_%d", time.Now().UnixNano(), schemas.Add(1))

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
	return pool
}

func migrated(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := newPool(t)
	if err := durable.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return pool
}

// A missing pool is a configuration mistake, and it has to stop startup with
// something a caller can match on.
func TestMigrateRefusesANilPool(t *testing.T) {
	if err := durable.Migrate(context.Background(), nil); !errors.Is(err, durable.ErrNoPool) {
		t.Fatalf("Migrate(nil) = %v, want ErrNoPool", err)
	}
}

// Migrate runs from every replica at startup, so running it again, and running
// it from several places at once, has to be a no-op and never an error.
func TestMigrateIsIdempotentAndConcurrent(t *testing.T) {
	pool := newPool(t)

	errs := make(chan error, 8)
	for range cap(errs) {
		go func() { errs <- durable.Migrate(context.Background(), pool) }()
	}
	for range cap(errs) {
		if err := <-errs; err != nil {
			t.Fatalf("Migrate: %v", err)
		}
	}
	if err := durable.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("Migrate after Migrate: %v", err)
	}
}

// The exported names are what a caller puts in an adapter mapping. If one
// drifts from the DDL, the adapter is pointed at a table that does not exist.
func TestExportedTableNamesAreTheTablesMigrateCreates(t *testing.T) {
	pool := migrated(t)
	for _, table := range []string{durable.RunsTable, durable.RecordsTable, durable.QueueTable} {
		var found bool
		if err := pool.QueryRow(context.Background(),
			`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&found); err != nil {
			t.Fatalf("resolve %s: %v", table, err)
		}
		if !found {
			t.Errorf("Migrate did not create %s", table)
		}
	}
}

// The reason this package exists: rows written through one pool are there for
// the next one, which is what a restarted process is. Migrating again on the
// way up must leave them alone.
func TestMigrateLeavesExistingRunsInPlace(t *testing.T) {
	pool := migrated(t)
	ctx := context.Background()

	if _, err := pool.Exec(ctx,
		`INSERT INTO fiber_runs (run_id, status, envelope, created_at) VALUES ('run-1', 'pending', '{}', now())`); err != nil {
		t.Fatalf("insert run: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO fiber_run_queue (run_id, ready_at, attempt, failures) VALUES ('run-1', NULL, 3, 1)`); err != nil {
		t.Fatalf("insert queue row: %v", err)
	}
	if err := durable.Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate on restart: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM fiber_runs WHERE run_id = 'run-1'`).Scan(&status); err != nil || status != "pending" {
		t.Errorf("run after a second Migrate = %q, %v; want pending", status, err)
	}
	// The counters are the one part of the queue the ledger cannot rebuild.
	var attempt, failures int
	if err := pool.QueryRow(ctx,
		`SELECT attempt, failures FROM fiber_run_queue WHERE run_id = 'run-1'`).Scan(&attempt, &failures); err != nil {
		t.Fatalf("read queue row: %v", err)
	}
	if attempt != 3 || failures != 1 {
		t.Errorf("queue counters after a second Migrate = attempt %d, failures %d; want 3, 1", attempt, failures)
	}
}

// The constraints are the schema's half of anchor's invariants. Each case is
// a row the adapters never write, and one that would corrupt a run if anything
// else did.
func TestSchemaRefusesRowsThatBreakTheLedgerContract(t *testing.T) {
	const (
		unique  = "23505"
		check   = "23514"
		foreign = "23503"
	)
	cases := []struct {
		name string
		sql  string
		code string
	}{
		{
			name: "a second run under an accepted id",
			sql:  `INSERT INTO fiber_runs (run_id, status, envelope, created_at) VALUES ('run-1', 'pending', '{}', now())`,
			code: unique,
		},
		{
			name: "a status anchor cannot finish",
			sql:  `INSERT INTO fiber_runs (run_id, status, envelope, created_at) VALUES ('run-2', 'running', '{}', now())`,
			code: check,
		},
		{
			name: "a second result under a recorded key",
			sql:  `INSERT INTO fiber_run_records (run_id, record_key, envelope) VALUES ('run-1', 'step-1', '{}')`,
			code: unique,
		},
		{
			name: "a record for a run nobody accepted",
			sql:  `INSERT INTO fiber_run_records (run_id, record_key, envelope) VALUES ('ghost', 'step-1', '{}')`,
			code: foreign,
		},
		{
			name: "a second queue row for one run",
			sql:  `INSERT INTO fiber_run_queue (run_id, ready_at) VALUES ('run-1', now())`,
			code: unique,
		},
		{
			name: "a fencing token below zero",
			sql:  `INSERT INTO fiber_run_queue (run_id, attempt) VALUES ('run-3', -1)`,
			code: check,
		},
	}

	pool := migrated(t)
	ctx := context.Background()
	for _, seed := range []string{
		`INSERT INTO fiber_runs (run_id, status, envelope, created_at) VALUES ('run-1', 'pending', '{}', now())`,
		`INSERT INTO fiber_run_records (run_id, record_key, envelope) VALUES ('run-1', 'step-1', '{}')`,
		`INSERT INTO fiber_run_queue (run_id, ready_at) VALUES ('run-1', now())`,
	} {
		if _, err := pool.Exec(ctx, seed); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, tc.sql)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != tc.code {
				t.Fatalf("got %v, want SQLSTATE %s", err, tc.code)
			}
		})
	}
}

// The janitor pages pending runs in (created_at, run_id) order and resumes
// after a cursor. Run ids must sort bytewise, as Go compares them, or a cursor
// from one backend lands somewhere else on another.
func TestRunIDsSortBytewise(t *testing.T) {
	pool := migrated(t)
	ctx := context.Background()

	// Under a locale-aware collation "a" sorts before "B"; bytewise it is the
	// other way round.
	for _, id := range []string{"a", "B", "_", "0"} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO fiber_runs (run_id, status, envelope, created_at) VALUES ($1, 'pending', '{}', 'epoch')`, id); err != nil {
			t.Fatalf("insert %q: %v", id, err)
		}
	}
	rows, err := pool.Query(ctx, `SELECT run_id FROM fiber_runs ORDER BY created_at, run_id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	want := []string{"0", "B", "_", "a"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("run ids ordered %v, want %v", got, want)
	}
}

// Retention is the caller's, and the cascade is what keeps it one statement: a
// run deleted without its records would leave results no replay can reach.
func TestDeletingARunTakesItsRecords(t *testing.T) {
	pool := migrated(t)
	ctx := context.Background()

	for _, stmt := range []string{
		`INSERT INTO fiber_runs (run_id, status, envelope, created_at) VALUES ('run-1', 'succeeded', '{}', now())`,
		`INSERT INTO fiber_run_records (run_id, record_key, envelope) VALUES ('run-1', 'step-1', '{}'), ('run-1', 'step-2', '{}')`,
		`DELETE FROM fiber_runs WHERE run_id = 'run-1'`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fiber_run_records`).Scan(&left); err != nil {
		t.Fatalf("count: %v", err)
	}
	if left != 0 {
		t.Errorf("%d records outlived their run", left)
	}
}
