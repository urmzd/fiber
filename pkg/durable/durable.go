// Package durable owns the Postgres schema that anchor's run ledger and queue
// are stored in, so a run in flight outlives the process that accepted it.
//
// It is deliberately only a schema. anchor already ships Postgres
// implementations of ledger.Ledger and queue.Queue, pgledger and pgqueue in the
// module github.com/urmzd/anchor/adapters/postgres, and they are held to
// anchor's own conformance suites. What they refuse to do is own tables: an
// adapter maps onto tables the application names, validates them, and never
// executes DDL. Something has to create those tables, and that something is the
// application. This package is fiber's half of that bargain and nothing
// more: three tables, the reasons they are shaped as they are, and a Migrate
// that is safe to run from every replica at startup.
//
// Writing a second ledger and queue here would have been the wrong kind of
// thoroughness. The lease, the fence and the Attempt and Failures counters are
// the parts of durable execution that are easy to get subtly wrong, and one
// implementation that every anchor user exercises is worth more than a
// private copy only fiber does.
//
// # What the tables hold
//
// The ledger is truth and the queue is flow. Runs and Records are the ledger:
// what was accepted, what each step returned, how it ended. Queue holds only
// run ids, leases and counters, and all of it except the counters can be
// rebuilt from the ledger by anchor's janitor. They share a database here
// because a deployment that has one Postgres should not need a second system to
// be durable, never because one depends on the other, which is why no foreign
// key joins the queue to the ledger.
//
// # Wiring
//
// The column names are the adapters' defaults, so a caller starts from
// pgledger.DefaultMapping and pgqueue.DefaultMapping and overrides only the
// three table names with the constants below. Both adapters offer Validate,
// which checks a mapping against the live database; calling it after Migrate
// turns any drift between this schema and an adapter into a startup error that
// names the column, instead of a failed statement in the middle of a run.
//
// # Boundary
//
// Nothing here knows what a run means, which is pkg/runtime's concern, or
// reads or writes a row, which is the adapters'. Retention is not implemented:
// terminal runs and settled queue rows stay until something deletes them.
// Deleting a run row takes its records with it.
package durable

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/fiber/pkg/internal/pgrename"
)

// The tables this package owns. They are exported so the mapping handed to
// anchor's adapters names the same tables Migrate creates, and a rename is
// one edit instead of two that can disagree.
const (
	// RunsTable holds one row per accepted run.
	RunsTable = "fiber_runs"

	// RecordsTable holds one row per memoized activity or step result.
	RecordsTable = "fiber_run_records"

	// QueueTable holds one row per run that has ever been enqueued.
	QueueTable = "fiber_run_queue"
)

// ErrNoPool is returned by Migrate when it is handed a nil pool. It is a
// sentinel, not a panic, because the pool comes from deployment configuration
// and a missing one should stop startup with a message, not a stack trace.
var ErrNoPool = errors.New("durable: no connection pool")

// schema is everything this package owns.
//
// fiber_runs is the ledger's run table. run_id is the primary key because
// accepting a run is an insert that must be idempotent on it: the run id is
// the submit idempotency key, and ON CONFLICT needs a plain unique index to
// arbitrate on. It is COLLATE "C" because the janitor pages through pending
// runs in (created_at, run_id) order and resumes strictly after a cursor; a
// locale-aware collation would order ids differently from the Go comparison
// anchor's in-memory ledger uses, and a cursor would mean a different
// position on each backend. status carries a CHECK because it is the column the
// pending to terminal compare-and-set turns on, and a value outside the three
// anchor knows would be a run no worker can ever finish. created_at has no
// default: the adapter always writes the time the run was accepted, and a
// default would let a row inserted by hand claim a time nobody chose.
//
// The index on (status, created_at, run_id) serves the janitor's scan. It is
// not a partial index on pending rows, tempting as that is with terminal runs
// in the majority, because the adapter passes the status as a parameter and a
// cached generic plan cannot prove a parameter matches an index predicate.
//
// fiber_run_records is write-once: (run_id, record_key) is the primary key
// and the first insert wins, which is how two attempts of one run converge on
// one history. The foreign key states what the adapter already enforces, that
// a record belongs to an accepted run, and its cascade makes deleting a run a
// single statement for whoever implements retention.
//
// fiber_run_queue is flow. ready_at and lease_until are both nullable and
// their combination is the row's state: ready_at set means queued, lease_until
// set means claimed, both null means settled. A settled row is kept, never
// deleted, because attempt is the fencing token and must stay monotonic across
// a run's whole life: a parked run that resumes has to claim at attempt N+1,
// not at 1, or a zombie from the first life could pass the fence. failures is a
// separate counter for the same reason anchor separates them: attempt counts
// claims, failures counts executions that actually failed, and waiting must not
// spend retry budget.
//
// The two partial indexes are the two arms of the claim predicate, due and
// ready or holding a lapsed lease. Because settled rows accumulate with both
// columns null, indexing only the non-null values keeps a claim proportional to
// the work outstanding and not to every run the deployment has ever finished.
const schema = `
CREATE TABLE IF NOT EXISTS fiber_runs (
	run_id     text        COLLATE "C" PRIMARY KEY,
	status     text        NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
	envelope   jsonb       NOT NULL,
	created_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS fiber_runs_status_created_idx
	ON fiber_runs (status, created_at, run_id);

CREATE TABLE IF NOT EXISTS fiber_run_records (
	run_id     text  COLLATE "C" NOT NULL REFERENCES fiber_runs (run_id) ON DELETE CASCADE,
	record_key text  NOT NULL,
	envelope   jsonb NOT NULL,
	PRIMARY KEY (run_id, record_key)
);

CREATE TABLE IF NOT EXISTS fiber_run_queue (
	run_id      text        COLLATE "C" PRIMARY KEY,
	ready_at    timestamptz,
	lease_until timestamptz,
	attempt     bigint      NOT NULL DEFAULT 0 CHECK (attempt >= 0),
	failures    integer     NOT NULL DEFAULT 0 CHECK (failures >= 0)
);

CREATE INDEX IF NOT EXISTS fiber_run_queue_ready_idx
	ON fiber_run_queue (ready_at) WHERE ready_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS fiber_run_queue_lease_idx
	ON fiber_run_queue (lease_until) WHERE lease_until IS NOT NULL;
`

// migrateLock is the advisory lock Migrate holds. CREATE TABLE IF NOT EXISTS is
// not safe against itself: two processes starting together can both pass the
// existence check and one then fails on the catalog's unique index. The value
// is arbitrary, only has to be the same in every fiber, and differs from the
// registry's so the two migrations never wait on each other.
const migrateLock = 0x64757261 // "dura"

// Migrate creates the ledger and queue tables if they are not there. It is
// idempotent and safe to run from every replica at startup.
//
// It does not own the pool. The caller opened it and the caller closes it, so
// a deployment whose registry already has a pool hands the same one here.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return ErrNoPool
	}
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(migrateLock)); err != nil {
			return err
		}
		// A database created before the rename is moved forward first.
		if _, err := tx.Exec(ctx, pgrename.SQL("runs", "run_records", "run_queue")); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, schema)
		return err
	})
	if err != nil {
		return fmt.Errorf("durable: migrate: %w", err)
	}
	return nil
}
