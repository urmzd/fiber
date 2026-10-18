package durable_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urmzd/anchor/adapters/postgres/pgledger"
	"github.com/urmzd/anchor/adapters/postgres/pgqueue"
	"github.com/urmzd/anchor/pkg/ledger"
	"github.com/urmzd/anchor/pkg/ledger/ledgertest"
	"github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/queue/queuetest"
	"github.com/urmzd/anchor/pkg/run"

	"github.com/urmzd/fiber/pkg/durable"
)

var verifySchemas atomic.Uint64

func openBackends(t *testing.T) (*pgxpool.Pool, *pgledger.Ledger, *pgqueue.Queue) {
	t.Helper()
	url := os.Getenv("FIBER_POSTGRES_TEST_URL")
	if url == "" {
		t.Skip("set FIBER_POSTGRES_TEST_URL")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("fibertest_verify_%d_%d", time.Now().UnixNano(), verifySchemas.Add(1))
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+ident+" CASCADE")
		pool.Close()
	})
	l, q, err := durable.Open(ctx, pool, pgqueue.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return pool, l, q
}

func TestLedgerConformanceOnFiberSchema(t *testing.T) {
	ledgertest.Run(t, func(t *testing.T) ledger.Ledger { _, l, _ := openBackends(t); return l })
}

func TestQueueConformanceOnFiberSchema(t *testing.T) {
	queuetest.Run(t, func(t *testing.T) queue.Queue { _, _, q := openBackends(t); return q })
}

// A second ledger and queue on the same database is what a restarted process
// is: the run, its lease lineage and its failure count must all be there.
func TestRunAndLeaseLineageSurviveNewBackends(t *testing.T) {
	ctx := context.Background()
	pool, l1, q1 := openBackends(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := l1.Accept(ctx, run.Run{ID: "run-1", Status: run.StatusPending, Input: []byte(`{"a":1}`), CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := q1.Enqueue(ctx, "run-1", 0); err != nil {
		t.Fatal(err)
	}
	it, _, err := q1.Claim(ctx, 50*time.Millisecond)
	if err != nil || it.Attempt != 1 || it.Failures != 0 {
		t.Fatalf("first claim = %+v, %v", it, err)
	}
	// The process dies holding the lease. A new one comes up on the same DB.
	l2, q2, err := durable.Open(ctx, pool, pgqueue.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	var _ ledger.RunGetter = l2
	got, err := l2.GetRun(ctx, "run-1")
	if err != nil || got.Status != run.StatusPending || string(got.Input) != `{"a":1}` {
		t.Fatalf("GetRun after restart = %+v, %v", got, err)
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	it2, _, err := q2.Claim(cctx, time.Second)
	if err != nil {
		t.Fatalf("reclaim after lease expiry: %v", err)
	}
	if it2.Attempt != 2 || it2.Failures != 1 {
		t.Fatalf("reclaim = %+v, want attempt 2, failures 1", it2)
	}
	if _, err := q2.Heartbeat(ctx, it, time.Second); err == nil {
		t.Fatal("the dead process's attempt still passes the fence")
	}
	if _, ok := any(q2).(queue.DeltaLog); ok {
		t.Fatal("pgqueue now implements DeltaLog; update the report")
	}
}
