package bus_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/bus/bustest"
)

// postgresEnv names the Postgres the conformance suite runs against. Without
// it the Postgres subtests skip.
const postgresEnv = "FIBER_POSTGRES_TEST_URL"

var schemas atomic.Uint64

// newPostgresPool returns a pool whose search_path is a schema of its own,
// dropped when the test ends, so buses built on it cannot see each other.
func newPostgresPool(t *testing.T, url string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("fibertest_bus_%d_%d", time.Now().UnixNano(), schemas.Add(1))
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

func TestPostgresConformance(t *testing.T) {
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("set %s to run the Postgres bus conformance suite", postgresEnv)
	}
	bustest.Run(t, func(t *testing.T, cfg bustest.Config) bus.Bus {
		b := bus.NewPostgres(newPostgresPool(t, url),
			bus.WithRetention(cfg.Retention),
			bus.WithBacklog(cfg.Backlog),
			bus.WithPollInterval(25*time.Millisecond),
		)
		if err := b.Migrate(t.Context()); err != nil {
			t.Fatal(err)
		}
		return b
	})
}

// Two processes on one database are one bus: what one publishes, a reader
// built on the other's pool receives. That is the point of a shared backend.
func TestPostgresIsSharedAcrossBusesOnOneDatabase(t *testing.T) {
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("set %s", postgresEnv)
	}
	pool := newPostgresPool(t, url)
	a, b := bus.NewPostgres(pool), bus.NewPostgres(pool)
	if err := a.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := b.Migrate(t.Context()); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	s, err := b.Subscribe(t.Context(), all)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := a.Publish(t.Context(), runEvent("run_1", 1)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	e, err := s.Recv(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if e.Seq != 1 || e.Run != "run_1" {
		t.Fatalf("got %s#%d, want run_1#1", e.Run, e.Seq)
	}
}
