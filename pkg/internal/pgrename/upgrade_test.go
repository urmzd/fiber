package pgrename_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urmzd/anchor/adapters/postgres/pgledger"
	"github.com/urmzd/anchor/pkg/run"

	"github.com/urmzd/fiber/pkg/durable"
	"github.com/urmzd/fiber/pkg/internal/pgrename"
	"github.com/urmzd/fiber/pkg/knowledge"
	"github.com/urmzd/fiber/pkg/thread"
	"github.com/urmzd/fiber/pkg/toolprovider"
)

const postgresEnv = "FIBER_POSTGRES_TEST_URL"

func pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("set %s to run the upgrade test", postgresEnv)
	}
	ctx := context.Background()
	schema := fmt.Sprintf("fibertest_upgrade_%d", time.Now().UnixNano())
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 4
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public" // pgvector lives in public
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err := p.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		p.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = p.Exec(context.Background(), "DROP SCHEMA "+ident+" CASCADE")
		p.Close()
	})
	return p
}

// downgrade turns the current schema into what a database written before the
// rename looks like: every fiber_ object under the old prefix.
func downgrade(t *testing.T, ctx context.Context, p *pgxpool.Pool) {
	t.Helper()
	_, err := p.Exec(ctx, `
DO $$
DECLARE r record;
BEGIN
	FOR r IN SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace
		WHERE ns.nspname = current_schema() AND c.relkind IN ('r', 'p') AND c.relname LIKE 'fiber\_%' AND c.relname NOT LIKE 'fiber\_bus\_%'
	LOOP
		EXECUTE format('ALTER TABLE %I RENAME TO %I', r.relname, '`+pgrename.LegacyPrefix+`' || substr(r.relname, 7));
	END LOOP;
	FOR r IN SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace
		WHERE ns.nspname = current_schema() AND c.relkind IN ('i', 'S') AND c.relname LIKE 'fiber\_%' AND c.relname NOT LIKE 'fiber\_bus\_%'
	LOOP
		EXECUTE format('ALTER %s %I RENAME TO %I', CASE WHEN r.relkind = 'S' THEN 'SEQUENCE' ELSE 'INDEX' END,
			r.relname, '`+pgrename.LegacyPrefix+`' || substr(r.relname, 7));
	END LOOP;
	FOR r IN SELECT con.conname, cl.relname AS tbl FROM pg_constraint con JOIN pg_class cl ON cl.oid = con.conrelid
		JOIN pg_namespace ns ON ns.oid = cl.relnamespace
		WHERE ns.nspname = current_schema() AND con.conname LIKE 'fiber\_%'
	LOOP
		EXECUTE format('ALTER TABLE %I RENAME CONSTRAINT %I TO %I', r.tbl, r.conname, '`+pgrename.LegacyPrefix+`' || substr(r.conname, 7));
	END LOOP;
END $$`)
	if err != nil {
		t.Fatalf("downgrade: %v", err)
	}
}

// named counts the objects in the current schema whose name starts with
// prefix, tables, indexes, sequences and constraints together.
func named(t *testing.T, ctx context.Context, p *pgxpool.Pool, prefix string) int {
	t.Helper()
	var n int
	err := p.QueryRow(ctx, `
SELECT (SELECT count(*) FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace
		WHERE ns.nspname = current_schema() AND left(c.relname, length($1)) = $1)
     + (SELECT count(*) FROM pg_constraint con JOIN pg_class cl ON cl.oid = con.conrelid
		JOIN pg_namespace ns ON ns.oid = cl.relnamespace
		WHERE ns.nspname = current_schema() AND left(con.conname, length($1)) = $1)`, prefix).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

type stores struct {
	threads *thread.Postgres
	docs    *knowledge.Postgres
	tools   *toolprovider.Postgres
	ledger  *pgledger.Ledger
}

func migrate(t *testing.T, ctx context.Context, p *pgxpool.Pool) stores {
	t.Helper()
	s := stores{
		threads: thread.NewPostgres(p),
		docs:    knowledge.NewPostgres(p, knowledge.NewHashing(0)),
		tools:   toolprovider.NewPostgres(p),
	}
	for name, m := range map[string]func(context.Context) error{
		"threads": s.threads.Migrate, "knowledge": s.docs.Migrate, "tool providers": s.tools.Migrate,
	} {
		if err := m(ctx); err != nil {
			t.Fatalf("migrate %s: %v", name, err)
		}
	}
	lgr, _, err := durable.Open(ctx, p)
	if err != nil {
		t.Fatalf("durable.Open: %v", err)
	}
	s.ledger = lgr
	return s
}

// A database written before the rename keeps every row across the upgrade:
// the migration renames its objects to fiber_, re-addresses its revisions
// under the current hash domain, and leaves nothing under the old prefix.
func TestADatabaseFromBeforeTheRenameKeepsItsData(t *testing.T) {
	ctx := context.Background()
	p := pool(t)

	// Written by fiber, then made to look as it would have before the rename.
	before := migrate(t, ctx, p)
	key := thread.Key{Tenant: "acme", Agent: "docs-bot", Surface: "slack://T01/C02?thread=1"}
	if _, err := before.threads.Append(ctx, key, thread.Entry{Role: thread.RoleUser, Text: "hello", RunID: "r1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := before.docs.Upsert(ctx, "acme", knowledge.Document{Source: "handbook", URI: "release.md", Content: "Merging to main runs the release workflow."}); err != nil {
		t.Fatal(err)
	}
	if _, err := before.tools.PutProvider(ctx, toolprovider.Provider{Tenant: "acme", Name: "issues", Transport: toolprovider.TransportHTTP, Endpoint: "https://mcp.example.com"}); err != nil {
		t.Fatal(err)
	}
	if err := before.ledger.Accept(ctx, run.Run{ID: "r1", Name: "fiber.turn", Input: []byte(`{}`), Status: run.StatusPending, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	downgrade(t, ctx, p)
	if named(t, ctx, p, "fiber_") != 0 || named(t, ctx, p, pgrename.LegacyPrefix) == 0 {
		t.Fatalf("the downgrade left %d fiber_ objects and %d old ones", named(t, ctx, p, "fiber_"), named(t, ctx, p, pgrename.LegacyPrefix))
	}
	// The upgrade, twice: it is idempotent.
	migrate(t, ctx, p)
	after := migrate(t, ctx, p)
	if n := named(t, ctx, p, pgrename.LegacyPrefix); n != 0 {
		t.Errorf("%d objects still carry the old prefix", n)
	}

	entries, err := after.threads.History(ctx, key, 1)
	if err != nil || len(entries) != 1 || entries[0].Text != "hello" {
		t.Errorf("thread after the upgrade = %+v, %v", entries, err)
	}
	hits, err := after.docs.Search(ctx, "acme", knowledge.Query{Text: "release workflow", Sources: []string{"handbook"}})
	if err != nil || len(hits) == 0 {
		t.Errorf("knowledge after the upgrade = %v, %v", hits, err)
	}
	if _, err := after.tools.GetProvider(ctx, "acme", "issues"); err != nil {
		t.Errorf("tool provider after the upgrade: %v", err)
	}
	if got, err := after.ledger.GetRun(ctx, "r1"); err != nil || got.ID != "r1" {
		t.Errorf("run after the upgrade = %+v, %v", got, err)
	}
}

// A process from before the rename that starts after the upgrade creates its
// old, empty tables again and writes there. The next migration refuses to
// choose between the two and names both, instead of skipping the old table
// and leaving whatever was written to it orphaned.
func TestAnOldTableRecreatedAfterTheUpgradeFailsTheMigrationNamingBoth(t *testing.T) {
	ctx := context.Background()
	p := pool(t)
	migrate(t, ctx, p)
	old := pgrename.LegacyPrefix + "threads"
	if _, err := p.Exec(ctx, `CREATE TABLE `+old+` (LIKE fiber_threads INCLUDING ALL)`); err != nil {
		t.Fatal(err)
	}

	err := thread.NewPostgres(p).Migrate(ctx)
	if err == nil {
		t.Fatalf("migrating with both %s and fiber_threads present succeeded", old)
	}
	for _, name := range []string{old, "fiber_threads"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("the migration error %q does not name %s", err, name)
		}
	}
	// Nothing was renamed or dropped: the operator decides.
	if n := named(t, ctx, p, old); n == 0 {
		t.Errorf("%s is gone after the refused migration", old)
	}
	var fiberThreads *string
	if err := p.QueryRow(ctx, `SELECT to_regclass('fiber_threads')::text`).Scan(&fiberThreads); err != nil || fiberThreads == nil {
		t.Errorf("fiber_threads after the refused migration = %v, %v", fiberThreads, err)
	}

	// Once the old table is gone, the migration goes through again.
	if _, err := p.Exec(ctx, `DROP TABLE `+old); err != nil {
		t.Fatal(err)
	}
	if err := thread.NewPostgres(p).Migrate(ctx); err != nil {
		t.Errorf("migrating after dropping %s: %v", old, err)
	}
}
