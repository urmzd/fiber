package knowledge_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/fiber/pkg/knowledge"
)

// postgresEnv names the Postgres the Store suite runs against. Without it the
// suite runs against Memory, so it stays runnable with no infrastructure; with
// it, the Postgres backend is held to exactly the same contract. The server
// must have pgvector available, which the compose stack's image does.
const postgresEnv = "FIBER_POSTGRES_TEST_URL"

// schemas numbers each store the suite builds, so tests sharing one database
// cannot see each other's documents.
var schemas atomic.Uint64

// newPostgres returns a migrated store in a schema of its own, dropped when the
// test ends.
//
// "public" stays on the search path behind the private schema. The tables are
// created in the first schema on the path, which keeps tests apart, while the
// vector type is found in public, where CREATE EXTENSION puts it for the whole
// database.
func newPostgres(t *testing.T, url string, embedder knowledge.Embedder, opts ...knowledge.Option) *knowledge.Postgres {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("fiberknowledge_%d_%d", time.Now().UnixNano(), schemas.Add(1))

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse $%s: %v", postgresEnv, err)
	}
	cfg.MaxConns = 4
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public"

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

	store := knowledge.NewPostgres(pool, embedder, opts...)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pools.Store(store, pool)
	return store
}

// pools remembers the pool behind each store the suite built, for the tests
// that need a second store on the same schema.
var pools sync.Map

func poolOf(t *testing.T, store *knowledge.Postgres) *pgxpool.Pool {
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
		t.Skipf("set %s to run the Postgres knowledge tests", postgresEnv)
	}
	return url
}

// Migrate runs from every replica at startup, so running it again, and running
// it from several places at once, has to be a no-op rather than an error.
func TestPostgresMigrateIsIdempotentAndConcurrent(t *testing.T) {
	store := newPostgres(t, postgresURL(t), knowledge.NewHashing(32))

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
// is what a restarted process is, searches what the first one indexed and
// recognises it as already indexed.
func TestPostgresIndexSurvivesANewStore(t *testing.T) {
	first := newPostgres(t, postgresURL(t), knowledge.NewHashing(32))
	doc := knowledge.Document{Source: "kb", URI: "a", Title: "Tides", Content: "The tide table for the harbour."}
	indexed := mustUpsert(t, first, tenantA, doc)

	second := knowledge.NewPostgres(poolOf(t, first), knowledge.NewHashing(32))
	hits := mustSearch(t, second, tenantA, knowledge.Query{Text: "harbour tide"})
	if len(hits) != 1 || hits[0].Title != "Tides" {
		t.Fatalf("a new store found %+v; want the document the first one indexed", hits)
	}
	again := mustUpsert(t, second, tenantA, doc)
	if again.Changed || again.Hash != indexed.Hash {
		t.Errorf("a new store re-indexed unchanged content: %+v", again)
	}
}

// Vectors of two widths cannot share a column. Swapping in a model of another
// size has to fail at startup, with an error that says why, not on a write.
func TestPostgresRefusesAnEmbedderOfAnotherWidth(t *testing.T) {
	first := newPostgres(t, postgresURL(t), knowledge.NewHashing(32))

	wider := knowledge.NewPostgres(poolOf(t, first), knowledge.NewHashing(64))
	if err := wider.Migrate(context.Background()); !errors.Is(err, knowledge.ErrDimensions) {
		t.Fatalf("Migrate with a wider embedder = %v; want ErrDimensions", err)
	}
}

// A model of the same width is a different space all the same. Its queries
// must not be compared against the old model's vectors, and resubmitting the
// corpus must re-embed it instead of calling it unchanged.
func TestPostgresDoesNotCompareVectorsAcrossModels(t *testing.T) {
	old := &synonyms{groups: [][]string{{"refund", "reimburse"}, {"invoice"}}}
	first := newPostgres(t, postgresURL(t), old)
	doc := knowledge.Document{Source: "kb", URI: "a", Content: "We reimburse within a week."}
	mustUpsert(t, first, tenantA, doc)
	if hits := mustSearch(t, first, tenantA, knowledge.Query{Text: "refund"}); len(hits) != 1 {
		t.Fatalf("setup: the original model does not find the passage: %+v", hits)
	}

	second := knowledge.NewPostgres(poolOf(t, first), renamed{old, "synonyms-v2"})
	if hits := mustSearch(t, second, tenantA, knowledge.Query{Text: "refund"}); len(hits) != 0 {
		t.Errorf("a query from one model matched vectors from another: %+v", hits)
	}
	if hits := mustSearch(t, second, tenantA, knowledge.Query{Text: "reimburse"}); len(hits) != 1 {
		t.Errorf("the lexical arm lost a document that only needs re-embedding: %+v", hits)
	}
	if r := mustUpsert(t, second, tenantA, doc); !r.Changed {
		t.Error("content embedded by another model was treated as already indexed")
	}
	if hits := mustSearch(t, second, tenantA, knowledge.Query{Text: "refund"}); len(hits) != 1 {
		t.Errorf("after re-indexing, the new model does not find the passage: %+v", hits)
	}
}

type renamed struct {
	knowledge.Embedder
	model string
}

func (r renamed) Model() string { return r.model }

// The text search configuration is checked at startup, by name, so a typo is
// a clear refusal and not a failed insert later.
func TestPostgresRefusesAnUnknownTextConfiguration(t *testing.T) {
	first := newPostgres(t, postgresURL(t), knowledge.NewHashing(32))

	bad := knowledge.NewPostgres(poolOf(t, first), knowledge.NewHashing(32), knowledge.WithTextConfig("klingon"))
	if err := bad.Migrate(context.Background()); !errors.Is(err, knowledge.ErrInvalid) {
		t.Fatalf("Migrate with an unknown text configuration = %v; want ErrInvalid", err)
	}
}

// With a stemming configuration the lexical arm matches inflections, which is
// what the option is for.
func TestPostgresStemsWhenAskedTo(t *testing.T) {
	// An embedder with no opinion, so only the lexical arm can find anything.
	blind := &synonyms{groups: [][]string{{"\x01"}}}
	store := newPostgres(t, postgresURL(t), blind, knowledge.WithTextConfig("english"))
	mustUpsert(t, store, tenantA, knowledge.Document{Source: "kb", URI: "a", Content: "Rotating credentials quarterly."})

	if hits := mustSearch(t, store, tenantA, knowledge.Query{Text: "rotate credential"}); len(hits) != 1 {
		t.Errorf("hits = %+v; want the passage found through its stems", hits)
	}
}
