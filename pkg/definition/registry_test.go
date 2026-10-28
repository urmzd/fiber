package definition_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/definition"
)

// registrySchema is fiber v0.25's agent registry.
const registrySchema = `
CREATE TABLE fiber_agents (
	tenant  text    NOT NULL,
	name    text    COLLATE "C" NOT NULL,
	head    integer NOT NULL,
	deleted boolean NOT NULL,
	PRIMARY KEY (tenant, name)
);
CREATE TABLE fiber_agent_revisions (
	tenant     text        NOT NULL,
	name       text        COLLATE "C" NOT NULL,
	rev        integer     NOT NULL CHECK (rev >= 1),
	hash       text        NOT NULL,
	spec       jsonb       NOT NULL,
	created_at timestamptz NOT NULL,
	created_by text        NOT NULL,
	PRIMARY KEY (tenant, name, rev),
	FOREIGN KEY (tenant, name) REFERENCES fiber_agents (tenant, name)
);
`

// A deployment upgraded from the registry keeps its agents: each revision
// becomes version N.0.0 with its author and time, a deleted agent is retired,
// one that cannot move is reported, and the move runs once.
func TestPostgresImportsTheRegistryOnce(t *testing.T) {
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("$%s is not set", postgresEnv)
	}
	ctx := context.Background()
	pool := newPool(t, url)
	if _, err := pool.Exec(ctx, registrySchema); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	agent := func(tenant, name string, head int, deleted bool) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO fiber_agents VALUES ($1, $2, $3, $4)`, tenant, name, head, deleted); err != nil {
			t.Fatal(err)
		}
	}
	revision := func(tenant, name string, rev int, prompt string) {
		t.Helper()
		spec := `{"name":"` + name + `","provider":"offline","model":"echo","system_prompt":"` + prompt + `"}`
		if _, err := pool.Exec(ctx, `INSERT INTO fiber_agent_revisions VALUES ($1, $2, $3, 'h', $4, $5, 'ada')`,
			tenant, name, rev, spec, at.Add(time.Duration(rev)*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	agent("acme", "docs-bot", 2, false)
	revision("acme", "docs-bot", 1, "Answer.")
	revision("acme", "docs-bot", 2, "Answer well.")
	agent("acme", "old-bot", 1, true)
	revision("acme", "old-bot", 1, "Gone.")
	agent("acme", "Bad_Name", 1, false)
	revision("acme", "Bad_Name", 1, "Broken.")

	src := definition.NewPostgres(pool)
	if err := src.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := src.ImportRegistry(ctx)
	if err != nil {
		t.Fatalf("ImportRegistry: %v", err)
	}
	if got.Agents != 2 || got.Versions != 3 || len(got.Disabled) != 1 || got.Disabled[0].Name != "Bad_Name" {
		t.Fatalf("report = %+v", got)
	}
	latest, err := src.Latest(ctx, "acme", "docs-bot")
	if err != nil {
		t.Fatal(err)
	}
	if latest.Version != "2.0.0" || latest.CreatedBy != "ada" || !latest.CreatedAt.Equal(at.Add(2*time.Hour)) {
		t.Errorf("docs-bot latest = %s by %s at %s", latest.Version, latest.CreatedBy, latest.CreatedAt)
	}
	if _, err := src.Get(ctx, "acme", "docs-bot", "1.0.0"); err != nil {
		t.Errorf("revision 1: %v", err)
	}
	if _, err := src.Latest(ctx, "acme", "old-bot"); !errors.Is(err, definition.ErrRetired) {
		t.Errorf("old-bot latest = %v, want ErrRetired", err)
	}

	// Marked: a later start reads the kept report and nothing else.
	revision("acme", "docs-bot", 3, "Answer later.")
	again, err := src.ImportRegistry(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.Agents != 2 || again.Versions != 3 {
		t.Errorf("second report = %+v", again)
	}
	if _, err := src.Get(ctx, "acme", "docs-bot", "3.0.0"); !errors.Is(err, definition.ErrNotFound) {
		t.Errorf("a revision written after the move was moved: %v", err)
	}
}

// A database that never had the registry imports nothing.
func TestPostgresImportWithoutTheRegistry(t *testing.T) {
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("$%s is not set", postgresEnv)
	}
	src := newPostgres(t, url)
	got, err := src.ImportRegistry(context.Background())
	if err != nil || got.Agents != 0 || len(got.Disabled) != 0 {
		t.Fatalf("ImportRegistry = %+v, %v", got, err)
	}
}
