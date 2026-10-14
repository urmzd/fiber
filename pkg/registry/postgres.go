package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/mandatum/pkg/agentspec"
)

// schema is everything the Postgres store owns: one row per agent name, one row
// per revision.
//
// The split is the tombstone rule written as tables. mandatum_agents holds the
// two facts that are allowed to change, which revision is current and whether
// the name is retired; mandatum_agent_revisions holds what is not, and this
// package never issues an UPDATE or a DELETE against it. head is kept on the
// agent row rather than derived with max(rev) so that the row is the single
// thing a writer locks: two revises of one agent serialize on it, and two
// agents never wait for each other.
//
// name is COLLATE "C" because List orders by it and resumes after it. A
// locale-aware collation would order names differently from the Go comparison
// Memory uses, and a page token issued by one backend would mean a different
// position in the other.
const schema = `
CREATE TABLE IF NOT EXISTS mandatum_agents (
	tenant  text    NOT NULL,
	name    text    COLLATE "C" NOT NULL,
	head    integer NOT NULL,
	deleted boolean NOT NULL,
	PRIMARY KEY (tenant, name)
);

CREATE TABLE IF NOT EXISTS mandatum_agent_revisions (
	tenant     text        NOT NULL,
	name       text        COLLATE "C" NOT NULL,
	rev        integer     NOT NULL CHECK (rev >= 1),
	hash       text        NOT NULL,
	spec       jsonb       NOT NULL,
	created_at timestamptz NOT NULL,
	created_by text        NOT NULL,
	PRIMARY KEY (tenant, name, rev),
	FOREIGN KEY (tenant, name) REFERENCES mandatum_agents (tenant, name)
);
`

// migrateLock is the advisory lock Migrate holds. CREATE TABLE IF NOT EXISTS is
// not safe against itself: two processes starting together can both pass the
// existence check and one then fails on the catalog's unique index. The value
// is arbitrary and only has to be the same in every mandatum.
const migrateLock = 0x6d616e64 // "mand"

// Postgres is a Store that outlives the process.
//
// It keeps every rule Memory documents, and it is held to them by the same
// tests: the suite in this package runs against Postgres whenever
// $MANDATUM_POSTGRES_TEST_URL is set. What it adds is the one thing Memory says
// it lacks, so a deployment whose agent definitions must survive a restart
// swaps this in and changes nothing else.
//
// It does not own the pool. The caller opened it and the caller closes it,
// because a deployment that already has a pool should not be made to hold two.
type Postgres struct {
	pool *pgxpool.Pool
	settings
}

// NewPostgres returns a store on pool. It does not touch the database; call
// Migrate once before the first use.
func NewPostgres(pool *pgxpool.Pool, opts ...Option) *Postgres {
	return &Postgres{pool: pool, settings: newSettings(opts)}
}

var _ Store = (*Postgres)(nil)

// Migrate creates the tables this store needs if they are not there. It is
// idempotent and safe to run from every replica at startup.
func (p *Postgres) Migrate(ctx context.Context) error {
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(migrateLock)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, schema)
		return err
	})
	if err != nil {
		return fmt.Errorf("registry: migrate: %w", err)
	}
	return nil
}

// Create records spec as a new agent. See Memory.Create for why a deleted name
// continues its numbering.
//
// The name is claimed by inserting its row before reading it. Two creates of
// one name then meet on the primary key rather than on a check each could pass:
// the second insert waits for the first transaction, finds the row live, and
// reports ErrExists.
func (p *Postgres) Create(ctx context.Context, tenant string, spec agentspec.AgentSpec, by string) (agentspec.Revision, error) {
	canonical, err := p.prepare(ctx, tenant, spec)
	if err != nil {
		return agentspec.Revision{}, err
	}
	var out agentspec.Revision
	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO mandatum_agents (tenant, name, head, deleted) VALUES ($1, $2, 0, true)
			 ON CONFLICT (tenant, name) DO NOTHING`,
			tenant, canonical.Name); err != nil {
			return err
		}
		head, deleted, err := lockAgent(ctx, tx, tenant, canonical.Name)
		if err != nil {
			return err
		}
		if !deleted {
			return fmt.Errorf("%w: agent %q is already at revision %d", ErrExists, canonical.Name, head)
		}
		out, err = p.appendLocked(ctx, tx, tenant, canonical, head, by)
		return err
	})
	if err != nil {
		return agentspec.Revision{}, wrap("create", err)
	}
	return out, nil
}

// Revise appends spec as the next revision of a live agent. The concurrency
// check runs before the no-op check, for the reason Memory.Revise gives.
func (p *Postgres) Revise(ctx context.Context, tenant string, spec agentspec.AgentSpec, expectedRev int, by string) (agentspec.Revision, error) {
	if expectedRev < 0 {
		return agentspec.Revision{}, fmt.Errorf("%w: expected revision %d is negative", ErrInvalid, expectedRev)
	}
	canonical, err := p.prepare(ctx, tenant, spec)
	if err != nil {
		return agentspec.Revision{}, err
	}
	var out agentspec.Revision
	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		head, deleted, err := lockAgent(ctx, tx, tenant, canonical.Name)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && deleted) {
			return fmt.Errorf("%w: agent %q", ErrNotFound, canonical.Name)
		}
		if err != nil {
			return err
		}
		if expectedRev != 0 && expectedRev != head {
			return fmt.Errorf("%w: agent %q is at revision %d, not %d; re-read it and reapply the change", ErrConflict, canonical.Name, head, expectedRev)
		}
		out, err = p.appendLocked(ctx, tx, tenant, canonical, head, by)
		return err
	})
	if err != nil {
		return agentspec.Revision{}, wrap("revise", err)
	}
	return out, nil
}

// Get reads one revision. rev zero is the current one, which a tombstone hides;
// an explicit rev resolves whether or not the agent is live.
func (p *Postgres) Get(ctx context.Context, tenant, name string, rev int) (agentspec.Revision, error) {
	if err := ctx.Err(); err != nil {
		return agentspec.Revision{}, err
	}
	if err := checkTenant(tenant); err != nil {
		return agentspec.Revision{}, err
	}
	key := agentspec.NormalizeName(name)
	if key == "" {
		return agentspec.Revision{}, fmt.Errorf("%w: no agent name", ErrInvalid)
	}
	if rev < 0 {
		return agentspec.Revision{}, fmt.Errorf("%w: revision %d is negative", ErrInvalid, rev)
	}

	// One statement, so the head it reports and the revision it returns are
	// the same snapshot: a revise landing between two queries could otherwise
	// hand back revision N described as the latest of N+1.
	var (
		head    int
		deleted bool
		row     revisionRow
	)
	err := p.pool.QueryRow(ctx,
		`SELECT a.head, a.deleted, r.rev, r.hash, r.spec, r.created_at, r.created_by
		   FROM mandatum_agents a
		   LEFT JOIN mandatum_agent_revisions r
		     ON r.tenant = a.tenant AND r.name = a.name
		    AND r.rev = CASE WHEN $3::integer = 0 THEN a.head ELSE $3::integer END
		  WHERE a.tenant = $1 AND a.name = $2`,
		tenant, key, rev,
	).Scan(&head, &deleted, &row.rev, &row.hash, &row.spec, &row.at, &row.by)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return agentspec.Revision{}, fmt.Errorf("%w: agent %q", ErrNotFound, key)
	case err != nil:
		return agentspec.Revision{}, wrap("get", err)
	case rev == 0 && deleted:
		return agentspec.Revision{}, fmt.Errorf("%w: agent %q is deleted; revisions 1 to %d still resolve by number", ErrNotFound, key, head)
	case row.rev == nil:
		return agentspec.Revision{}, fmt.Errorf("%w: agent %q has no revision %d; the latest is %d", ErrNotFound, key, rev, head)
	}
	return row.decode(key)
}

// List returns one page of live agents at their current revision.
func (p *Postgres) List(ctx context.Context, tenant string, page Page) ([]agentspec.Revision, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := checkTenant(tenant); err != nil {
		return nil, "", err
	}
	after, err := decodeToken(page.Token, tenant)
	if err != nil {
		return nil, "", err
	}
	size := page.size()

	// One more than the page, which is how "is there another page" is
	// answered without a second query. An empty after sorts before every
	// name, so the first page needs no special case.
	rows, err := p.pool.Query(ctx,
		`SELECT a.name, r.rev, r.hash, r.spec, r.created_at, r.created_by
		   FROM mandatum_agents a
		   JOIN mandatum_agent_revisions r
		     ON r.tenant = a.tenant AND r.name = a.name AND r.rev = a.head
		  WHERE a.tenant = $1 AND NOT a.deleted AND a.name > $2
		  ORDER BY a.name
		  LIMIT $3`,
		tenant, after, size+1)
	if err != nil {
		return nil, "", wrap("list", err)
	}
	out, err := collect(rows)
	if err != nil {
		return nil, "", wrap("list", err)
	}
	if out == nil {
		out = []agentspec.Revision{}
	}
	next := ""
	if len(out) > size {
		out = out[:size]
		next = encodeToken(tenant, out[size-1].Spec.Name)
	}
	return out, next, nil
}

// History returns every revision of one agent, oldest first, deleted or not.
func (p *Postgres) History(ctx context.Context, tenant, name string) ([]agentspec.Revision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkTenant(tenant); err != nil {
		return nil, err
	}
	key := agentspec.NormalizeName(name)
	if key == "" {
		return nil, fmt.Errorf("%w: no agent name", ErrInvalid)
	}
	rows, err := p.pool.Query(ctx,
		`SELECT name, rev, hash, spec, created_at, created_by
		   FROM mandatum_agent_revisions
		  WHERE tenant = $1 AND name = $2
		  ORDER BY rev`,
		tenant, key)
	if err != nil {
		return nil, wrap("history", err)
	}
	out, err := collect(rows)
	if err != nil {
		return nil, wrap("history", err)
	}
	// An agent row never commits without a revision, so no revisions means no
	// agent.
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: agent %q", ErrNotFound, key)
	}
	return out, nil
}

// Delete retires a name, leaving its history intact. Deleting a deleted agent
// is ErrNotFound, for the reason Memory.Delete gives.
func (p *Postgres) Delete(ctx context.Context, tenant, name string, expectedRev int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkTenant(tenant); err != nil {
		return err
	}
	key := agentspec.NormalizeName(name)
	if key == "" {
		return fmt.Errorf("%w: no agent name", ErrInvalid)
	}
	if expectedRev < 0 {
		return fmt.Errorf("%w: expected revision %d is negative", ErrInvalid, expectedRev)
	}
	// The row lock is what makes the check and the retirement one step: a
	// revise of the same agent waits here, then finds the name retired.
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		head, deleted, err := lockAgent(ctx, tx, tenant, key)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && deleted) {
			return fmt.Errorf("%w: agent %q", ErrNotFound, key)
		}
		if err != nil {
			return err
		}
		if expectedRev != 0 && expectedRev != head {
			return fmt.Errorf("%w: agent %q is at revision %d, not %d; re-read it before retiring it", ErrConflict, key, head, expectedRev)
		}
		_, err = tx.Exec(ctx,
			`UPDATE mandatum_agents SET deleted = true WHERE tenant = $1 AND name = $2`,
			tenant, key)
		return err
	})
	if err != nil {
		return wrap("delete", err)
	}
	return nil
}

// lockAgent reads an agent row and holds it until the transaction ends. It is
// the Postgres spelling of Memory's write lock, scoped to one agent.
func lockAgent(ctx context.Context, tx pgx.Tx, tenant, name string) (head int, deleted bool, err error) {
	err = tx.QueryRow(ctx,
		`SELECT head, deleted FROM mandatum_agents WHERE tenant = $1 AND name = $2 FOR UPDATE`,
		tenant, name,
	).Scan(&head, &deleted)
	return head, deleted, err
}

// appendLocked appends the next revision, or returns the current one when the
// content has not changed. The caller holds the agent's row lock. Either way
// the name ends up live, which is what makes a create of a deleted name with
// its old content a revival rather than an error.
func (p *Postgres) appendLocked(ctx context.Context, tx pgx.Tx, tenant string, canonical agentspec.AgentSpec, head int, by string) (agentspec.Revision, error) {
	if head > 0 {
		var row revisionRow
		err := tx.QueryRow(ctx,
			`SELECT rev, hash, spec, created_at, created_by
			   FROM mandatum_agent_revisions
			  WHERE tenant = $1 AND name = $2 AND rev = $3`,
			tenant, canonical.Name, head,
		).Scan(&row.rev, &row.hash, &row.spec, &row.at, &row.by)
		if err != nil {
			return agentspec.Revision{}, err
		}
		if *row.hash == canonical.Hash() {
			if _, err := tx.Exec(ctx,
				`UPDATE mandatum_agents SET deleted = false WHERE tenant = $1 AND name = $2`,
				tenant, canonical.Name); err != nil {
				return agentspec.Revision{}, err
			}
			return row.decode(canonical.Name)
		}
	}

	// Postgres keeps microseconds. Truncating before the revision is built
	// means the value returned now is the value every later read returns,
	// rather than one that loses its last three digits on the way back.
	rev, err := agentspec.NewRevisionWith(canonical, head+1, by, p.clock().Truncate(time.Microsecond), p.providers)
	if err != nil {
		return agentspec.Revision{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	body, err := json.Marshal(rev.Spec)
	if err != nil {
		return agentspec.Revision{}, fmt.Errorf("encode agent %q: %w", canonical.Name, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO mandatum_agent_revisions (tenant, name, rev, hash, spec, created_at, created_by)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tenant, canonical.Name, rev.Rev, rev.Hash, body, rev.CreatedAt, rev.CreatedBy); err != nil {
		return agentspec.Revision{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE mandatum_agents SET head = $3, deleted = false WHERE tenant = $1 AND name = $2`,
		tenant, canonical.Name, rev.Rev); err != nil {
		return agentspec.Revision{}, err
	}
	return rev, nil
}

// revisionRow is one stored revision as it comes off the wire. The fields are
// pointers because Get reads them through a LEFT JOIN, where a missing revision
// is a row of nulls rather than no row.
type revisionRow struct {
	rev  *int
	hash *string
	spec []byte
	at   *time.Time
	by   *string
}

// decode rebuilds the revision and checks it against its recorded hash.
//
// The check is the store refusing to vouch for a row it did not write. A
// revision is the bytes a run replays against, so a spec column edited by hand
// has to fail here, loudly, rather than reach a worker as a definition that
// claims an address it does not hash to.
func (r revisionRow) decode(name string) (agentspec.Revision, error) {
	var spec agentspec.AgentSpec
	if err := json.Unmarshal(r.spec, &spec); err != nil {
		return agentspec.Revision{}, fmt.Errorf("decode agent %q revision %d: %w", name, *r.rev, err)
	}
	if got := spec.Hash(); got != *r.hash {
		return agentspec.Revision{}, fmt.Errorf("agent %q revision %d records hash %s but its content hashes to %s; an immutable revision changed", name, *r.rev, *r.hash, got)
	}
	return agentspec.Revision{
		Spec:      spec,
		Rev:       *r.rev,
		Hash:      *r.hash,
		CreatedAt: r.at.UTC(),
		CreatedBy: *r.by,
	}, nil
}

// collect decodes rows of (name, rev, hash, spec, created_at, created_by).
func collect(rows pgx.Rows) ([]agentspec.Revision, error) {
	defer rows.Close()
	var out []agentspec.Revision
	for rows.Next() {
		var (
			name string
			row  revisionRow
		)
		if err := rows.Scan(&name, &row.rev, &row.hash, &row.spec, &row.at, &row.by); err != nil {
			return nil, err
		}
		rev, err := row.decode(name)
		if err != nil {
			return nil, err
		}
		out = append(out, rev)
	}
	return out, rows.Err()
}

// wrap adds this package's context to a database failure and leaves the
// sentinels alone: they were built with their own message, and a caller
// matching ErrExists should read "already exists", not "postgres: create".
func wrap(op string, err error) error {
	for _, sentinel := range []error{ErrNotFound, ErrExists, ErrConflict, ErrInvalid, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	return fmt.Errorf("registry: postgres %s: %w", op, err)
}
