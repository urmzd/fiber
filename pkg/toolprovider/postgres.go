package toolprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/fiber/pkg/internal/pgrename"
)

// schema is everything the Postgres store owns: one row per provider, one row
// per accepted snapshot.
//
// fiber_tool_providers holds the current record and exists to be locked:
// an Accept takes the provider's row FOR UPDATE, so two accepts for one
// provider serialize on it and the snapshot number each checks is the one it
// writes after.
//
// credential_scope arrived after the table did, so it is added to a table
// that predates it, where every existing row is the shared provider it was.
//
// fiber_tool_snapshots only ever receives INSERTs. Its tools column is
// text, not jsonb, on purpose: jsonb reorders keys and rewrites whitespace,
// and an accepted schema is shown to the model byte for byte, so a snapshot
// must read back exactly as it was written.
const schema = `
CREATE TABLE IF NOT EXISTS fiber_tool_providers (
	tenant         text        NOT NULL,
	name           text        NOT NULL,
	transport      text        NOT NULL CHECK (transport IN ('stdio', 'http')),
	endpoint       text        NOT NULL,
	args           jsonb       NOT NULL DEFAULT '[]'::jsonb,
	credential_ref text        NOT NULL,
	credential_env text        NOT NULL,
	version        bigint      NOT NULL CHECK (version >= 1),
	updated_by     text        NOT NULL,
	updated_at     timestamptz NOT NULL,
	PRIMARY KEY (tenant, name)
);

ALTER TABLE fiber_tool_providers ADD COLUMN IF NOT EXISTS
	credential_scope text NOT NULL DEFAULT 'shared' CHECK (credential_scope IN ('shared', 'person'));

CREATE TABLE IF NOT EXISTS fiber_tool_snapshots (
	tenant      text        NOT NULL,
	provider    text        NOT NULL,
	version     bigint      NOT NULL CHECK (version >= 1),
	tools       text        NOT NULL,
	accepted_by text        NOT NULL,
	accepted_at timestamptz NOT NULL,
	PRIMARY KEY (tenant, provider, version),
	FOREIGN KEY (tenant, provider) REFERENCES fiber_tool_providers (tenant, name)
);
`

// migrateLock is the advisory lock Migrate holds, the same one every fiber
// store takes, so concurrent CREATE TABLE IF NOT EXISTS from several replicas
// cannot race on the catalog.
const migrateLock = 0x6d616e64 // "mand"

// Postgres is a Store that outlives the process. It does not own the pool.
type Postgres struct {
	pool *pgxpool.Pool
	settings
}

// NewPostgres returns a store on pool. Call Migrate once before first use.
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
		// A database created before the rename is moved forward first.
		if _, err := tx.Exec(ctx, pgrename.SQL("tool_providers", "tool_snapshots")); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, schema)
		return err
	})
	if err != nil {
		return fmt.Errorf("toolprovider: migrate: %w", err)
	}
	return nil
}

const providerColumns = `tenant, name, transport, endpoint, args, credential_scope, credential_ref, credential_env, version, updated_by, updated_at`

// PutProvider implements Store.
func (p *Postgres) PutProvider(ctx context.Context, rec Provider) (Provider, error) {
	if err := ctx.Err(); err != nil {
		return Provider{}, err
	}
	if err := rec.Validate(); err != nil {
		return Provider{}, err
	}
	args := rec.Args
	if args == nil {
		args = []string{}
	}
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return Provider{}, fmt.Errorf("toolprovider: encode args: %w", err)
	}
	out := rec.stored()
	out.Version = rec.Version + 1
	out.UpdatedAt = p.stamp()

	var tag interface{ RowsAffected() int64 }
	if rec.Version == 0 {
		tag, err = p.pool.Exec(ctx,
			`INSERT INTO fiber_tool_providers (`+providerColumns+`)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9, $10)
			 ON CONFLICT (tenant, name) DO NOTHING`,
			rec.Tenant, rec.Name, string(rec.Transport), rec.Endpoint, argsJSON,
			string(out.CredentialScope), rec.CredentialRef, rec.CredentialEnv, rec.UpdatedBy, out.UpdatedAt)
		if err != nil {
			return Provider{}, wrap("put provider", err)
		}
		if tag.RowsAffected() == 0 {
			return Provider{}, fmt.Errorf("%w: provider %q", ErrExists, rec.Name)
		}
		return out, nil
	}
	tag, err = p.pool.Exec(ctx,
		`UPDATE fiber_tool_providers
		    SET transport = $3, endpoint = $4, args = $5, credential_scope = $6, credential_ref = $7,
		        credential_env = $8, version = version + 1, updated_by = $9, updated_at = $10
		  WHERE tenant = $1 AND name = $2 AND version = $11`,
		rec.Tenant, rec.Name, string(rec.Transport), rec.Endpoint, argsJSON,
		string(out.CredentialScope), rec.CredentialRef, rec.CredentialEnv, rec.UpdatedBy, out.UpdatedAt, int64(rec.Version))
	if err != nil {
		return Provider{}, wrap("put provider", err)
	}
	if tag.RowsAffected() == 1 {
		return out, nil
	}
	cur, err := p.GetProvider(ctx, rec.Tenant, rec.Name)
	if err != nil {
		return Provider{}, err
	}
	return Provider{}, fmt.Errorf("%w: provider %q is at version %d, not %d", ErrConflict, rec.Name, cur.Version, rec.Version)
}

// GetProvider implements Store.
func (p *Postgres) GetProvider(ctx context.Context, tenant, name string) (Provider, error) {
	if err := checkName(ctx, tenant, name); err != nil {
		return Provider{}, err
	}
	rows, err := p.pool.Query(ctx,
		`SELECT `+providerColumns+` FROM fiber_tool_providers WHERE tenant = $1 AND name = $2`, tenant, name)
	if err != nil {
		return Provider{}, wrap("get provider", err)
	}
	got, err := collectProviders(rows)
	if err != nil {
		return Provider{}, wrap("get provider", err)
	}
	if len(got) == 0 {
		return Provider{}, fmt.Errorf("%w: provider %q", ErrNotFound, name)
	}
	return got[0], nil
}

// ListProviders implements Store.
func (p *Postgres) ListProviders(ctx context.Context, tenant string) ([]Provider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tenant == "" {
		return nil, fmt.Errorf("%w: no tenant", ErrInvalid)
	}
	rows, err := p.pool.Query(ctx,
		`SELECT `+providerColumns+` FROM fiber_tool_providers WHERE tenant = $1 ORDER BY name COLLATE "C"`, tenant)
	if err != nil {
		return nil, wrap("list providers", err)
	}
	out, err := collectProviders(rows)
	if err != nil {
		return nil, wrap("list providers", err)
	}
	return out, nil
}

func collectProviders(rows pgx.Rows) ([]Provider, error) {
	defer rows.Close()
	out := []Provider{}
	for rows.Next() {
		var (
			rec       Provider
			transport string
			scope     string
			args      []byte
			version   int64
		)
		if err := rows.Scan(&rec.Tenant, &rec.Name, &transport, &rec.Endpoint, &args,
			&scope, &rec.CredentialRef, &rec.CredentialEnv, &version, &rec.UpdatedBy, &rec.UpdatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(args, &rec.Args); err != nil {
			return nil, fmt.Errorf("decode args of %q: %w", rec.Name, err)
		}
		if len(rec.Args) == 0 {
			rec.Args = nil
		}
		rec.Transport, rec.CredentialScope = Transport(transport), CredentialScope(scope)
		rec.Version, rec.UpdatedAt = uint64(version), rec.UpdatedAt.UTC()
		out = append(out, rec)
	}
	return out, rows.Err()
}

// Accept implements Store.
func (p *Postgres) Accept(ctx context.Context, tenant, provider string, base uint64, tools []Tool, by string) (Snapshot, error) {
	if err := checkName(ctx, tenant, provider); err != nil {
		return Snapshot{}, err
	}
	prepared, err := prepareTools(provider, tools)
	if err != nil {
		return Snapshot{}, err
	}
	body, err := json.Marshal(prepared)
	if err != nil {
		return Snapshot{}, fmt.Errorf("toolprovider: encode tools: %w", err)
	}
	s := Snapshot{
		Tenant: tenant, Provider: provider, Version: base + 1,
		Tools: prepared, AcceptedBy: by, AcceptedAt: p.stamp(),
	}
	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		var one int
		err := tx.QueryRow(ctx,
			`SELECT 1 FROM fiber_tool_providers WHERE tenant = $1 AND name = $2 FOR UPDATE`,
			tenant, provider).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: provider %q", ErrNotFound, provider)
		}
		if err != nil {
			return err
		}
		var cur int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(max(version), 0) FROM fiber_tool_snapshots WHERE tenant = $1 AND provider = $2`,
			tenant, provider).Scan(&cur); err != nil {
			return err
		}
		if uint64(cur) != base {
			return fmt.Errorf("%w: provider %q is at snapshot %d, not %d", ErrConflict, provider, cur, base)
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO fiber_tool_snapshots (tenant, provider, version, tools, accepted_by, accepted_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			tenant, provider, int64(s.Version), string(body), by, s.AcceptedAt)
		return err
	})
	if err != nil {
		return Snapshot{}, wrap("accept", err)
	}
	return s, nil
}

// Snapshot implements Store.
func (p *Postgres) Snapshot(ctx context.Context, tenant, provider string, version uint64) (Snapshot, error) {
	if err := checkName(ctx, tenant, provider); err != nil {
		return Snapshot{}, err
	}
	var exists bool
	if err := p.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM fiber_tool_providers WHERE tenant = $1 AND name = $2)`,
		tenant, provider).Scan(&exists); err != nil {
		return Snapshot{}, wrap("snapshot", err)
	}
	if !exists {
		return Snapshot{}, fmt.Errorf("%w: provider %q", ErrNotFound, provider)
	}
	q := `SELECT version, tools, accepted_by, accepted_at FROM fiber_tool_snapshots
	       WHERE tenant = $1 AND provider = $2 AND version = $3`
	args := []any{tenant, provider, int64(version)}
	if version == 0 {
		q = `SELECT version, tools, accepted_by, accepted_at FROM fiber_tool_snapshots
		      WHERE tenant = $1 AND provider = $2 ORDER BY version DESC LIMIT 1`
		args = args[:2]
	}
	var (
		s    = Snapshot{Tenant: tenant, Provider: provider}
		v    int64
		body string
	)
	err := p.pool.QueryRow(ctx, q, args...).Scan(&v, &body, &s.AcceptedBy, &s.AcceptedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows) && version == 0:
		return emptySnapshot(tenant, provider), nil
	case errors.Is(err, pgx.ErrNoRows):
		return Snapshot{}, fmt.Errorf("%w: provider %q has no snapshot %d", ErrNotFound, provider, version)
	case err != nil:
		return Snapshot{}, wrap("snapshot", err)
	}
	if err := json.Unmarshal([]byte(body), &s.Tools); err != nil {
		return Snapshot{}, fmt.Errorf("toolprovider: decode snapshot %d of %q: %w", v, provider, err)
	}
	s.Version, s.AcceptedAt = uint64(v), s.AcceptedAt.UTC()
	return s, nil
}

// wrap adds this package's context to a database failure and leaves the
// sentinels alone.
func wrap(op string, err error) error {
	for _, sentinel := range []error{ErrNotFound, ErrExists, ErrConflict, ErrInvalid, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	return fmt.Errorf("toolprovider: postgres %s: %w", op, err)
}
