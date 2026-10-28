package definition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/urmzd/fiber/pkg/internal/pgrename"
)

// Agents were kept by fiber's registry before v0.26, in fiber_agents and
// fiber_agent_revisions, as AgentSpec JSON with a revision number. They are
// read only here, to move them once into fiber_definitions.

// RegistryImportedBy retires an agent the registry had deleted.
const RegistryImportedBy = "system:registry"

// registryMarker is the row in fiber_definition_imports that says the move
// ran.
const registryMarker = "registry"

// RegistryImport is what ImportRegistry did. It is kept with the marker.
type RegistryImport struct {
	At       time.Time       `json:"at"`
	Agents   int             `json:"agents"`
	Versions int             `json:"versions"`
	Disabled []DisabledAgent `json:"disabled,omitempty"`
}

// DisabledAgent is a registry agent that was not moved, and why.
type DisabledAgent struct {
	Tenant string `json:"tenant"`
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// oldRevision is one stored revision of a registry agent.
type oldRevision struct {
	rev       int
	spec      []byte
	createdAt time.Time
	createdBy string
}

// oldAgent is one registry agent in one tenant, with every revision.
type oldAgent struct {
	tenant, name string
	deleted      bool
	revisions    []oldRevision
}

// ImportRegistry moves every agent of the registry's tables into this source
// once. Revision N becomes version N.0.0 through FromAgentSpec, keeping who
// wrote it and when. A deleted agent is retired after its versions move. An
// agent that cannot be moved (its name breaks saige's rule, it does not pass
// the deployment's checks, or one of its versions is already taken by other
// content) is left out and reported in Disabled; it runs nothing, so the move
// fails closed.
//
// Every write is idempotent and the marker is written last, so a move cut
// short is finished by the next call. Once marked, later calls return the
// kept report and read nothing. A database without the registry's tables
// moves nothing and is not marked.
func (p *Postgres) ImportRegistry(ctx context.Context) (RegistryImport, error) {
	if done, ok, err := p.registryMarker(ctx); err != nil || ok {
		return done, err
	}
	old, present, err := p.loadRegistry(ctx)
	if err != nil || !present {
		return RegistryImport{}, err
	}
	report := RegistryImport{At: p.clock().UTC()}
	for _, a := range old {
		n, err := p.importAgent(ctx, a)
		if err != nil {
			if !errors.Is(err, ErrInvalid) && !errors.Is(err, ErrExists) {
				return RegistryImport{}, fmt.Errorf("definition: import agent %q of %q: %w", a.name, a.tenant, err)
			}
			report.Disabled = append(report.Disabled, DisabledAgent{Tenant: a.tenant, Name: a.name, Reason: err.Error()})
			continue
		}
		report.Agents++
		report.Versions += n
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return RegistryImport{}, fmt.Errorf("definition: encode the import report: %w", err)
	}
	if _, err := p.pool.Exec(ctx, `INSERT INTO fiber_definition_imports (source, report) VALUES ($1, $2)
		ON CONFLICT (source) DO NOTHING`, registryMarker, raw); err != nil {
		return RegistryImport{}, fmt.Errorf("definition: mark the registry imported: %w", err)
	}
	return report, nil
}

// registryMarker returns the kept report, and whether the move has run.
func (p *Postgres) registryMarker(ctx context.Context) (RegistryImport, bool, error) {
	var raw []byte
	err := p.pool.QueryRow(ctx, `SELECT report FROM fiber_definition_imports WHERE source = $1`, registryMarker).Scan(&raw)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return RegistryImport{}, false, nil
	case err != nil:
		return RegistryImport{}, false, fmt.Errorf("definition: read the import marker: %w", err)
	}
	var done RegistryImport
	if err := json.Unmarshal(raw, &done); err != nil {
		return RegistryImport{}, false, fmt.Errorf("definition: read the import marker: %w", err)
	}
	return done, true, nil
}

// loadRegistry reads the registry's tables, renaming them from before the
// rename first. present is false when they do not exist.
func (p *Postgres) loadRegistry(ctx context.Context) (agents []oldAgent, present bool, err error) {
	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(migrateLock)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, pgrename.SQL("agents", "agent_revisions")); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx,
			`SELECT to_regclass('fiber_agents') IS NOT NULL AND to_regclass('fiber_agent_revisions') IS NOT NULL`,
		).Scan(&present); err != nil || !present {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT a.tenant, a.name, a.deleted, r.rev, r.spec, r.created_at, r.created_by
			  FROM fiber_agents a
			  JOIN fiber_agent_revisions r ON r.tenant = a.tenant AND r.name = a.name
			 ORDER BY a.tenant, a.name, r.rev`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				a   oldAgent
				rev oldRevision
			)
			if err := rows.Scan(&a.tenant, &a.name, &a.deleted, &rev.rev, &rev.spec, &rev.createdAt, &rev.createdBy); err != nil {
				return err
			}
			if n := len(agents); n > 0 && agents[n-1].tenant == a.tenant && agents[n-1].name == a.name {
				agents[n-1].revisions = append(agents[n-1].revisions, rev)
				continue
			}
			a.revisions = []oldRevision{rev}
			agents = append(agents, a)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, false, fmt.Errorf("definition: read the registry: %w", err)
	}
	return agents, present, nil
}

// importAgent converts and validates every revision of a before it writes
// any, then writes them in revision order and retires a deleted agent. It
// returns how many versions it wrote.
func (p *Postgres) importAgent(ctx context.Context, a oldAgent) (int, error) {
	revs := slices.Clone(a.revisions)
	slices.SortStableFunc(revs, func(x, y oldRevision) int { return x.rev - y.rev })
	recs := make([]Record, 0, len(revs))
	for _, r := range revs {
		var spec AgentSpec
		if err := json.Unmarshal(r.spec, &spec); err != nil {
			return 0, fmt.Errorf("%w: revision %d: %v", ErrInvalid, r.rev, err)
		}
		rec, err := FromAgentSpec(spec, r.rev)
		if err != nil {
			return 0, err
		}
		if rec.Name != NormalizeName(a.name) {
			return 0, fmt.Errorf("%w: revision %d names %q", ErrInvalid, r.rev, rec.Name)
		}
		rec.Tenant, rec.CreatedBy = a.tenant, r.createdBy
		if rec, err = p.prepare(rec); err != nil {
			return 0, err
		}
		rec.CreatedAt = r.createdAt.UTC()
		recs = append(recs, rec)
	}
	for _, rec := range recs {
		if _, err := p.put(ctx, rec); err != nil {
			return 0, err
		}
	}
	if a.deleted && len(recs) > 0 {
		if err := p.Retire(ctx, a.tenant, recs[0].Name, RegistryImportedBy); err != nil {
			return 0, err
		}
	}
	return len(recs), nil
}
