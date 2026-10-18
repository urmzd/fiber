package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urmzd/anchor/pkg/run"
)

// Records reads a run's records by key prefix, from the tables Open maps the
// ledger onto. It exists because anchor's ledger reads a run's records only
// all at once, and some of them are large (a parked turn's transcript): a
// poll that wants one small record should not read every other one.
type Records struct {
	pool *pgxpool.Pool
}

// NewRecords returns a reader over the records table. Pass the pool Open was
// given.
func NewRecords(pool *pgxpool.Pool) *Records { return &Records{pool: pool} }

// RecordsWithPrefix returns the records of runID whose key starts with one of
// prefixes, in no particular order.
func (r *Records) RecordsWithPrefix(ctx context.Context, runID string, prefixes ...string) ([]run.Record, error) {
	if r == nil || r.pool == nil {
		return nil, ErrNoPool
	}
	if len(prefixes) == 0 {
		return nil, nil
	}
	patterns := make([]string, len(prefixes))
	for i, p := range prefixes {
		patterns[i] = likeEscape(p) + "%"
	}
	rows, err := r.pool.Query(ctx,
		`SELECT record_key, envelope FROM `+RecordsTable+` WHERE run_id = $1 AND record_key LIKE ANY($2)`,
		runID, patterns)
	if err != nil {
		return nil, fmt.Errorf("durable: records of %s: %w", runID, err)
	}
	defer rows.Close()
	var out []run.Record
	for rows.Next() {
		var key string
		var env []byte
		if err := rows.Scan(&key, &env); err != nil {
			return nil, fmt.Errorf("durable: records of %s: %w", runID, err)
		}
		var rec run.Record
		if err := json.Unmarshal(env, &rec); err != nil {
			return nil, fmt.Errorf("durable: record %s/%s: %w", runID, key, err)
		}
		rec.RunID, rec.Key = runID, key
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("durable: records of %s: %w", runID, err)
	}
	return out, nil
}

// likeEscape escapes LIKE's wildcards and its escape character.
func likeEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}
