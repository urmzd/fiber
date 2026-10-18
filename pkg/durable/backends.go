package durable

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urmzd/anchor/adapters/postgres/pgledger"
	"github.com/urmzd/anchor/adapters/postgres/pgqueue"
)

// Open migrates the schema and returns anchor's Postgres ledger and queue
// mapped onto it, both validated against the live database.
func Open(ctx context.Context, pool *pgxpool.Pool, opts ...pgqueue.Option) (*pgledger.Ledger, *pgqueue.Queue, error) {
	if err := Migrate(ctx, pool); err != nil {
		return nil, nil, err
	}

	lm := pgledger.DefaultMapping()
	lm.Runs.Table = RunsTable
	lm.Records.Table = RecordsTable
	lgr, err := pgledger.New(pool, lm)
	if err != nil {
		return nil, nil, fmt.Errorf("durable: ledger: %w", err)
	}
	if err := lgr.Validate(ctx); err != nil {
		return nil, nil, fmt.Errorf("durable: ledger: %w", err)
	}

	qm := pgqueue.DefaultMapping()
	qm.Table = QueueTable
	q, err := pgqueue.New(pool, qm, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("durable: queue: %w", err)
	}
	if err := q.Validate(ctx); err != nil {
		return nil, nil, fmt.Errorf("durable: queue: %w", err)
	}
	return lgr, q, nil
}
