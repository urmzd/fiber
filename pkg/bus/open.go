package bus

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Open returns the bus a URL names, picking the backend by scheme, and a
// function that releases whatever Open opened:
//
//	""  or "memory:"            Memory
//	redis://, rediss://         Redis Streams
//	postgres://, postgresql://  Postgres (migrated before Open returns)
//
// It is how a deployment chooses a backend with a line of configuration
// rather than code. A caller that already holds a client or a pool builds the
// backend itself with NewRedis or NewPostgres.
//
// opts are passed to the backend. WithPoolSize sizes the connection pool
// Open creates, unless the URL sets one itself (pool_size for Redis,
// pool_max_conns for Postgres).
func Open(ctx context.Context, rawURL string, opts ...Option) (Bus, func() error, error) {
	raw := strings.TrimSpace(rawURL)
	nop := func() error { return nil }
	scheme := ""
	if raw != "" {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: bus URL: %v", ErrInvalid, err)
		}
		scheme = strings.ToLower(u.Scheme)
	}
	o := newOptions(opts)
	switch scheme {
	case "", "memory":
		return NewMemory(opts...), nop, nil
	case "redis", "rediss":
		ro, err := redis.ParseURL(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: redis URL: %v", ErrInvalid, err)
		}
		if o.poolSize > 0 && !strings.Contains(raw, "pool_size=") {
			ro.PoolSize = o.poolSize
		}
		client := redis.NewClient(ro)
		return NewRedis(client, opts...), client.Close, nil
	case "postgres", "postgresql":
		cfg, err := pgxpool.ParseConfig(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: postgres URL: %v", ErrInvalid, err)
		}
		if o.poolSize > 0 && !strings.Contains(raw, "pool_max_conns=") {
			cfg.MaxConns = int32(min(o.poolSize, 1<<30))
		}
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("bus: postgres: %w", err)
		}
		b := NewPostgres(pool, opts...)
		if err := b.Migrate(ctx); err != nil {
			pool.Close()
			return nil, nil, err
		}
		return b, func() error { pool.Close(); return nil }, nil
	default:
		return nil, nil, fmt.Errorf("%w: bus URL scheme %q; want memory:, redis:// or postgres://", ErrInvalid, scheme)
	}
}
