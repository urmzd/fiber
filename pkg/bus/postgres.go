package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/fiber/pkg/event"
)

// pgSchema is everything the Postgres bus owns. The tables of the agent-keyed
// bus it replaced (fiber_bus_runs, fiber_bus_events) are not touched or read.
//
// fiber_event_runs holds each run's high-water mark and the time of its last
// event, which the sweep ages runs out by.
//
// fiber_events is the retained window. id is the append order across every
// stream and the cursor every subscriber reads by. Publishes serialize on one
// advisory lock per schema (see fiber_event_publish), so an id is assigned and
// committed before the next one is, and a reader scanning "id > cursor" can
// never step over a row that commits late. A run event (seq > 0) is unique by
// (tenant, run, seq); every other event (seq = 0) by (tenant, event_id).
const pgSchema = `
CREATE TABLE IF NOT EXISTS fiber_event_runs (
	tenant     text        NOT NULL,
	run        text        NOT NULL,
	top        bigint      NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (tenant, run)
);

CREATE TABLE IF NOT EXISTS fiber_events (
	id       bigserial   PRIMARY KEY,
	tenant   text        NOT NULL,
	channel  text        NOT NULL,
	type     text        NOT NULL,
	run      text        NOT NULL DEFAULT '',
	seq      bigint      NOT NULL DEFAULT 0,
	event_id text        NOT NULL,
	body     text        NOT NULL,
	at       timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS fiber_events_run_seq ON fiber_events (tenant, run, seq) WHERE seq > 0;
CREATE UNIQUE INDEX IF NOT EXISTS fiber_events_event_id ON fiber_events (tenant, event_id) WHERE seq = 0;
CREATE INDEX IF NOT EXISTS fiber_events_channel ON fiber_events (tenant, channel, id);
CREATE INDEX IF NOT EXISTS fiber_events_run ON fiber_events (tenant, run, id);
CREATE INDEX IF NOT EXISTS fiber_events_at ON fiber_events (at);

CREATE OR REPLACE FUNCTION fiber_event_publish(p_tenant text, p_channel text, p_type text, p_run text, p_seq bigint, p_event_id text, p_body text, p_retention int)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
	v_top bigint;
BEGIN
	PERFORM pg_advisory_xact_lock(hashtextextended('fiber_events:' || current_schema(), 0));
	IF p_seq > 0 THEN
		SELECT r.top INTO v_top FROM fiber_event_runs r WHERE r.tenant = p_tenant AND r.run = p_run;
		IF v_top IS NOT NULL AND p_seq <= v_top THEN
			RETURN false;
		END IF;
		INSERT INTO fiber_events (tenant, channel, type, run, seq, event_id, body)
			VALUES (p_tenant, p_channel, p_type, p_run, p_seq, p_event_id, p_body);
		INSERT INTO fiber_event_runs (tenant, run, top, updated_at) VALUES (p_tenant, p_run, p_seq, now())
			ON CONFLICT (tenant, run) DO UPDATE SET top = EXCLUDED.top, updated_at = EXCLUDED.updated_at;
		DELETE FROM fiber_events e
			WHERE e.tenant = p_tenant AND e.run = p_run AND e.seq > 0
			AND e.seq < (SELECT x.seq FROM fiber_events x
				WHERE x.tenant = p_tenant AND x.run = p_run AND x.seq > 0
				ORDER BY x.seq DESC OFFSET p_retention - 1 LIMIT 1);
	ELSE
		INSERT INTO fiber_events (tenant, channel, type, run, seq, event_id, body)
			VALUES (p_tenant, p_channel, p_type, p_run, 0, p_event_id, p_body)
			ON CONFLICT (tenant, event_id) WHERE seq = 0 DO NOTHING;
		IF NOT FOUND THEN
			RETURN false;
		END IF;
		DELETE FROM fiber_events e
			WHERE e.tenant = p_tenant AND e.channel = p_channel AND e.seq = 0
			AND e.id < (SELECT x.id FROM fiber_events x
				WHERE x.tenant = p_tenant AND x.channel = p_channel AND x.seq = 0
				ORDER BY x.id DESC OFFSET p_retention - 1 LIMIT 1);
	END IF;
	PERFORM pg_notify('fiber_events', p_tenant);
	RETURN true;
END
$$;
`

// pgMigrateLock is the advisory lock Migrate holds, the one every fiber store
// takes, so two processes starting together do not race on the catalog.
const pgMigrateLock = 0x6d616e64

// pgChannel is the LISTEN/NOTIFY channel publishes wake readers on.
const pgChannel = "fiber_events"

// listeningPollFactor stretches a reader's safety poll while the LISTEN
// connection is healthy.
const listeningPollFactor = 20

// Postgres is a bus on two tables in the database a deployment already has.
//
// Sequencing, idempotent replay, dedupe and retention happen inside one
// publish function, under one advisory lock per schema, so publishes are
// serialized across every stream.
//
// Readers are one goroutine per subscription. Each scans fiber_events by id,
// narrowed in SQL to its tenant, channels and run, then waits for a NOTIFY on
// its tenant (one LISTEN connection per bus, held while anything is
// subscribed) or the poll interval.
//
// Events are durable rows until retention trims them or a sweep finds them
// older than WithIndexTTL (24 hours by default). Those rows reach the
// database's WAL, replicas and backups like any other.
//
// It does not own the pool. Call Migrate once before first use.
type Postgres struct {
	pool      *pgxpool.Pool
	retention int
	backlog   int
	poll      time.Duration
	ttl       time.Duration
	opts      options
	hub       *pgHub

	mu    sync.Mutex
	swept time.Time
}

var _ Bus = (*Postgres)(nil)

// NewPostgres returns a bus on pool. It honours WithRetention, WithBacklog,
// WithPollInterval, WithIndexTTL and WithMaxHops, and does not touch the
// database.
func NewPostgres(pool *pgxpool.Pool, opts ...Option) *Postgres {
	o := newOptions(opts)
	return &Postgres{
		pool:      pool,
		retention: o.retention,
		backlog:   o.backlog,
		poll:      o.poll,
		ttl:       o.indexTTL,
		opts:      o,
		hub:       &pgHub{pool: pool, retry: o.poll},
		swept:     time.Now(),
	}
}

// Migrate creates the tables and the publish function. It is idempotent and
// safe to run from every replica at startup.
func (p *Postgres) Migrate(ctx context.Context) error {
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(pgMigrateLock)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, pgSchema)
		return err
	})
	if err != nil {
		return fmt.Errorf("bus: migrate: %w", err)
	}
	return nil
}

// Publish implements Bus.
func (p *Postgres) Publish(ctx context.Context, e event.Envelope) error {
	if err := normalize(&e, p.opts); err != nil {
		return err
	}
	if e.Seq > uint64(1<<63-1) {
		return fmt.Errorf("%w: sequence %d does not fit in a bigint", ErrInvalid, e.Seq)
	}
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("bus: encode %s %q: %w", e.Type, e.ID, err)
	}
	var stored bool
	err = p.pool.QueryRow(ctx, `SELECT fiber_event_publish($1, $2, $3, $4, $5, $6, $7, $8)`,
		e.Tenant, e.Channel, e.Type, e.Run, int64(e.Seq), e.ID, string(body), p.retention).Scan(&stored)
	if err != nil {
		return fmt.Errorf("bus: publish %s %q on %s: %w", e.Type, e.ID, e.Channel, err)
	}
	p.sweep(ctx)
	return nil
}

// sweep forgets runs and events older than the TTL. A failure is ignored:
// the next sweep tries again.
func (p *Postgres) sweep(ctx context.Context) {
	p.mu.Lock()
	due := time.Since(p.swept) >= p.ttl/4
	if due {
		p.swept = time.Now()
	}
	p.mu.Unlock()
	if !due {
		return
	}
	cutoff := time.Now().Add(-p.ttl)
	_, _ = p.pool.Exec(ctx, `DELETE FROM fiber_events WHERE at < $1`, cutoff)
	_, _ = p.pool.Exec(ctx, `DELETE FROM fiber_event_runs WHERE updated_at < $1`, cutoff)
}

// Subscribe implements Bus.
func (p *Postgres) Subscribe(ctx context.Context, sub Subscription) (Stream, error) {
	if err := validate(sub); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sub.From > uint64(1<<63-1) {
		return nil, fmt.Errorf("%w: cursor %d does not fit in a bigint", ErrInvalid, sub.From)
	}
	readCtx, cancel := context.WithCancel(ctx)
	s := &pgStream{
		p:      p,
		sub:    sub,
		ch:     make(chan event.Envelope, p.backlog),
		cancel: cancel,
		closed: make(chan struct{}),
		done:   make(chan struct{}),
	}
	if sub.matchesNothing() {
		go func() {
			defer close(s.done)
			<-readCtx.Done()
			s.err = ErrClosed
		}()
	} else {
		wake, unsubscribe := p.hub.subscribe(sub.Tenant)
		go s.read(readCtx, wake, unsubscribe)
	}
	if done := ctx.Done(); done != nil {
		go func() {
			select {
			case <-done:
				_ = s.Close()
			case <-s.closed:
			}
		}()
	}
	return s, nil
}

// pgStream is one subscription: a reader goroutine filling a bounded buffer.
type pgStream struct {
	p   *Postgres
	sub Subscription
	ch  chan event.Envelope

	once   sync.Once
	cancel context.CancelFunc
	closed chan struct{}
	done   chan struct{}
	err    error
}

var _ Stream = (*pgStream)(nil)

// Recv implements Stream.
func (s *pgStream) Recv(ctx context.Context) (event.Envelope, error) {
	return recv(ctx, s.ch, s.closed, s.done, func() error { return s.err })
}

// Close implements Stream. It waits for the reader, so nothing outlives it.
func (s *pgStream) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.cancel()
	})
	<-s.done
	return nil
}

// read scans the events table from the start of what is retained and then
// keeps scanning as rows arrive.
func (s *pgStream) read(ctx context.Context, wake <-chan struct{}, unsubscribe func()) {
	defer close(s.done)
	defer unsubscribe()

	var after int64
	for {
		if err := ctx.Err(); err != nil {
			s.err = ErrClosed
			return
		}
		n, err := s.batch(ctx, &after)
		if err != nil {
			if ctx.Err() != nil {
				s.err = ErrClosed
			} else {
				s.err = err
			}
			return
		}
		if n == readBatch {
			continue
		}
		wait := s.p.poll
		if s.p.hub.listening.Load() {
			wait *= listeningPollFactor
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
		case <-wake:
			t.Stop()
		case <-t.C:
		}
	}
}

// pgQuery narrows the scan as far as SQL can: the tenant, a channel prefix,
// the run, and the cursor over run sequences. Accepts decides the rest.
var pgQuery = `SELECT id, body FROM fiber_events
WHERE id > $1 AND tenant = $2
AND (seq = 0 OR seq > $3)
AND ($4 = '' OR run = $4)
AND EXISTS (SELECT 1 FROM unnest($5::text[]) AS p(prefix) WHERE starts_with(channel, p.prefix))
ORDER BY id LIMIT ` + strconv.Itoa(readBatch)

// batch reads one page after the cursor and hands what the subscription
// accepts to the consumer, advancing the cursor past every row it read.
func (s *pgStream) batch(ctx context.Context, after *int64) (int, error) {
	rows, err := s.p.pool.Query(ctx, pgQuery, *after, s.sub.Tenant, int64(s.sub.From), s.sub.Run, s.sub.Channels)
	if err != nil {
		return 0, fmt.Errorf("bus: read %s: %w", s.sub.Tenant, err)
	}
	type row struct {
		id   int64
		e    event.Envelope
		keep bool
	}
	var page []row
	for rows.Next() {
		var (
			id   int64
			body string
		)
		if err := rows.Scan(&id, &body); err != nil {
			rows.Close()
			return 0, fmt.Errorf("bus: read %s: %w", s.sub.Tenant, err)
		}
		var e event.Envelope
		if err := json.Unmarshal([]byte(body), &e); err != nil {
			rows.Close()
			return 0, fmt.Errorf("bus: decode row %d: %w", id, err)
		}
		page = append(page, row{id: id, e: e, keep: s.sub.Accepts(e)})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("bus: read %s: %w", s.sub.Tenant, err)
	}
	for _, r := range page {
		*after = r.id
		if !r.keep {
			continue
		}
		select {
		case s.ch <- r.e:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return len(page), nil
}

// pgHub multiplexes one LISTEN connection across every live reader of a bus.
// It listens only while something is subscribed. A notification's payload is
// the tenant that was published to, so it wakes only that tenant's readers.
// Every (re)connect wakes every reader.
type pgHub struct {
	pool  *pgxpool.Pool
	retry time.Duration

	listening atomic.Bool

	mu     sync.Mutex
	subs   map[string]map[chan struct{}]struct{}
	count  int
	cancel context.CancelFunc
}

// subscribe registers a reader of tenant and returns its wake channel and the
// function that removes it.
func (h *pgHub) subscribe(tenant string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	if h.subs == nil {
		h.subs = map[string]map[chan struct{}]struct{}{}
	}
	if h.subs[tenant] == nil {
		h.subs[tenant] = map[chan struct{}]struct{}{}
	}
	h.subs[tenant][ch] = struct{}{}
	h.count++
	if h.count == 1 {
		ctx, cancel := context.WithCancel(context.Background())
		h.cancel = cancel
		go h.listen(ctx)
	}
	h.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			delete(h.subs[tenant], ch)
			if len(h.subs[tenant]) == 0 {
				delete(h.subs, tenant)
			}
			h.count--
			if h.count == 0 && h.cancel != nil {
				h.cancel()
				h.cancel = nil
			}
		})
	}
}

// wake signals the readers of the tenant a publish concerns. An empty
// payload wakes everyone.
func (h *pgHub) wake(tenant string) {
	if strings.TrimSpace(tenant) == "" {
		h.wakeAll()
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs[tenant] {
		signal(ch)
	}
}

func (h *pgHub) wakeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, set := range h.subs {
		for ch := range set {
			signal(ch)
		}
	}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (h *pgHub) listen(ctx context.Context) {
	for ctx.Err() == nil {
		h.session(ctx)
		h.listening.Store(false)
		t := time.NewTimer(h.retry)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// session holds one LISTEN connection until it fails or ctx ends.
func (h *pgHub) session(ctx context.Context) {
	pc, err := h.pool.Acquire(ctx)
	if err != nil {
		return
	}
	conn := pc.Hijack()
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, "LISTEN "+pgChannel); err != nil {
		return
	}
	h.listening.Store(true)
	h.wakeAll()
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return
		}
		h.wake(n.Payload)
	}
}
