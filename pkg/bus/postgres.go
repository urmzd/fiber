package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/topic"
)

// pgSchema is everything the Postgres bus owns.
//
// fiber_bus_runs holds each run's high-water mark, the one fact a publish
// must read before it writes, and the time of its last event, which is what
// the sweep ages runs out by.
//
// fiber_bus_events is the retained window. id is the append order across
// every run and the cursor every subscriber reads by. Publishes serialize on
// one advisory lock per schema (see fiber_bus_publish), so an id is assigned
// and committed before the next one is, and a reader scanning "id > cursor"
// can never step over a row that commits late. That is the whole catch-up and
// live-tail story: both are the same scan.
//
// fiber_bus_publish is a function so that a publish is one round trip and one
// transaction: take the lock, read the mark, decide, insert, advance, trim
// the run to its retention depth, and notify.
const pgSchema = `
CREATE TABLE IF NOT EXISTS fiber_bus_runs (
	topic      text        PRIMARY KEY,
	top        bigint      NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fiber_bus_events (
	id    bigserial   PRIMARY KEY,
	topic text        NOT NULL,
	agent text        NOT NULL,
	seq   bigint      NOT NULL,
	body  text        NOT NULL,
	at    timestamptz NOT NULL DEFAULT now(),
	UNIQUE (topic, seq)
);

CREATE INDEX IF NOT EXISTS fiber_bus_events_agent ON fiber_bus_events (agent, id);
CREATE INDEX IF NOT EXISTS fiber_bus_events_topic ON fiber_bus_events (topic, id);
CREATE INDEX IF NOT EXISTS fiber_bus_events_at ON fiber_bus_events (at);

CREATE OR REPLACE FUNCTION fiber_bus_publish(p_topic text, p_agent text, p_seq bigint, p_body text, p_retention int)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
	v_top bigint;
	v_seq bigint := p_seq;
BEGIN
	PERFORM pg_advisory_xact_lock(hashtextextended('fiber_bus:' || current_schema(), 0));
	SELECT r.top INTO v_top FROM fiber_bus_runs r WHERE r.topic = p_topic;
	IF v_top IS NULL THEN
		v_top := 0;
	END IF;
	IF v_seq = 0 THEN
		v_seq := v_top + 1;
	ELSIF v_seq <= v_top THEN
		RETURN v_seq;
	END IF;
	INSERT INTO fiber_bus_events (topic, agent, seq, body) VALUES (p_topic, p_agent, v_seq, p_body);
	INSERT INTO fiber_bus_runs (topic, top, updated_at) VALUES (p_topic, v_seq, now())
		ON CONFLICT (topic) DO UPDATE SET top = EXCLUDED.top, updated_at = EXCLUDED.updated_at;
	DELETE FROM fiber_bus_events e
		WHERE e.topic = p_topic
		AND e.seq < (SELECT x.seq FROM fiber_bus_events x WHERE x.topic = p_topic
			ORDER BY x.seq DESC OFFSET p_retention - 1 LIMIT 1);
	PERFORM pg_notify('fiber_bus', p_topic);
	RETURN v_seq;
END
$$;
`

// pgMigrateLock is the advisory lock Migrate holds, the one every fiber store
// takes, so two processes starting together do not race on the catalog.
const pgMigrateLock = 0x6d616e64

// pgChannel is the LISTEN/NOTIFY channel publishes wake readers on.
const pgChannel = "fiber_bus"

// listeningPollFactor stretches a reader's safety poll while the LISTEN
// connection is healthy: notifications do the waking, and the poll only
// covers one lost between reconnects.
const listeningPollFactor = 20

// Postgres is a bus on two tables in the database a deployment already has,
// so a multi-process deployment can stream without Redis.
//
// It keeps the contract bustest holds every backend to. Sequencing,
// idempotent replay and per-run retention happen inside one publish
// function, under one advisory lock per schema. That lock is the price of
// the design: publishes are serialized across every run, so throughput is
// bounded by one short Postgres transaction per event. It is the right
// trade for a deployment that would rather not run Redis, and the wrong one
// for a very chatty one.
//
// Readers are one goroutine per subscription, like Redis's. Each scans the
// events table by id, from the start of what is retained, filtering on the
// subscription's topic and From, then waits for a NOTIFY (one LISTEN
// connection per bus, held while anything is subscribed) or the poll
// interval. A reader holds a pooled connection only while it queries. A full
// buffer stops the reader, which is backpressure onto the table: like Redis,
// a consumer slow enough to fall behind retention loses events to the trim
// rather than being terminated.
//
// Runs are forgotten WithIndexTTL after their last event, by a sweep a
// publish runs at most every quarter of that.
//
// Unlike Redis's window, this one is durable: every event the bus carries,
// body included, is a row in fiber_bus_events in the database it is given,
// until its run's retention trim drops it or a sweep finds it older than
// WithIndexTTL (24 hours by default; with no publishes, nothing is swept).
// Those rows reach that database's WAL, replicas and backups like any other,
// and tool events carry their call's arguments (up to 8 KiB). Where those can
// hold secrets, scrub them before they are published
// (agentrt.WithToolArguments), shorten WithIndexTTL, or use Redis.
//
// It does not own the pool. Call Migrate once before first use.
type Postgres struct {
	pool      *pgxpool.Pool
	retention int
	backlog   int
	poll      time.Duration
	ttl       time.Duration
	hub       *pgHub

	mu    sync.Mutex
	swept time.Time
}

var _ Bus = (*Postgres)(nil)

// NewPostgres returns a bus on pool. It honours WithRetention, WithBacklog,
// WithPollInterval and WithIndexTTL, and does not touch the database.
func NewPostgres(pool *pgxpool.Pool, opts ...Option) *Postgres {
	o := newOptions(opts)
	return &Postgres{
		pool:      pool,
		retention: o.retention,
		backlog:   o.backlog,
		poll:      o.poll,
		ttl:       o.indexTTL,
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
func (p *Postgres) Publish(ctx context.Context, e envelope.Event) error {
	if err := normalize(&e); err != nil {
		return err
	}
	if e.Seq > uint64(1<<63-1) {
		return fmt.Errorf("%w: sequence %d does not fit in a bigint", ErrInvalid, e.Seq)
	}
	body, err := encodeEvent(e)
	if err != nil {
		return fmt.Errorf("bus: encode event on %s: %w", e.Topic, err)
	}
	var seq int64
	err = p.pool.QueryRow(ctx, `SELECT fiber_bus_publish($1, $2, $3, $4, $5)`,
		e.Topic.String(), e.Topic.Name(), int64(e.Seq), body, p.retention).Scan(&seq)
	if err != nil {
		return fmt.Errorf("bus: publish to %s: %w", e.Topic, err)
	}
	p.sweep(ctx)
	return nil
}

// sweep forgets runs and events older than the TTL. A failure is ignored:
// the next sweep tries again, and nothing correct depends on it.
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
	_, _ = p.pool.Exec(ctx, `DELETE FROM fiber_bus_events WHERE at < $1`, cutoff)
	_, _ = p.pool.Exec(ctx, `DELETE FROM fiber_bus_runs WHERE updated_at < $1`, cutoff)
}

// Subscribe implements Bus.
func (p *Postgres) Subscribe(ctx context.Context, sub envelope.Subscription) (Stream, error) {
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
		ch:     make(chan envelope.Event, p.backlog),
		cancel: cancel,
		closed: make(chan struct{}),
		done:   make(chan struct{}),
	}
	wake, unsubscribe := p.hub.subscribe()
	go s.read(readCtx, wake, unsubscribe)
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
// Its Recv and Close are redisStream's, for the same reasons.
type pgStream struct {
	p   *Postgres
	sub envelope.Subscription
	ch  chan envelope.Event

	once   sync.Once
	cancel context.CancelFunc
	closed chan struct{}
	done   chan struct{}
	err    error
}

var _ Stream = (*pgStream)(nil)

// Recv implements Stream.
func (s *pgStream) Recv(ctx context.Context) (envelope.Event, error) {
	for {
		if err := ctx.Err(); err != nil {
			return envelope.Event{}, err
		}
		select {
		case <-s.closed:
			return envelope.Event{}, ErrClosed
		default:
		}
		select {
		case e := <-s.ch:
			return e, nil
		default:
		}
		select {
		case <-s.done:
			if s.err == nil {
				return envelope.Event{}, ErrClosed
			}
			return envelope.Event{}, s.err
		default:
		}
		select {
		case e := <-s.ch:
			return e, nil
		case <-s.done:
		case <-s.closed:
		case <-ctx.Done():
			return envelope.Event{}, ctx.Err()
		}
	}
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

	query, arg := s.query()
	var after int64
	for {
		if err := ctx.Err(); err != nil {
			s.err = ErrClosed
			return
		}
		n, err := s.batch(ctx, query, arg, &after)
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

// query narrows the scan as far as SQL can: one run, or one agent. The
// root's scan is every row, and Covers keeps the control namespace out.
func (s *pgStream) query() (string, string) {
	const cols = `SELECT id, topic, seq, body FROM fiber_bus_events WHERE id > $1 AND seq > $2`
	tail := ` ORDER BY id LIMIT ` + strconv.Itoa(readBatch)
	switch {
	case s.sub.Topic.RunID() != "":
		return cols + ` AND topic = $3` + tail, s.sub.Topic.String()
	case s.sub.Topic.Name() != "":
		return cols + ` AND agent = $3` + tail, s.sub.Topic.Name()
	default:
		return cols + ` AND $3 = ''` + tail, ""
	}
}

// batch reads one page after the cursor and hands what the subscription
// accepts to the consumer, advancing the cursor past every row it read.
func (s *pgStream) batch(ctx context.Context, query, arg string, after *int64) (int, error) {
	rows, err := s.p.pool.Query(ctx, query, *after, int64(s.sub.From), arg)
	if err != nil {
		return 0, fmt.Errorf("bus: read %s: %w", s.sub.Topic, err)
	}
	type row struct {
		id   int64
		e    envelope.Event
		keep bool
	}
	var page []row
	for rows.Next() {
		var (
			id, seq    int64
			name, body string
		)
		if err := rows.Scan(&id, &name, &seq, &body); err != nil {
			rows.Close()
			return 0, fmt.Errorf("bus: read %s: %w", s.sub.Topic, err)
		}
		e, err := decodeRow(name, seq, body)
		if err != nil {
			rows.Close()
			return 0, fmt.Errorf("bus: decode %s#%d: %w", name, seq, err)
		}
		page = append(page, row{id: id, e: e, keep: s.sub.Accepts(e)})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("bus: read %s: %w", s.sub.Topic, err)
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

func decodeRow(name string, seq int64, body string) (envelope.Event, error) {
	t, err := topic.Parse(name)
	if err != nil {
		return envelope.Event{}, err
	}
	var w wireEvent
	if err := json.Unmarshal([]byte(body), &w); err != nil {
		return envelope.Event{}, err
	}
	e := w.Event
	e.Topic = t
	e.Seq = uint64(seq)
	return e, nil
}

// pgHub multiplexes one LISTEN connection across every live reader of a bus.
// It listens only while something is subscribed, and every (re)connect wakes
// every reader, so a notification lost in a gap costs one extra query.
type pgHub struct {
	pool  *pgxpool.Pool
	retry time.Duration

	listening atomic.Bool

	mu     sync.Mutex
	subs   map[chan struct{}]struct{}
	cancel context.CancelFunc
}

func (h *pgHub) subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	if h.subs == nil {
		h.subs = map[chan struct{}]struct{}{}
	}
	h.subs[ch] = struct{}{}
	if len(h.subs) == 1 {
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
			delete(h.subs, ch)
			if len(h.subs) == 0 && h.cancel != nil {
				h.cancel()
				h.cancel = nil
			}
		})
	}
}

func (h *pgHub) wakeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
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

// session holds one LISTEN connection until it fails or ctx ends. The
// connection is taken out of the pool, so a LISTEN never leaks into a
// connection someone else borrows.
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
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return
		}
		h.wakeAll()
	}
}
