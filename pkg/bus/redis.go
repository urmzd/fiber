package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/urmzd/fiber/pkg/event"
)

// readBatch bounds how many entries one XRANGE or XREAD asks for.
const readBatch = 128

// fieldEvent is the single stream field each entry carries.
const fieldEvent = "e"

// publishRunScript appends one run event to its run's stream. The entry ID is
// the sequence, "<seq>-0", so XADD's monotonic-ID rule is the run's
// monotonic-sequence rule: a sequence at or below the stream's top is a
// replay and is dropped. The stream's TTL is refreshed on every write.
var publishRunScript = redis.NewScript(`
local seq = tonumber(ARGV[1])
local top = 0
local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
if #last > 0 then
  top = tonumber(string.match(last[1][1], '^(%d+)'))
end
if seq <= top then
  return 0
end
redis.call('XADD', KEYS[1], 'MAXLEN', ARGV[2], seq .. '-0', '` + fieldEvent + `', ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return seq
`)

// publishOtherScript appends an event that is not a run event to its
// channel's stream, unless its id was already seen in the tenant.
var publishOtherScript = redis.NewScript(`
if not redis.call('SET', KEYS[2], '1', 'NX', 'PX', ARGV[3]) then
  return 0
end
redis.call('XADD', KEYS[1], 'MAXLEN', ARGV[1], '*', '` + fieldEvent + `', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`)

// Redis is a bus backed by Redis Streams: one stream per run, and one per
// channel for the events that are not run events, each trimmed to the
// retention depth.
//
// Every key of a tenant carries the hash tag {<tenant>}, so a publish and its
// dedupe record live in one slot under Redis Cluster. Each tenant has one
// discovery index, a sorted set of its streams and their channels scored by
// publish time. The index is a hint: a subscriber filters its members by its
// own channels and run, and every event it reads is checked against the
// subscription before delivery.
//
// Requires Redis 6.2 or later, for exclusive XRANGE ranges.
type Redis struct {
	client    redis.UniversalClient
	retention int
	backlog   int
	prefix    string
	poll      time.Duration
	indexTTL  time.Duration
	opts      options

	// mu guards indexed: when each stream was last written to its index.
	mu      sync.Mutex
	indexed map[string]time.Time
}

// indexCacheBound caps the index-refresh cache.
const indexCacheBound = 8192

var _ Bus = (*Redis)(nil)

// NewRedis returns a bus backed by client. It honours WithRetention,
// WithBacklog, WithKeyPrefix, WithPollInterval, WithIndexTTL and WithMaxHops.
// The client is borrowed, not owned.
func NewRedis(client redis.UniversalClient, opts ...Option) *Redis {
	o := newOptions(opts)
	return &Redis{
		client:    client,
		retention: o.retention,
		backlog:   o.backlog,
		prefix:    o.prefix,
		poll:      o.poll,
		indexTTL:  o.indexTTL,
		opts:      o,
		indexed:   make(map[string]time.Time),
	}
}

// tenantKey is the prefix of every key of one tenant.
func (r *Redis) tenantKey(tenant string) string {
	return r.prefix + ":{" + tenant + "}"
}

// streamKey is the key of the stream e is appended to.
func (r *Redis) streamKey(e event.Envelope) string {
	if isRunEvent(e.Type) {
		return r.tenantKey(e.Tenant) + ":run:" + e.Run
	}
	return r.tenantKey(e.Tenant) + ":channel:" + e.Channel
}

// indexKey is the key of a tenant's discovery index.
func (r *Redis) indexKey(tenant string) string { return r.tenantKey(tenant) + ":index" }

// indexMember is one index entry: the stream key, its channel, its run.
func indexMember(key, channel, run string) string {
	return key + "\x1f" + channel + "\x1f" + run
}

func parseIndexMember(m string) (key, channel, run string, ok bool) {
	parts := strings.Split(m, "\x1f")
	if len(parts) != 3 {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// Publish implements Bus.
func (r *Redis) Publish(ctx context.Context, e event.Envelope) error {
	if err := normalize(&e, r.opts); err != nil {
		return err
	}
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("bus: encode %s %q: %w", e.Type, e.ID, err)
	}
	key := r.streamKey(e)
	ttl := r.indexTTL.Milliseconds()
	if isRunEvent(e.Type) {
		err = publishRunScript.Run(ctx, r.client, []string{key}, e.Seq, r.retention, string(body), ttl).Err()
	} else {
		idKey := r.tenantKey(e.Tenant) + ":id:" + e.ID
		err = publishOtherScript.Run(ctx, r.client, []string{key, idKey}, r.retention, string(body), ttl).Err()
	}
	if err != nil {
		return fmt.Errorf("bus: publish %s %q on %s: %w", e.Type, e.ID, e.Channel, err)
	}
	run := ""
	if isRunEvent(e.Type) {
		run = e.Run
	}
	return r.index(ctx, e.Tenant, indexMember(key, e.Channel, run))
}

// index records a stream in its tenant's index. Throttled by the refresh
// cache: an active stream pays once per quarter of the index TTL.
func (r *Redis) index(ctx context.Context, tenant, member string) error {
	now := time.Now()
	r.mu.Lock()
	fresh := now.Sub(r.indexed[member]) < r.indexTTL/4
	r.mu.Unlock()
	if fresh {
		return nil
	}
	key := r.indexKey(tenant)
	cutoff := strconv.FormatInt(now.Add(-r.indexTTL).UnixMilli(), 10)
	pipe := r.client.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(now.UnixMilli()), Member: member})
	pipe.ZRemRangeByScore(ctx, key, "-inf", "("+cutoff)
	pipe.PExpire(ctx, key, r.indexTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("bus: index %s: %w", tenant, err)
	}
	r.mu.Lock()
	if len(r.indexed) >= indexCacheBound {
		r.indexed = make(map[string]time.Time)
	}
	r.indexed[member] = now
	r.mu.Unlock()
	return nil
}

// Subscribe implements Bus.
func (r *Redis) Subscribe(ctx context.Context, sub Subscription) (Stream, error) {
	if err := validate(sub); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	readCtx, cancel := context.WithCancel(ctx)
	s := &redisStream{
		r:      r,
		sub:    sub,
		ch:     make(chan event.Envelope, r.backlog),
		cancel: cancel,
		closed: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go s.read(readCtx)
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

// redisStream is one subscription: a reader goroutine filling a bounded
// buffer. A full buffer stops the reader, which is backpressure onto Redis.
type redisStream struct {
	r   *Redis
	sub Subscription
	ch  chan event.Envelope

	once   sync.Once
	cancel context.CancelFunc
	closed chan struct{}
	done   chan struct{}
	err    error
}

var _ Stream = (*redisStream)(nil)

// Recv implements Stream. Buffered events are handed over before any
// terminal error.
func (s *redisStream) Recv(ctx context.Context) (event.Envelope, error) {
	return recv(ctx, s.ch, s.closed, s.done, func() error { return s.err })
}

// recv is the Recv of a stream fed by a reader goroutine.
func recv(ctx context.Context, ch <-chan event.Envelope, closed, done <-chan struct{}, err func() error) (event.Envelope, error) {
	for {
		if e := ctx.Err(); e != nil {
			return event.Envelope{}, e
		}
		select {
		case <-closed:
			return event.Envelope{}, ErrClosed
		default:
		}
		select {
		case e := <-ch:
			return e, nil
		default:
		}
		select {
		case <-done:
			if err() == nil {
				return event.Envelope{}, ErrClosed
			}
			return event.Envelope{}, err()
		default:
		}
		select {
		case e := <-ch:
			return e, nil
		case <-done:
		case <-closed:
		case <-ctx.Done():
			return event.Envelope{}, ctx.Err()
		}
	}
}

// Close implements Stream. It waits for the reader to return.
func (s *redisStream) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.cancel()
	})
	<-s.done
	return nil
}

// read is the reader loop: discover the streams in scope, drain each from its
// cursor, and when everything is drained block once across all of them.
func (s *redisStream) read(ctx context.Context) {
	defer close(s.done)
	if s.sub.matchesNothing() {
		<-ctx.Done()
		s.err = ErrClosed
		return
	}
	cursors := make(map[string]string)
	var discovered time.Time
	for {
		if err := ctx.Err(); err != nil {
			s.terminate(ctx, err)
			return
		}
		if time.Since(discovered) >= s.r.poll {
			if err := s.discover(ctx, cursors); err != nil {
				s.terminate(ctx, err)
				return
			}
			discovered = time.Now()
		}
		n, err := s.catchUp(ctx, cursors)
		if err != nil {
			s.terminate(ctx, err)
			return
		}
		if n > 0 {
			continue
		}
		if err := s.tail(ctx, cursors); err != nil {
			s.terminate(ctx, err)
			return
		}
	}
}

// terminate records the error Recv reports once the buffer is drained. A
// cancelled reader context means the stream was closed.
func (s *redisStream) terminate(ctx context.Context, err error) {
	if ctx.Err() != nil {
		s.err = ErrClosed
		return
	}
	s.err = err
}

// discover adds the streams in scope that the reader has not seen yet. A run
// stream starts at the subscription's cursor; a channel stream at its start.
func (s *redisStream) discover(ctx context.Context, cursors map[string]string) error {
	cutoff := strconv.FormatInt(time.Now().Add(-s.r.indexTTL).UnixMilli(), 10)
	members, err := s.r.client.ZRangeByScore(ctx, s.r.indexKey(s.sub.Tenant), &redis.ZRangeBy{Min: cutoff, Max: "+inf"}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("bus: discover streams of %s: %w", s.sub.Tenant, err)
	}
	for _, m := range members {
		key, channel, run, ok := parseIndexMember(m)
		if !ok || !strings.HasPrefix(key, s.r.tenantKey(s.sub.Tenant)+":") {
			continue
		}
		if !slices.ContainsFunc(s.sub.Channels, func(p string) bool { return event.ChannelHasPrefix(channel, p) }) {
			continue
		}
		if _, seen := cursors[key]; seen {
			continue
		}
		if run != "" {
			if s.sub.Run != "" && run != s.sub.Run {
				continue
			}
			cursors[key] = strconv.FormatUint(s.sub.From, 10) + "-0"
		} else {
			cursors[key] = "0-0"
		}
	}
	return nil
}

// catchUp reads one batch of retained history per stream and reports how
// many entries it consumed.
func (s *redisStream) catchUp(ctx context.Context, cursors map[string]string) (int, error) {
	keys := sortedKeys(cursors)
	if len(keys) == 0 {
		return 0, nil
	}
	pipe := s.r.client.Pipeline()
	cmds := make([]*redis.XMessageSliceCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.XRangeN(ctx, key, "("+cursors[key], "+", readBatch)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return 0, fmt.Errorf("bus: read history of %s: %w", s.sub.Tenant, err)
	}
	total := 0
	for i, cmd := range cmds {
		msgs, err := cmd.Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return total, fmt.Errorf("bus: read history of %s: %w", keys[i], err)
		}
		n, err := s.emit(ctx, keys[i], msgs, cursors)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// tail blocks in one XREAD across every stream in scope, for at most the
// poll interval.
func (s *redisStream) tail(ctx context.Context, cursors map[string]string) error {
	keys := sortedKeys(cursors)
	if len(keys) == 0 {
		return sleep(ctx, s.r.poll)
	}
	args := make([]string, 0, 2*len(keys))
	args = append(args, keys...)
	for _, key := range keys {
		args = append(args, cursors[key])
	}
	streams, err := s.r.client.XRead(ctx, &redis.XReadArgs{Streams: args, Count: readBatch, Block: s.r.poll}).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return nil
	case err != nil:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("bus: tail %s: %w", s.sub.Tenant, err)
	}
	for _, st := range streams {
		if _, err := s.emit(ctx, st.Stream, st.Messages, cursors); err != nil {
			return err
		}
	}
	return nil
}

// emit decodes each entry, advances the cursor past it, and hands the ones
// the subscription accepts to the consumer.
func (s *redisStream) emit(ctx context.Context, key string, msgs []redis.XMessage, cursors map[string]string) (int, error) {
	for i, msg := range msgs {
		e, err := decodeEntry(msg)
		if err != nil {
			return i, fmt.Errorf("bus: decode %s entry %s: %w", key, msg.ID, err)
		}
		cursors[key] = msg.ID
		if !s.sub.Accepts(e) {
			continue
		}
		select {
		case s.ch <- e:
		case <-ctx.Done():
			return i, ctx.Err()
		}
	}
	return len(msgs), nil
}

func sortedKeys(cursors map[string]string) []string {
	keys := make([]string, 0, len(cursors))
	for key := range cursors {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// sleep waits for d, or returns early if ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func decodeEntry(msg redis.XMessage) (event.Envelope, error) {
	raw, ok := msg.Values[fieldEvent].(string)
	if !ok {
		return event.Envelope{}, fmt.Errorf("entry has no %q field", fieldEvent)
	}
	var e event.Envelope
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return event.Envelope{}, err
	}
	return e, nil
}
