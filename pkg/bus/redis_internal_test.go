package bus

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/urmzd/fiber/pkg/event"
)

// A stored entry that does not decode is skipped, and the stream moves past
// it to the next event instead of failing.
func TestRedisSkipsAnEntryThatDoesNotDecode(t *testing.T) {
	addr := os.Getenv("FIBER_REDIS_ADDR")
	if addr == "" {
		t.Skip("set FIBER_REDIS_ADDR")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	prefix := fmt.Sprintf("fibertest:skip:%d", time.Now().UnixNano())
	r := NewRedis(client, WithKeyPrefix(prefix), WithPollInterval(25*time.Millisecond))
	t.Cleanup(func() {
		keys, _ := client.Keys(context.Background(), prefix+"*").Result()
		if len(keys) > 0 {
			client.Del(context.Background(), keys...)
		}
	})

	good := event.Envelope{
		ID: "run_1#2", Source: "fiber://run/run_1", Type: event.TypeText,
		Channel: "slack://T01/C02", Tenant: "acme", Run: "run_1", Seq: 2,
		Data: event.Text{Text: "hi"},
	}
	key := r.streamKey(good)
	if err := client.XAdd(t.Context(), &redis.XAddArgs{Stream: key, ID: "1-0", Values: map[string]any{fieldEvent: bad}}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.index(t.Context(), "acme", indexMember(key, good.Channel, good.Run)); err != nil {
		t.Fatal(err)
	}
	if err := r.Publish(t.Context(), good); err != nil {
		t.Fatal(err)
	}
	s, err := r.Subscribe(t.Context(), Subscription{Tenant: "acme", Channels: []string{"slack://"}, Types: []string{"run"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	e, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv = %v; want the entry after the broken one", err)
	}
	if e.Seq != 2 {
		t.Fatalf("got seq %d, want 2", e.Seq)
	}
}
