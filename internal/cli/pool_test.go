package cli

import (
	goruntime "runtime"
	"testing"
)

// Every run this process executes holds its control subscriptions for its
// whole turn, each a pooled connection on Redis: one for stop requests, and
// one for requests to compact when the deployment takes them. The pool must
// reserve that many per concurrent run on top of go-redis's default, or a
// publish waits behind subscriptions and the turn fails. A pool_size the URL
// sets is the operator's and is used as given.
func TestTheRedisPoolReservesEveryControlStreamOfEveryConcurrentRun(t *testing.T) {
	base := 10 * goruntime.GOMAXPROCS(0)
	tests := []struct {
		name        string
		url         string
		fromURL     int
		concurrency int
		cfg         coreConfig
		want        int
	}{
		{"one run, stop only", "redis://localhost:6379", 0, 0, coreConfig{}, base + 1},
		{"eight runs, stop only", "redis://localhost:6379", 0, 8, coreConfig{}, base + 8},
		{"eight runs, stop and compact", "redis://localhost:6379", 0, 8, coreConfig{CompactOnRequest: true}, base + 16},
		{"the URL decides", "redis://localhost:6379/?pool_size=7", 7, 8, coreConfig{CompactOnRequest: true}, 7},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := redisPoolSize(tc.url, tc.fromURL, tc.concurrency, controlStreams(tc.cfg))
			if got != tc.want {
				t.Errorf("pool size = %d, want %d", got, tc.want)
			}
		})
	}
}
