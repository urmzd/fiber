package bus_test

import (
	"errors"
	"os"
	"testing"

	"github.com/urmzd/fiber/pkg/bus"
)

// A URL is the whole choice of backend: its scheme picks one, and a scheme
// nothing implements is refused rather than quietly kept in memory.
func TestOpenPicksTheBackendByScheme(t *testing.T) {
	for _, tc := range []struct {
		url  string
		want string
		env  string
	}{
		{url: "", want: "*bus.Memory"},
		{url: "memory:", want: "*bus.Memory"},
		{url: "redis://" + os.Getenv(addrEnv), want: "*bus.Redis", env: addrEnv},
		{url: os.Getenv(postgresEnv), want: "*bus.Postgres", env: postgresEnv},
	} {
		if tc.env != "" && os.Getenv(tc.env) == "" {
			continue
		}
		b, closeFn, err := bus.Open(t.Context(), tc.url, bus.WithPoolSize(4))
		if err != nil {
			t.Fatalf("Open(%q): %v", tc.url, err)
		}
		if got := typeName(b); got != tc.want {
			t.Errorf("Open(%q) = %s, want %s", tc.url, got, tc.want)
		}
		if err := closeFn(); err != nil {
			t.Errorf("close %q: %v", tc.url, err)
		}
	}
	for _, url := range []string{"kafka://broker:9092", "redis://[::1", "postgres://%zz"} {
		if _, _, err := bus.Open(t.Context(), url); !errors.Is(err, bus.ErrInvalid) {
			t.Errorf("Open(%q) = %v, want bus.ErrInvalid", url, err)
		}
	}
}

func typeName(b bus.Bus) string {
	switch b.(type) {
	case *bus.Memory:
		return "*bus.Memory"
	case *bus.Redis:
		return "*bus.Redis"
	case *bus.Postgres:
		return "*bus.Postgres"
	}
	return "unknown"
}
