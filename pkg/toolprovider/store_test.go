package toolprovider_test

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/toolprovider"
	"github.com/urmzd/fiber/pkg/toolprovider/toolprovidertest"
)

// The Store contract, against every backend this package has: Memory always,
// Postgres when $FIBER_POSTGRES_TEST_URL names one.
func TestMemoryConformance(t *testing.T) {
	toolprovidertest.Run(t, func(_ *testing.T, opts ...toolprovider.Option) toolprovider.Store {
		return toolprovider.NewMemory(opts...)
	})
}

func TestPostgresConformance(t *testing.T) {
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("set %s to run the tool provider suite against Postgres", postgresEnv)
	}
	toolprovidertest.Run(t, func(t *testing.T, opts ...toolprovider.Option) toolprovider.Store {
		return newPostgres(t, url, opts...)
	})
}

const (
	tenantA = "acme"
	tenantB = "globex"
)

// newStore returns the store under test. Every property here is a property
// of the Store contract, so the suite runs against Postgres when
// $FIBER_POSTGRES_TEST_URL is set (see postgres_test.go) and Memory otherwise.
func newStore(t *testing.T) toolprovider.Store {
	t.Helper()
	var mu sync.Mutex
	at := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	clock := toolprovider.WithClock(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		at = at.Add(time.Second)
		return at
	})
	if url := os.Getenv(postgresEnv); url != "" {
		return newPostgres(t, url, clock)
	}
	return toolprovider.NewMemory(clock)
}

func github(tenant string) toolprovider.Provider {
	return toolprovider.Provider{
		Tenant: tenant, Name: "github", Transport: toolprovider.TransportHTTP,
		Endpoint: "https://mcp.example.com/github", CredentialRef: "env:GITHUB_TOKEN", UpdatedBy: "admin",
	}
}

func tool(t *testing.T, name, desc, schema string) toolprovider.Tool {
	t.Helper()
	out, err := toolprovider.NewTool(name, desc, json.RawMessage(schema))
	if err != nil {
		t.Fatalf("NewTool(%q): %v", name, err)
	}
	return out
}

func mustPut(t *testing.T, s toolprovider.Store, p toolprovider.Provider) toolprovider.Provider {
	t.Helper()
	got, err := s.PutProvider(context.Background(), p)
	if err != nil {
		t.Fatalf("PutProvider(%s): %v", p.Name, err)
	}
	return got
}
