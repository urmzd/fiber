package toolprovider_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

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
