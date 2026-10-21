package registry_test

import (
	"context"
	"os"
	"testing"

	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/registry"
	"github.com/urmzd/fiber/pkg/registry/registrytest"
)

// The Store contract, against every backend this package has: Memory always,
// Postgres when $FIBER_POSTGRES_TEST_URL names one.
func TestMemoryConformance(t *testing.T) {
	registrytest.Run(t, func(_ *testing.T, opts ...registry.Option) registry.Store {
		return registry.NewMemory(opts...)
	})
}

func TestPostgresConformance(t *testing.T) {
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("set %s to run the registry suite against Postgres", postgresEnv)
	}
	registrytest.Run(t, func(t *testing.T, opts ...registry.Option) registry.Store {
		return newPostgres(t, url, opts...)
	})
}

const tenantA, tenantB = "acme", "globex"

// spec returns a valid definition whose content is a function of prompt, so a
// test can produce "the same definition" or "a different one" on demand.
func spec(name, prompt string) agentspec.AgentSpec {
	return agentspec.AgentSpec{
		Name:         name,
		Description:  "answers questions",
		Model:        "claude-haiku-5-5",
		Provider:     agentspec.ProviderOffline,
		SystemPrompt: prompt,
		Tools:        []string{"search"},
		Sources:      []agentspec.Source{{Name: "handbook", URI: "s3://h", Options: map[string]string{"k": "v"}}},
		Access:       agentspec.Access{Spawn: []string{"linter"}, WorkspaceAreas: []string{"docs/"}},
	}
}

func mustCreate(t *testing.T, s registry.Store, tenant, name, prompt string) agentspec.Revision {
	t.Helper()
	rev, err := s.Create(context.Background(), tenant, spec(name, prompt), "ada")
	if err != nil {
		t.Fatalf("Create(%s/%s): %v", tenant, name, err)
	}
	return rev
}

func mustRevise(t *testing.T, s registry.Store, tenant, name, prompt string, expected int) agentspec.Revision {
	t.Helper()
	rev, err := s.Revise(context.Background(), tenant, spec(name, prompt), expected, "grace")
	if err != nil {
		t.Fatalf("Revise(%s/%s): %v", tenant, name, err)
	}
	return rev
}
