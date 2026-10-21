package knowledge_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/urmzd/fiber/pkg/knowledge"
	"github.com/urmzd/fiber/pkg/knowledge/knowledgetest"
)

// The Store contract, against every backend this package has: Memory always,
// Postgres when $FIBER_POSTGRES_TEST_URL names one.
func TestMemoryConformance(t *testing.T) {
	knowledgetest.Run(t, func(_ *testing.T, e knowledge.Embedder, opts ...knowledge.Option) knowledge.Store {
		return knowledge.NewMemory(e, opts...)
	})
}

func TestPostgresConformance(t *testing.T) {
	url := os.Getenv(postgresEnv)
	if url == "" {
		t.Skipf("set %s to run the knowledge suite against Postgres", postgresEnv)
	}
	knowledgetest.Run(t, func(t *testing.T, e knowledge.Embedder, opts ...knowledge.Option) knowledge.Store {
		return newPostgres(t, url, e, opts...)
	})
}

const tenantA, tenantB = "acme", "globex"

// newStore returns an empty store that embeds with embedder.
//
// Which store is decided by the environment, not by the test: every property
// in this file is a property of the Store contract, so the suite runs unchanged
// against Postgres when $FIBER_POSTGRES_TEST_URL names one (see
// postgres_test.go) and against Memory otherwise.
func newStore(t *testing.T, embedder knowledge.Embedder, opts ...knowledge.Option) knowledge.Store {
	t.Helper()
	if url := os.Getenv(postgresEnv); url != "" {
		return newPostgres(t, url, embedder, opts...)
	}
	return knowledge.NewMemory(embedder, opts...)
}

// axes is an embedder whose geometry a test controls: each keyword owns one
// dimension, and a text points along the dimensions of the keywords it
// contains. Two texts are similar exactly when they share a keyword, whatever
// else they say, which lets a test build a passage the dense arm finds and the
// lexical arm cannot.
type axes struct {
	keywords []string
	calls    atomic.Int64
	fail     atomic.Bool
}

func (a *axes) Dimensions() int { return len(a.keywords) }

func (a *axes) Model() string { return "axes:" + strings.Join(a.keywords, ",") }

func (a *axes) Embed(_ context.Context, texts []string) ([][]float32, error) {
	a.calls.Add(1)
	if a.fail.Load() {
		return nil, errors.New("embedding service is down")
	}
	out := make([][]float32, len(texts))
	for i, text := range texts {
		v := make([]float32, len(a.keywords))
		for j, k := range a.keywords {
			if strings.Contains(strings.ToLower(text), k) {
				v[j] = 1
			}
		}
		out[i] = v
	}
	return out, nil
}

func mustUpsert(t *testing.T, s knowledge.Store, tenant string, doc knowledge.Document) knowledge.Receipt {
	t.Helper()
	r, err := s.Upsert(context.Background(), tenant, doc)
	if err != nil {
		t.Fatalf("Upsert(%s, %s): %v", tenant, doc.URI, err)
	}
	return r
}

func mustSearch(t *testing.T, s knowledge.Store, tenant string, q knowledge.Query) []knowledge.Hit {
	t.Helper()
	hits, err := s.Search(context.Background(), tenant, q)
	if err != nil {
		t.Fatalf("Search(%s, %q): %v", tenant, q.Text, err)
	}
	return hits
}

const handbook = `# Security handbook

Everything an engineer needs to know about credentials.

## Rotating API keys

Rotate an API key every ninety days. Open the console, choose Credentials,
and press Rotate. The old key keeps working for one hour.

## Reporting an incident

Page the on-call engineer and open a ticket in the incident tracker.
`

// synonyms is axes with several keywords per dimension.
type synonyms struct{ groups [][]string }

func (s *synonyms) Dimensions() int { return len(s.groups) }

func (s *synonyms) Model() string { return "synonyms" }

func (s *synonyms) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		v := make([]float32, len(s.groups))
		for j, g := range s.groups {
			for _, k := range g {
				if strings.Contains(strings.ToLower(text), k) {
					v[j] = 1
				}
			}
		}
		out[i] = v
	}
	return out, nil
}
