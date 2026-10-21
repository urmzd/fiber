// Package knowledgetest is the conformance suite every knowledge.Store must
// pass: exact citations, idempotent and atomic reindexing, tenant and source
// confinement, deletion, hybrid fusion, reranking, limits, stable order, and
// refusing input or vectors the store cannot keep. Memory and Postgres both
// run it; a new backend is a Factory.
package knowledgetest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/urmzd/fiber/pkg/knowledge"
)

const tenantA, tenantB = "acme", "globex"

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

func uris(hits []knowledge.Hit) []string {
	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = h.URI
	}
	return out
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

type broken struct {
	dims  int
	embed func(n int) [][]float32
}

func (b broken) Dimensions() int { return b.dims }

func (b broken) Model() string { return "broken" }

func (b broken) Embed(_ context.Context, texts []string) ([][]float32, error) {
	return b.embed(len(texts)), nil
}

func repeat(n int, v []float32) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func nan() float64 {
	var zero float64
	return zero / zero
}

// Factory returns a fresh, empty store that embeds with embedder and applies
// opts. It must isolate each call: no store may see another's documents.
type Factory func(t *testing.T, embedder knowledge.Embedder, opts ...knowledge.Option) knowledge.Store

// Run exercises a Store implementation against the full contract.
func Run(t *testing.T, factory Factory) {
	newStore := func(t *testing.T, embedder knowledge.Embedder, opts ...knowledge.Option) knowledge.Store {
		t.Helper()
		return factory(t, embedder, opts...)
	}

	// The point of the package: a hit is not just a passage but a passage with an
	// address. The text must be findable at the cited lines of the cited document.
	t.Run("AHitCitesTheExactTextAtItsLocator", func(t *testing.T) {
		t.Parallel()
		s := newStore(t, knowledge.NewHashing(0), knowledge.WithChunking(160, 0))
		receipt := mustUpsert(t, s, tenantA, knowledge.Document{
			Source:   "handbook",
			URI:      "https://docs.example.com/security.md",
			Title:    "Security handbook",
			Content:  handbook,
			Metadata: map[string]string{"owner": "secops"},
		})
		if !receipt.Changed || receipt.Chunks < 3 {
			t.Fatalf("receipt = %+v; want a changed document in at least 3 chunks", receipt)
		}

		hits := mustSearch(t, s, tenantA, knowledge.Query{Text: "how do I rotate an API key?"})
		if len(hits) == 0 {
			t.Fatal("no hits for a question the handbook answers")
		}
		top := hits[0]
		if !strings.Contains(top.Text, "Rotate an API key every ninety days") {
			t.Errorf("top hit is %q; want the passage about rotation", top.Text)
		}
		if top.Source != "handbook" || top.URI != "https://docs.example.com/security.md" || top.Title != "Security handbook" {
			t.Errorf("top hit provenance = %q %q %q", top.Source, top.URI, top.Title)
		}
		if top.Metadata["owner"] != "secops" {
			t.Errorf("metadata = %v; want the document's", top.Metadata)
		}
		if top.Locator.Heading != "Rotating API keys" {
			t.Errorf("heading = %q; want the section the passage is in", top.Locator.Heading)
		}
		for _, h := range hits {
			l := h.Locator
			if handbook[l.Start:l.End] != h.Text {
				t.Errorf("chunk %d: content[%d:%d] is not the hit's text", l.Chunk, l.Start, l.End)
			}
			lines := strings.Split(handbook, "\n")
			cited := strings.Join(lines[l.StartLine-1:l.EndLine], "\n")
			if !strings.Contains(cited, strings.TrimSpace(h.Text)) {
				t.Errorf("chunk %d: lines %d to %d do not contain the hit's text", l.Chunk, l.StartLine, l.EndLine)
			}
		}
	})

	// A crawler resubmits everything on every pass. Unchanged content must cost
	// neither a write nor an embedding request.
	t.Run("ReindexingUnchangedContentDoesNotEmbedOrWrite", func(t *testing.T) {
		t.Parallel()
		e := &axes{keywords: []string{"alpha", "beta"}}
		s := newStore(t, e)
		doc := knowledge.Document{Source: "kb", URI: "a", Title: "A", Content: "alpha text", Metadata: map[string]string{"k": "v"}}

		first := mustUpsert(t, s, tenantA, doc)
		calls := e.calls.Load()
		second := mustUpsert(t, s, tenantA, doc)

		if !first.Changed || second.Changed {
			t.Errorf("Changed = %v then %v; want true then false", first.Changed, second.Changed)
		}
		if second.Hash != first.Hash || second.Chunks != first.Chunks {
			t.Errorf("no-op receipt %+v differs from the original %+v", second, first)
		}
		if got := e.calls.Load(); got != calls {
			t.Errorf("the embedder was called %d more times for unchanged content", got-calls)
		}

		for name, changed := range map[string]knowledge.Document{
			"content":  {Source: "kb", URI: "a", Title: "A", Content: "alpha text, edited", Metadata: map[string]string{"k": "v"}},
			"title":    {Source: "kb", URI: "a", Title: "B", Content: "alpha text, edited", Metadata: map[string]string{"k": "v"}},
			"metadata": {Source: "kb", URI: "a", Title: "B", Content: "alpha text, edited", Metadata: map[string]string{"k": "w"}},
		} {
			if r := mustUpsert(t, s, tenantA, changed); !r.Changed {
				t.Errorf("a changed %s was treated as a no-op", name)
			}
		}
	})

	// Replacing a document replaces all of it. A chunk of the old version that the
	// new version has no counterpart for must not linger and be cited.
	t.Run("ReindexingChangedContentLeavesNoOldChunkBehind", func(t *testing.T) {
		t.Parallel()
		s := newStore(t, knowledge.NewHashing(0), knowledge.WithChunking(80, 0))
		long := strings.Repeat("The legacy procedure mentions zeppelins.\n\n", 12)
		if r := mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "proc", Content: long}); r.Chunks < 6 {
			t.Fatalf("setup: old version has %d chunks; want several", r.Chunks)
		}
		r := mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "proc", Content: "The current procedure mentions submarines."})
		if !r.Changed || r.Chunks != 1 {
			t.Fatalf("receipt = %+v; want one changed chunk", r)
		}

		for _, query := range []string{"zeppelins", "legacy procedure", "submarines"} {
			for _, h := range mustSearch(t, s, tenantA, knowledge.Query{Text: query, Limit: 50}) {
				if h.Locator.Chunk != 0 || !strings.Contains(h.Text, "submarines") {
					t.Errorf("%q found chunk %d of the replaced version: %q", query, h.Locator.Chunk, h.Text)
				}
			}
		}
		if hits := mustSearch(t, s, tenantA, knowledge.Query{Text: "submarines", Limit: 50}); len(hits) != 1 {
			t.Errorf("hits = %+v; want exactly the new version's single chunk", hits)
		}
	})

	// A re-index that cannot finish must not be half applied: the version that
	// was searchable before is the version that is searchable after.
	t.Run("AFailedReindexLeavesThePreviousVersionSearchable", func(t *testing.T) {
		t.Parallel()
		e := &axes{keywords: []string{"alpha", "beta"}}
		s := newStore(t, e)
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "a", Content: "alpha original"})

		e.fail.Store(true)
		_, err := s.Upsert(context.Background(), tenantA, knowledge.Document{Source: "kb", URI: "a", Content: "beta replacement"})
		if !errors.Is(err, knowledge.ErrEmbedding) {
			t.Fatalf("Upsert with a failing embedder = %v; want ErrEmbedding", err)
		}
		e.fail.Store(false)

		hits := mustSearch(t, s, tenantA, knowledge.Query{Text: "alpha original"})
		if len(hits) != 1 || hits[0].Text != "alpha original" {
			t.Errorf("hits = %+v; want the original version intact", hits)
		}
		if hits := mustSearch(t, s, tenantA, knowledge.Query{Text: "replacement"}); len(hits) != 0 {
			t.Errorf("part of the failed version is searchable: %+v", hits)
		}
	})

	// Atomic means a reader never sees two versions of one document at once, even
	// while it is being rewritten. Every chunk names its version, so a mixture is
	// visible in a single result set.
	t.Run("ASearchNeverSeesTwoVersionsOfOneDocument", func(t *testing.T) {
		t.Parallel()
		s := newStore(t, knowledge.NewHashing(0), knowledge.WithChunking(60, 0))
		version := func(name string) knowledge.Document {
			var b strings.Builder
			for i := range 8 {
				fmt.Fprintf(&b, "Paragraph %d of the %s edition of the manual.\n\n", i, name)
			}
			return knowledge.Document{Source: "kb", URI: "manual", Content: b.String()}
		}
		mustUpsert(t, s, tenantA, version("crimson"))

		ctx := context.Background()
		stop := make(chan struct{})
		var writers sync.WaitGroup
		for w := range 2 {
			writers.Add(1)
			go func() {
				defer writers.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					name := []string{"crimson", "indigo"}[(i+w)%2]
					if _, err := s.Upsert(ctx, tenantA, version(name)); err != nil {
						t.Errorf("Upsert: %v", err)
						return
					}
				}
			}()
		}
		for range 60 {
			hits, err := s.Search(ctx, tenantA, knowledge.Query{Text: "paragraph of the manual", Limit: 50})
			if err != nil {
				t.Errorf("Search: %v", err)
				break
			}
			seen := map[string]bool{}
			for _, h := range hits {
				seen["crimson"] = seen["crimson"] || strings.Contains(h.Text, "crimson")
				seen["indigo"] = seen["indigo"] || strings.Contains(h.Text, "indigo")
			}
			if len(hits) == 0 || (seen["crimson"] && seen["indigo"]) {
				t.Errorf("a search saw %d hits, crimson=%v indigo=%v; want all of exactly one version", len(hits), seen["crimson"], seen["indigo"])
				break
			}
		}
		close(stop)
		writers.Wait()
	})

	// The tenant is a boundary, not a filter: same source, same URI, nothing
	// shared, and no way to learn that the other tenant's document exists.
	t.Run("TenantsShareNothing", func(t *testing.T) {
		t.Parallel()
		s := newStore(t, knowledge.NewHashing(0))
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "plan", Content: "Acme will acquire a lighthouse."})
		mustUpsert(t, s, tenantB, knowledge.Document{Source: "kb", URI: "plan", Content: "Globex will acquire a windmill."})

		for tenant, want := range map[string]string{tenantA: "lighthouse", tenantB: "windmill"} {
			hits := mustSearch(t, s, tenant, knowledge.Query{Text: "what will be acquired, a lighthouse or a windmill?", Limit: 50})
			if len(hits) != 1 || !strings.Contains(hits[0].Text, want) {
				t.Errorf("tenant %s sees %+v; want only its own plan", tenant, hits)
			}
		}

		if err := s.Delete(context.Background(), tenantA, "kb", "plan"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if hits := mustSearch(t, s, tenantB, knowledge.Query{Text: "windmill"}); len(hits) != 1 {
			t.Errorf("deleting tenant A's document removed tenant B's: %+v", hits)
		}
		if err := s.Delete(context.Background(), "initech", "kb", "plan"); !errors.Is(err, knowledge.ErrNotFound) {
			t.Errorf("deleting another tenant's document = %v; want ErrNotFound", err)
		}
	})

	// An agent is granted named corpora. A query scoped to some sources must not
	// return a passage from another, however well it matches.
	t.Run("SearchIsConfinedToTheSourcesItNames", func(t *testing.T) {
		t.Parallel()
		s := newStore(t, knowledge.NewHashing(0))
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "public", URI: "faq", Content: "The office opens at nine."})
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "hr", URI: "pay", Content: "The office salary bands are confidential."})
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "eng", URI: "run", Content: "The office VPN runbook."})

		cases := []struct {
			name    string
			sources []string
			want    []string
		}{
			{"one source", []string{"public"}, []string{"faq"}},
			{"two sources", []string{"eng", "public"}, []string{"faq", "run"}},
			{"a source with no documents", []string{"legal"}, nil},
			{"no filter is every source", nil, []string{"faq", "pay", "run"}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				hits := mustSearch(t, s, tenantA, knowledge.Query{Text: "office", Sources: tc.sources, Limit: 50})
				got := map[string]bool{}
				for _, h := range hits {
					got[h.URI] = true
				}
				if len(got) != len(tc.want) {
					t.Fatalf("hits = %v; want %v", uris(hits), tc.want)
				}
				for _, w := range tc.want {
					if !got[w] {
						t.Errorf("hits = %v; missing %s", uris(hits), w)
					}
				}
			})
		}
	})

	t.Run("ADeletedDocumentIsNoLongerSearchable", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		s := newStore(t, knowledge.NewHashing(0), knowledge.WithChunking(40, 0))
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "gone", Content: "Flamingos stand.\n\nFlamingos wade.\n\nFlamingos fly."})
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "kept", Content: "Herons stand."})

		if err := s.Delete(ctx, tenantA, "kb", "gone"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		for _, h := range mustSearch(t, s, tenantA, knowledge.Query{Text: "flamingos stand", Limit: 50}) {
			if h.URI == "gone" {
				t.Errorf("chunk %d of a deleted document is searchable", h.Locator.Chunk)
			}
		}
		if hits := mustSearch(t, s, tenantA, knowledge.Query{Text: "herons"}); len(hits) != 1 {
			t.Errorf("deleting one document disturbed another: %+v", hits)
		}
		if err := s.Delete(ctx, tenantA, "kb", "gone"); !errors.Is(err, knowledge.ErrNotFound) {
			t.Errorf("deleting twice = %v; want ErrNotFound", err)
		}
		// The name is free again, and indexing under it starts clean.
		if r := mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "gone", Content: "Flamingos stand.\n\nFlamingos wade.\n\nFlamingos fly."}); !r.Changed {
			t.Error("re-indexing a deleted document was treated as unchanged")
		}
	})

	// Why the search is hybrid. One passage only the dense arm can find, one only
	// the lexical arm can find, and one both agree on: all three come back, the
	// agreed one first, and each hit says which arm found it.
	t.Run("FusionSurfacesWhatEitherArmFindsAndPrefersWhatBothFind", func(t *testing.T) {
		t.Parallel()
		// "refund" and "reimburse" share a dimension: to this embedder they
		// mean the same thing, though they share no word.
		e := &synonyms{groups: [][]string{{"refund", "reimburse"}, {"invoice"}, {"weather"}}}
		s := newStore(t, e)
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "dense-only", Content: "We reimburse within a week."})
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "lexical-only", Content: "Form 7781 covers travel."})
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "both", Content: "A refund needs form 7781."})
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "neither", Content: "The weather is mild."})

		hits := mustSearch(t, s, tenantA, knowledge.Query{Text: "refund 7781"})
		if got := uris(hits); len(got) != 3 || got[0] != "both" {
			t.Fatalf("hits = %v; want three with the passage both arms found first", got)
		}
		by := map[string]knowledge.Scores{}
		for i, h := range hits {
			by[h.URI] = h.Scores
			if i > 0 && hits[i-1].Scores.Fused < h.Scores.Fused {
				t.Errorf("hits are not ordered by fused score: %v", hits)
			}
			if h.Scores.Reranked {
				t.Errorf("%s is marked reranked with no reranker configured", h.URI)
			}
		}
		if sc := by["both"]; sc.DenseRank == 0 || sc.LexicalRank == 0 || sc.Dense <= 0 || sc.Lexical <= 0 {
			t.Errorf("both = %+v; want a rank and a score from each arm", sc)
		}
		if sc := by["dense-only"]; sc.DenseRank == 0 || sc.LexicalRank != 0 || sc.Lexical != 0 {
			t.Errorf("dense-only = %+v; want the dense arm alone", sc)
		}
		if sc := by["lexical-only"]; sc.LexicalRank == 0 || sc.DenseRank != 0 || sc.Dense != 0 {
			t.Errorf("lexical-only = %+v; want the lexical arm alone", sc)
		}
		want := 1.0/float64(knowledge.RRFK+by["both"].DenseRank) + 1.0/float64(knowledge.RRFK+by["both"].LexicalRank)
		if got := by["both"].Fused; got < want-1e-12 || got > want+1e-12 {
			t.Errorf("fused score = %v; want reciprocal rank fusion's %v", got, want)
		}
	})

	// A reranker has the last word on the candidates it is shown, and a reranker
	// that fails fails the search instead of silently handing back another order.
	t.Run("ARerankerDecidesTheFinalOrder", func(t *testing.T) {
		t.Parallel()
		var failing atomic.Bool
		var sawQuery atomic.Value
		prefer := knowledge.RerankerFunc(func(_ context.Context, query string, passages []string) ([]float64, error) {
			if failing.Load() {
				return nil, errors.New("rerank service is down")
			}
			sawQuery.Store(query)
			scores := make([]float64, len(passages))
			for i, p := range passages {
				if strings.Contains(p, "appendix") {
					scores[i] = 10
				}
			}
			return scores, nil
		})
		s := newStore(t, knowledge.NewHashing(0), knowledge.WithReranker(prefer, 10))
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "strong", Content: "Deploy deploy deploy: the deploy guide for every deploy."})
		mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: "weak", Content: "See the appendix for how to deploy on the rare occasions it is needed at all."})

		hits := mustSearch(t, s, tenantA, knowledge.Query{Text: "deploy"})
		if got := uris(hits); len(got) != 2 || got[0] != "weak" {
			t.Fatalf("hits = %v; want the reranker's choice first", got)
		}
		if hits[0].Scores.Fused >= hits[1].Scores.Fused {
			t.Fatalf("setup: fusion already preferred %s, so the reranker changed nothing", hits[0].URI)
		}
		for _, h := range hits {
			if !h.Scores.Reranked {
				t.Errorf("%s was returned without a rerank score", h.URI)
			}
		}
		if hits[0].Scores.Rerank != 10 || hits[1].Scores.Rerank != 0 {
			t.Errorf("rerank scores = %v, %v; want the reranker's 10 and 0", hits[0].Scores.Rerank, hits[1].Scores.Rerank)
		}
		if sawQuery.Load() != "deploy" {
			t.Errorf("the reranker was given the query %q", sawQuery.Load())
		}

		failing.Store(true)
		if _, err := s.Search(context.Background(), tenantA, knowledge.Query{Text: "deploy"}); err == nil {
			t.Error("a failing reranker did not fail the search")
		}
	})

	t.Run("LimitBoundsTheResult", func(t *testing.T) {
		t.Parallel()
		s := newStore(t, knowledge.NewHashing(0))
		for i := range 12 {
			mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: fmt.Sprintf("doc-%02d", i), Content: fmt.Sprintf("Entry %d about penguins.", i)})
		}
		for _, tc := range []struct{ limit, want int }{{0, knowledge.DefaultLimit}, {3, 3}, {12, 12}, {1000, 12}} {
			if hits := mustSearch(t, s, tenantA, knowledge.Query{Text: "penguins", Limit: tc.limit}); len(hits) != tc.want {
				t.Errorf("Limit %d returned %d hits; want %d", tc.limit, len(hits), tc.want)
			}
		}
		if hits := mustSearch(t, s, "nobody", knowledge.Query{Text: "penguins"}); hits == nil || len(hits) != 0 {
			t.Errorf("no match = %#v; want an empty, non-nil slice", hits)
		}
	})

	// Ties are broken the same way everywhere, so the same index answers the same
	// query with the same order on every call and on every backend.
	t.Run("EquallyGoodHitsComeBackInAStableOrder", func(t *testing.T) {
		t.Parallel()
		s := newStore(t, knowledge.NewHashing(0))
		for _, uri := range []string{"c", "a", "B", "b"} {
			mustUpsert(t, s, tenantA, knowledge.Document{Source: "kb", URI: uri, Content: "Identical sentence about otters."})
		}
		want := []string{"B", "a", "b", "c"} // byte order, as Postgres's "C" collation and Go agree
		for range 3 {
			if got := uris(mustSearch(t, s, tenantA, knowledge.Query{Text: "otters"})); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("order = %v; want %v", got, want)
			}
		}
	})

	t.Run("InvalidInputIsRefusedBeforeAnythingIsWritten", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		e := &axes{keywords: []string{"alpha"}}
		s := newStore(t, e)
		good := knowledge.Document{Source: "kb", URI: "a", Content: "alpha"}

		docs := map[string]struct {
			tenant string
			edit   func(*knowledge.Document)
		}{
			"no tenant":            {"", func(*knowledge.Document) {}},
			"blank tenant":         {"  ", func(*knowledge.Document) {}},
			"no source":            {tenantA, func(d *knowledge.Document) { d.Source = " " }},
			"no uri":               {tenantA, func(d *knowledge.Document) { d.URI = "" }},
			"no content":           {tenantA, func(d *knowledge.Document) { d.Content = " \n\t\n" }},
			"content with NUL":     {tenantA, func(d *knowledge.Document) { d.Content = "alpha\x00beta" }},
			"content not UTF-8":    {tenantA, func(d *knowledge.Document) { d.Content = "alpha\xff" }},
			"metadata with NUL":    {tenantA, func(d *knowledge.Document) { d.Metadata = map[string]string{"k": "\x00"} }},
			"content over the cap": {tenantA, func(d *knowledge.Document) { d.Content = strings.Repeat("a ", knowledge.MaxDocumentBytes/2+1) }},
		}
		for name, tc := range docs {
			doc := good
			tc.edit(&doc)
			if _, err := s.Upsert(ctx, tc.tenant, doc); !errors.Is(err, knowledge.ErrInvalid) {
				t.Errorf("Upsert with %s = %v; want ErrInvalid", name, err)
			}
		}
		if e.calls.Load() != 0 {
			t.Errorf("the embedder was called %d times for documents that were refused", e.calls.Load())
		}

		queries := map[string]struct {
			tenant string
			q      knowledge.Query
		}{
			"no tenant":         {"", knowledge.Query{Text: "alpha"}},
			"empty text":        {tenantA, knowledge.Query{Text: "  "}},
			"blank source name": {tenantA, knowledge.Query{Text: "alpha", Sources: []string{"kb", " "}}},
		}
		for name, tc := range queries {
			if _, err := s.Search(ctx, tc.tenant, tc.q); !errors.Is(err, knowledge.ErrInvalid) {
				t.Errorf("Search with %s = %v; want ErrInvalid", name, err)
			}
		}
		for name, args := range map[string][3]string{
			"no tenant": {"", "kb", "a"}, "no source": {tenantA, "", "a"}, "no uri": {tenantA, "kb", ""},
		} {
			if err := s.Delete(ctx, args[0], args[1], args[2]); !errors.Is(err, knowledge.ErrInvalid) {
				t.Errorf("Delete with %s = %v; want ErrInvalid", name, err)
			}
		}

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := s.Upsert(cancelled, tenantA, good); !errors.Is(err, context.Canceled) {
			t.Errorf("Upsert on a cancelled context = %v; want context.Canceled", err)
		}
	})

	// An embedder is somebody else's service. Vectors of the wrong shape must be
	// refused at the door, not stored and discovered by a later search.
	t.Run("VectorsTheStoreCannotUseAreRefused", func(t *testing.T) {
		t.Parallel()
		for name, e := range map[string]knowledge.Embedder{
			"too few vectors": broken{dims: 2, embed: func(n int) [][]float32 { return make([][]float32, 0) }},
			"wrong width":     broken{dims: 2, embed: func(n int) [][]float32 { return repeat(n, []float32{1, 0, 0}) }},
			"not a number":    broken{dims: 2, embed: func(n int) [][]float32 { return repeat(n, []float32{float32(nan()), 0}) }},
		} {
			s := newStore(t, e)
			if _, err := s.Upsert(context.Background(), tenantA, knowledge.Document{Source: "kb", URI: "a", Content: "alpha"}); !errors.Is(err, knowledge.ErrEmbedding) {
				t.Errorf("Upsert with %s = %v; want ErrEmbedding", name, err)
			}
		}
	})
}
