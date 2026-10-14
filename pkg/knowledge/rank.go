package knowledge

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode"
)

// tokenize lowercases text and returns its runs of letters and digits.
//
// It is the one definition of "a word" in this package. The hashing embedder,
// Memory's lexical index and the text Postgres is given to index all use it,
// so a term that matches in one backend is a term in the other. In particular
// Postgres never sees raw text: its own parser treats "docs.example.com" and
// "v1.2" as single tokens, and a query for "example" would then match in
// Memory and not in Postgres.
func tokenize(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

// stopwords are dropped from a query's lexical terms, never from a document.
//
// A question is mostly glue ("how do I rotate the key"), the lexical arm
// matches any term, and Postgres's ts_rank_cd has no notion of a term being
// common. Without this a passage that says "the" often enough outranks the one
// that says "rotate". The list is English and short on purpose: it is a guard
// against the worst case, not a language model, and the dense arm still sees
// the whole query.
var stopwords = func() map[string]struct{} {
	words := strings.Fields(`a an and are as at be but by can could did do does for from had has
		have how i if in into is it its me my of on or our should so than that the their them
		then there these they this to was we were what when where which who why will with would
		you your`)
	set := make(map[string]struct{}, len(words))
	for _, w := range words {
		set[w] = struct{}{}
	}
	return set
}()

// queryTerms returns the distinct terms the lexical arm searches for. A query
// made only of stopwords keeps them: "to be or not to be" is a legitimate
// thing to look up.
func queryTerms(text string) []string {
	all := tokenize(text)
	var kept []string
	seen := make(map[string]struct{}, len(all))
	for _, pass := range []bool{true, false} {
		for _, t := range all {
			if _, stop := stopwords[t]; stop && pass {
				continue
			}
			if _, dup := seen[t]; dup {
				continue
			}
			seen[t] = struct{}{}
			kept = append(kept, t)
		}
		if len(kept) > 0 {
			break
		}
	}
	return kept
}

// BM25 parameters, the usual ones.
const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

// bm25 scores one chunk against the query terms.
//
// docs is the number of chunks in scope and df how many of them contain each
// term. Both are taken over the chunks the query may see, so a term's rarity
// is measured within the tenant and sources being searched and never leaks
// what another tenant's corpus contains.
func bm25(tf map[string]int, length int, avgLength float64, docs int, df map[string]int, terms []string) float64 {
	var score float64
	for _, t := range terms {
		f := float64(tf[t])
		if f == 0 {
			continue
		}
		n := float64(df[t])
		idf := math.Log(1 + (float64(docs)-n+0.5)/(n+0.5))
		norm := 1.0
		if avgLength > 0 {
			norm = 1 - bm25B + bm25B*float64(length)/avgLength
		}
		score += idf * f * (bm25K1 + 1) / (f + bm25K1*norm)
	}
	return score
}

// minSimilarity is the cosine a chunk must exceed to be a dense candidate.
//
// A nearest-neighbour search always returns its k nearest, however far away
// they are, so without a floor a corpus of ten unrelated chunks answers every
// query with all ten. Zero is the natural floor: at or below it the two
// vectors share no direction at all. The margin absorbs float32 rounding, so
// the two backends agree on which side of the line a chunk falls.
const minSimilarity = 1e-6

// chunkID names a chunk within a tenant.
type chunkID struct {
	source, uri string
	chunk       int
}

func idOf(h Hit) chunkID { return chunkID{h.Source, h.URI, h.Locator.Chunk} }

// compareKeys orders chunks the same way in both backends: by source, then
// URI, then position, comparing bytes. It breaks every tie, so a ranking is a
// function of the index and not of map iteration or scan order.
func compareKeys(a, b Hit) int {
	return cmp.Or(
		strings.Compare(a.Source, b.Source),
		strings.Compare(a.URI, b.URI),
		cmp.Compare(a.Locator.Chunk, b.Locator.Chunk),
	)
}

// rankCandidates sorts an arm's candidates best first and keeps the top k.
func rankCandidates(cs []candidate, k int) []candidate {
	slices.SortFunc(cs, func(a, b candidate) int {
		return cmp.Or(cmp.Compare(b.score, a.score), compareKeys(a.hit, b.hit))
	})
	if len(cs) > k {
		cs = cs[:k]
	}
	return cs
}

// fuse merges the two arms by reciprocal rank fusion.
//
// Each list is already best first. A chunk's fused score is the sum over the
// arms that returned it of 1/(RRFK+rank). Only ranks are used: a cosine and a
// BM25 score are on unrelated scales, and any weighting of the raw numbers is
// a tuning knob that silently breaks when the embedding model changes.
func fuse(dense, lexical []candidate) []Hit {
	merged := make(map[chunkID]*Hit, len(dense)+len(lexical))
	at := func(c candidate) *Hit {
		key := idOf(c.hit)
		h, ok := merged[key]
		if !ok {
			hit := c.hit
			h = &hit
			merged[key] = h
		}
		return h
	}
	for i, c := range dense {
		h := at(c)
		h.Scores.Dense, h.Scores.DenseRank = c.score, i+1
		h.Scores.Fused += 1 / float64(RRFK+i+1)
	}
	for i, c := range lexical {
		h := at(c)
		h.Scores.Lexical, h.Scores.LexicalRank = c.score, i+1
		h.Scores.Fused += 1 / float64(RRFK+i+1)
	}
	out := make([]Hit, 0, len(merged))
	for _, h := range merged {
		out = append(out, *h)
	}
	slices.SortFunc(out, func(a, b Hit) int {
		return cmp.Or(cmp.Compare(b.Scores.Fused, a.Scores.Fused), compareKeys(a, b))
	})
	return out
}

// rerank reorders the first depth hits by the reranker's scores and leaves the
// rest where fusion put them, below every reranked hit.
//
// A reranker that fails fails the search. Falling back to the fused order
// would hand the caller a ranking it did not ask for with nothing to say so,
// and a deployment that configured a reranker has decided the fused order is
// not good enough.
func rerank(ctx context.Context, r Reranker, query string, hits []Hit, depth int) ([]Hit, error) {
	n := min(depth, len(hits))
	if n == 0 {
		return hits, nil
	}
	passages := make([]string, n)
	for i := range n {
		passages[i] = hits[i].Text
	}
	scores, err := r.Rerank(ctx, query, passages)
	if err != nil {
		return nil, fmt.Errorf("knowledge: rerank: %w", err)
	}
	if len(scores) != n {
		return nil, fmt.Errorf("knowledge: rerank: %d scores for %d passages", len(scores), n)
	}
	for i := range n {
		if math.IsNaN(scores[i]) {
			return nil, fmt.Errorf("knowledge: rerank: score %d is not a number", i)
		}
		hits[i].Scores.Rerank, hits[i].Scores.Reranked = scores[i], true
	}
	// Stable, so passages the reranker scores equally keep their fused
	// order.
	slices.SortStableFunc(hits[:n], func(a, b Hit) int {
		return cmp.Compare(b.Scores.Rerank, a.Scores.Rerank)
	})
	return hits, nil
}

// dot is the cosine of two vectors that are already unit length.
func dot(a, b []float32) float64 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

// normalize returns v scaled to unit length, and false when v is all zeros
// and therefore has no direction to compare.
func normalize(v []float32) ([]float32, bool) {
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm == 0 {
		return nil, false
	}
	norm = math.Sqrt(norm)
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = float32(float64(x) / norm)
	}
	return out, true
}
