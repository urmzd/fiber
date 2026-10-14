package knowledge

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// Memory is a Store held in process memory.
//
// It is the reference implementation, and a complete one: exact cosine search
// for the dense arm, BM25 for the lexical arm, the same fusion and reranking
// Postgres uses, and the same atomic replace. It is what the tests, the local
// example and a single-binary deployment run on.
//
// What it does not do is survive a restart or scale past what a linear scan
// can serve: every search reads every chunk in scope. That is fine for
// thousands of chunks and wrong for millions.
//
// All methods are safe for concurrent use.
type Memory struct {
	engine

	mu      sync.RWMutex
	tenants map[string]map[docKey]*memDoc
}

type docKey struct{ source, uri string }

type memDoc struct {
	title    string
	hash     string
	metadata map[string]string
	chunks   []memChunk
}

type memChunk struct {
	chunk  Chunk
	vector []float32 // unit length; nil when the embedding had no direction
	tf     map[string]int
	length int
}

// NewMemory returns an empty store that embeds with embedder.
func NewMemory(embedder Embedder, opts ...Option) *Memory {
	return &Memory{engine: newEngine(embedder, opts), tenants: make(map[string]map[docKey]*memDoc)}
}

var _ Store = (*Memory)(nil)

// Upsert indexes doc. The embedder is called outside the lock, so a slow
// embedding request does not stop searches, and the swap itself is one
// assignment under the write lock, which is what makes the replace atomic.
func (m *Memory) Upsert(ctx context.Context, tenant string, doc Document) (Receipt, error) {
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	p, err := m.prepare(tenant, doc)
	if err != nil {
		return Receipt{}, err
	}
	key := docKey{p.doc.Source, p.doc.URI}
	receipt := Receipt{Source: key.source, URI: key.uri, Hash: p.hash, Chunks: len(p.chunks)}

	m.mu.RLock()
	current, ok := m.tenants[tenant][key]
	unchanged := ok && current.hash == p.hash
	m.mu.RUnlock()
	if unchanged {
		return receipt, nil
	}

	vectors, err := m.embedChunks(ctx, p)
	if err != nil {
		return Receipt{}, err
	}
	next := &memDoc{title: p.doc.Title, hash: p.hash, metadata: p.doc.Metadata, chunks: make([]memChunk, len(p.chunks))}
	for i, c := range p.chunks {
		tokens := tokenize(indexText(p.doc.Title, c))
		tf := make(map[string]int, len(tokens))
		for _, t := range tokens {
			tf[t]++
		}
		unit, _ := normalize(vectors[i])
		next.chunks[i] = memChunk{chunk: c, vector: unit, tf: tf, length: len(tokens)}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	docs := m.tenants[tenant]
	if docs == nil {
		docs = make(map[docKey]*memDoc)
		m.tenants[tenant] = docs
	}
	// Two upserts of the same content can both pass the check above. The
	// second one to arrive changes nothing and says so.
	if current, ok := docs[key]; ok && current.hash == p.hash {
		return receipt, nil
	}
	docs[key] = next
	receipt.Changed = true
	return receipt, nil
}

// Delete removes a document and its chunks.
func (m *Memory) Delete(ctx context.Context, tenant, source, uri string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	source, uri, err := checkKey(tenant, source, uri)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := docKey{source, uri}
	if _, ok := m.tenants[tenant][key]; !ok {
		return fmt.Errorf("%w: document %q in source %q", ErrNotFound, uri, source)
	}
	delete(m.tenants[tenant], key)
	return nil
}

// Search runs both arms over every chunk in scope under one read lock, so the
// two arms see the same index.
func (m *Memory) Search(ctx context.Context, tenant string, q Query) ([]Hit, error) {
	hits, err := m.search(ctx, tenant, q, m.arms)
	if err != nil {
		return nil, err
	}
	if hits == nil {
		hits = []Hit{}
	}
	return hits, nil
}

func (m *Memory) arms(_ context.Context, s scope) (dense, lexical []candidate, err error) {
	query, hasDirection := normalize(s.vector)

	m.mu.RLock()
	defer m.mu.RUnlock()

	type entry struct {
		key   docKey
		doc   *memDoc
		index int
	}
	var (
		entries []entry
		df      = make(map[string]int, len(s.terms))
		total   int
	)
	for key, doc := range m.tenants[s.tenant] {
		if s.sources != nil && !slices.Contains(s.sources, key.source) {
			continue
		}
		for i := range doc.chunks {
			c := &doc.chunks[i]
			entries = append(entries, entry{key, doc, i})
			total += c.length
			for _, t := range s.terms {
				if c.tf[t] > 0 {
					df[t]++
				}
			}
		}
	}
	if len(entries) == 0 {
		return nil, nil, nil
	}
	avg := float64(total) / float64(len(entries))

	hit := func(e entry) Hit {
		c := e.doc.chunks[e.index].chunk
		return Hit{
			Source:   e.key.source,
			URI:      e.key.uri,
			Title:    e.doc.title,
			Locator:  c.Locator,
			Text:     c.Text,
			Metadata: maps.Clone(e.doc.metadata),
		}
	}
	for _, e := range entries {
		c := &e.doc.chunks[e.index]
		if hasDirection && c.vector != nil {
			if sim := dot(query, c.vector); sim > minSimilarity {
				dense = append(dense, candidate{hit: hit(e), score: sim})
			}
		}
		if score := bm25(c.tf, c.length, avg, len(entries), df, s.terms); score > 0 {
			lexical = append(lexical, candidate{hit: hit(e), score: score})
		}
	}
	return rankCandidates(dense, s.k), rankCandidates(lexical, s.k), nil
}
