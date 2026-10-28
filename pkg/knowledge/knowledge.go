// Package knowledge is the retrieval layer: it indexes documents and answers a
// query with the passages most likely to contain the answer, each one carrying
// enough provenance to be cited.
//
// It answers one question, "which passages of this tenant's corpora bear on
// this text", and refuses to answer anything else. Where documents come from
// (a crawler, a connector, a CLI) is the caller's concern: this package is
// handed content and never fetches it. What a model does with the passages
// belongs to pkg/turn/native, which turns them into a prompt and into citation
// events; Retriever in this package is the adapter between the two.
//
// # Why hybrid
//
// Dense similarity finds a passage that means what the query means and misses
// one that merely contains the exact identifier the user typed. Lexical ranking
// does the reverse. Neither score is comparable with the other, so the two
// rankings are fused by reciprocal rank fusion, which uses only the position of
// a passage in each list and therefore needs no calibration between a cosine
// and a term score. An optional Reranker then reorders the head of the fused
// list, which is where a slower and more accurate model earns its cost.
//
// # Why chunks, and why they keep a locator
//
// A document is split into chunks because a model is given passages, not
// files, and because one embedding cannot represent forty pages. Every chunk
// keeps its document's URI and a Locator (byte range, line range, nearest
// heading), and its Text is always an exact substring of the indexed content.
// That is what makes a citation checkable: a reader can open the URI, go to
// the lines, and find the words.
//
// # Re-indexing
//
// A document is identified by (tenant, source, URI). Upsert hashes what it was
// given together with everything that decides how it is indexed (the embedding
// model, the chunking parameters). The same hash is a no-op that does not call
// the embedder, so a crawler can resubmit a whole corpus on every pass and pay
// only for what changed. A different hash replaces every chunk of that
// document in one step: a search sees the old chunks or the new ones, never a
// mixture, and a failed re-index leaves the old ones in place.
//
// # Tenancy
//
// The tenant is a hard boundary and an explicit argument on every method, for
// the reason pkg/definition gives: a call with no tenant does not compile. No
// operation reads across tenants, and no error reveals that a document exists
// in another one.
//
// # Backends
//
// Memory is the reference implementation and a complete system. Postgres keeps
// the same contract on pgvector and Postgres full text search. Both run the
// same fusion and reranking code, which lives here rather than in SQL, and both
// are held to one test suite. They differ in how the lexical arm scores (see
// Postgres), which can change the order of lexical candidates and therefore
// the fused order; they do not differ in which documents are visible.
package knowledge

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"
)

// Sentinels every implementation reports, matched with errors.Is.
var (
	// ErrNotFound reports a document this tenant does not have. A document
	// belonging to another tenant is not found either.
	ErrNotFound = errors.New("knowledge: not found")

	// ErrInvalid reports a document, a query or an argument the store will
	// not accept.
	ErrInvalid = errors.New("knowledge: invalid")

	// ErrEmbedding reports that the embedder failed or returned vectors the
	// store cannot use: the wrong count, the wrong width, or a value that is
	// not a finite number. It wraps the embedder's own error when there is
	// one. Nothing is written when an upsert fails this way.
	ErrEmbedding = errors.New("knowledge: embedding failed")

	// ErrDimensions reports that the Postgres table was created for vectors
	// of a different width than the embedder now produces. Vectors of two
	// widths cannot share a column, so the store refuses to start rather
	// than fail on the first write.
	ErrDimensions = errors.New("knowledge: embedding dimensions do not match the index")
)

// Limits.
const (
	// DefaultLimit is how many hits a search returns when the query asks for
	// none.
	DefaultLimit = 5
	// MaxLimit caps what a query can ask for. A larger value is clamped, not
	// rejected.
	MaxLimit = 100
	// DefaultCandidates is how many chunks each arm contributes to fusion.
	DefaultCandidates = 50
	// MaxCandidates caps the per-arm candidate count.
	MaxCandidates = 500
	// MaxDocumentBytes caps one document's content. A larger document is
	// refused: it should be split by whoever knows its structure.
	MaxDocumentBytes = 4 << 20
	// RRFK is the reciprocal rank fusion constant: a chunk at rank r in one
	// arm contributes 1/(RRFK+r). Sixty is the value from the original
	// paper; it damps the difference between adjacent ranks enough that
	// agreement between the arms beats a single first place.
	RRFK = 60
)

// Document is one unit of content to index.
type Document struct {
	// Source names the corpus the document belongs to. It is the name an
	// agent's policy corpus carries, which is how an agent is scoped to
	// the corpora it declared.
	Source string
	// URI identifies the document within its source and is what a citation
	// points at. It is stored as given.
	URI string
	// Title is shown with a citation and is indexed with every chunk, so a
	// passage can be found by what its document is about.
	Title string
	// Content is the text to index. It must be valid UTF-8.
	Content string
	// Metadata is returned with every hit from this document. The store
	// does not interpret it.
	Metadata map[string]string
}

// Receipt reports what an upsert did.
type Receipt struct {
	Source string
	URI    string
	// Hash is the content hash the document is now indexed under.
	Hash string
	// Chunks is how many chunks the document is indexed as.
	Chunks int
	// Changed is false when the content was already indexed and nothing was
	// written or embedded.
	Changed bool
}

// Locator says where in its document a chunk came from.
type Locator struct {
	// Chunk is the chunk's position in the document, from 0.
	Chunk int `json:"chunk"`
	// Start and End are byte offsets into the indexed content, End
	// exclusive: Content[Start:End] is exactly the chunk's text.
	Start int `json:"start"`
	End   int `json:"end"`
	// StartLine and EndLine are 1-based and inclusive.
	StartLine int `json:"start_line"`
	EndLine   int `json:"end_line"`
	// Heading is the nearest Markdown heading at or above the chunk, empty
	// when the document has none.
	Heading string `json:"heading,omitempty"`
}

// String renders the line range the way a code host's URL fragment does, as
// "L12-L30" or "L12".
func (l Locator) String() string {
	if l.EndLine <= l.StartLine {
		return fmt.Sprintf("L%d", l.StartLine)
	}
	return fmt.Sprintf("L%d-L%d", l.StartLine, l.EndLine)
}

// Query is one search.
type Query struct {
	// Text is what to look for, in natural language.
	Text string
	// Sources restricts the search to these corpora. Empty searches every
	// source the tenant has, which is right for an operator and wrong for an
	// agent: Retriever never issues an unscoped query.
	Sources []string
	// Limit caps the hits returned. Zero takes DefaultLimit.
	Limit int
	// Candidates is how many chunks each arm contributes to fusion. Zero
	// takes DefaultCandidates. It is raised to cover Limit and the rerank
	// depth when it is smaller than either.
	Candidates int
}

// Scores is why a hit ranked where it did.
type Scores struct {
	// Fused is the reciprocal rank fusion score. Without a reranker, hits
	// are ordered by it.
	Fused float64 `json:"fused"`
	// Dense is the cosine similarity to the query, and DenseRank the chunk's
	// 1-based position in the dense arm. Both are zero when the dense arm
	// did not return the chunk.
	Dense     float64 `json:"dense"`
	DenseRank int     `json:"dense_rank"`
	// Lexical is the lexical arm's score and LexicalRank the position in
	// it. The score's scale depends on the backend (BM25 in Memory,
	// ts_rank_cd in Postgres) and is not comparable across them.
	Lexical     float64 `json:"lexical"`
	LexicalRank int     `json:"lexical_rank"`
	// Rerank is the reranker's score. It is meaningful only when Reranked is
	// true, and then it is what the hits are ordered by.
	Rerank   float64 `json:"rerank"`
	Reranked bool    `json:"reranked"`
}

// Hit is one retrieved chunk with what is needed to cite it.
type Hit struct {
	Source   string            `json:"source"`
	URI      string            `json:"uri"`
	Title    string            `json:"title,omitempty"`
	Locator  Locator           `json:"locator"`
	Text     string            `json:"text"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Scores   Scores            `json:"scores"`
}

// Store indexes documents and searches them.
//
// Every method takes the tenant explicitly. The tenant comes from the caller's
// credential and is never read from a request body.
type Store interface {
	// Upsert indexes doc, replacing whatever was indexed under the same
	// (tenant, source, URI). Content already indexed is a no-op reported by
	// Receipt.Changed being false. A replacement is atomic.
	Upsert(ctx context.Context, tenant string, doc Document) (Receipt, error)

	// Delete removes a document and all of its chunks. It fails with
	// ErrNotFound when the tenant has no such document.
	Delete(ctx context.Context, tenant, source, uri string) error

	// Search returns the chunks that best match q, best first. No match is
	// an empty slice, not an error.
	Search(ctx context.Context, tenant string, q Query) ([]Hit, error)
}

// Reranker rescores passages against a query. It is the slot for a cross
// encoder or a hosted rerank endpoint; this package ships none, because every
// one worth using is a model behind an API.
//
// Rerank returns one score per passage, in the order given, higher is better.
type Reranker interface {
	Rerank(ctx context.Context, query string, passages []string) ([]float64, error)
}

// RerankerFunc adapts a function to Reranker.
type RerankerFunc func(ctx context.Context, query string, passages []string) ([]float64, error)

// Rerank calls f.
func (f RerankerFunc) Rerank(ctx context.Context, query string, passages []string) ([]float64, error) {
	return f(ctx, query, passages)
}

// Chunking defaults, in bytes.
const (
	// DefaultChunkSize is the target upper bound on a chunk. It is about
	// three hundred tokens of English: small enough that a hit is a passage
	// and not a page, large enough to hold a paragraph whole.
	DefaultChunkSize = 1200
	// DefaultChunkOverlap is how much trailing text a chunk may repeat from
	// the one before it, so a fact that straddles a boundary is whole in at
	// least one chunk.
	DefaultChunkOverlap = 200
	// DefaultRerankDepth is how many fused candidates a reranker is shown.
	DefaultRerankDepth = 20
)

// Option configures a store. The same options apply to every backend, so
// swapping Memory for Postgres changes a constructor and nothing else.
type Option func(*engine)

// WithChunking sets the target chunk size and the overlap between consecutive
// chunks, in bytes. Values that make no sense (a size below one, an overlap
// that is negative or not smaller than the size) are ignored.
func WithChunking(size, overlap int) Option {
	return func(e *engine) {
		if size >= 1 && overlap >= 0 && overlap < size {
			e.chunk = chunking{size: size, overlap: overlap}
		}
	}
}

// WithReranker reorders the head of the fused list with r. depth is how many
// candidates it is shown; zero or less takes DefaultRerankDepth, and a depth
// below the query's limit is raised to it so every returned hit was reranked.
func WithReranker(r Reranker, depth int) Option {
	return func(e *engine) {
		if depth <= 0 {
			depth = DefaultRerankDepth
		}
		e.reranker, e.rerankDepth = r, depth
	}
}

// WithTextConfig names the Postgres text search configuration the lexical arm
// indexes and queries with, such as "english" for stemming. The default is
// "simple", which does not stem and is therefore the closest to what Memory
// does. Memory ignores it.
func WithTextConfig(name string) Option {
	return func(e *engine) {
		if name = strings.TrimSpace(name); name != "" {
			e.textConfig = name
		}
	}
}

// engine is everything the two backends share: how a document becomes chunks
// and vectors, and how two ranked lists become one. Keeping it out of the
// backends is what makes "Memory and Postgres rank by the same rules" a fact
// about the code rather than a hope about two implementations.
type engine struct {
	embedder    Embedder
	chunk       chunking
	reranker    Reranker
	rerankDepth int
	textConfig  string
}

func newEngine(embedder Embedder, opts []Option) engine {
	e := engine{
		embedder:   embedder,
		chunk:      chunking{size: DefaultChunkSize, overlap: DefaultChunkOverlap},
		textConfig: "simple",
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&e)
		}
	}
	return e
}

// prepared is a validated document, split and hashed, not yet embedded.
type prepared struct {
	doc    Document
	hash   string
	chunks []Chunk
}

// prepare validates doc and does every part of indexing that needs neither the
// embedder nor the store.
func (e *engine) prepare(tenant string, doc Document) (prepared, error) {
	if err := checkTenant(tenant); err != nil {
		return prepared{}, err
	}
	doc.Source = strings.TrimSpace(doc.Source)
	doc.URI = strings.TrimSpace(doc.URI)
	doc.Title = strings.TrimSpace(doc.Title)
	switch {
	case doc.Source == "":
		return prepared{}, fmt.Errorf("%w: document has no source", ErrInvalid)
	case doc.URI == "":
		return prepared{}, fmt.Errorf("%w: document in source %q has no uri", ErrInvalid, doc.Source)
	case len(doc.Content) > MaxDocumentBytes:
		return prepared{}, fmt.Errorf("%w: document %q is %d bytes; the limit is %d", ErrInvalid, doc.URI, len(doc.Content), MaxDocumentBytes)
	}
	fields := []string{doc.Source, doc.URI, doc.Title, doc.Content}
	for k, v := range doc.Metadata {
		fields = append(fields, k, v)
	}
	for _, f := range fields {
		if err := checkText(f); err != nil {
			return prepared{}, fmt.Errorf("%w: document %q: %w", ErrInvalid, doc.URI, err)
		}
	}
	// An empty map and no map are the same metadata; one spelling keeps the
	// backends from disagreeing about which they hand back.
	if doc.Metadata = maps.Clone(doc.Metadata); len(doc.Metadata) == 0 {
		doc.Metadata = nil
	}

	chunks := split(doc.Content, e.chunk)
	if len(chunks) == 0 {
		return prepared{}, fmt.Errorf("%w: document %q has no content to index", ErrInvalid, doc.URI)
	}
	return prepared{doc: doc, hash: e.hash(doc), chunks: chunks}, nil
}

// embedChunks embeds every chunk of p, in order.
func (e *engine) embedChunks(ctx context.Context, p prepared) ([][]float32, error) {
	texts := make([]string, len(p.chunks))
	for i, c := range p.chunks {
		texts[i] = indexText(p.doc.Title, c)
	}
	return e.embed(ctx, texts)
}

// indexText is what a chunk is embedded and lexically indexed as: its own text
// under its document's title and its section heading. A chunk in the middle of
// a refund policy rarely says "refund policy"; this is what lets it be found
// by that name. The chunk's Text stays the exact source text.
func indexText(title string, c Chunk) string {
	var b strings.Builder
	for _, part := range []string{title, c.Locator.Heading} {
		if part != "" {
			b.WriteString(part)
			b.WriteByte('\n')
		}
	}
	b.WriteString(c.Text)
	return b.String()
}

// scope is a validated search, ready for a backend's two arms.
type scope struct {
	tenant  string
	sources []string // nil means every source
	vector  []float32
	terms   []string
	k       int
}

// candidate is one chunk as an arm returned it. Its position in the arm's list
// is its rank.
type candidate struct {
	hit   Hit
	score float64
}

// fetch runs both arms against one consistent view of the index.
type fetch func(ctx context.Context, s scope) (dense, lexical []candidate, err error)

// search is the whole query path apart from the two arms themselves.
func (e *engine) search(ctx context.Context, tenant string, q Query, run fetch) ([]Hit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkTenant(tenant); err != nil {
		return nil, err
	}
	text := strings.TrimSpace(q.Text)
	if text == "" {
		return nil, fmt.Errorf("%w: empty query", ErrInvalid)
	}
	if err := checkText(text); err != nil {
		return nil, fmt.Errorf("%w: query: %w", ErrInvalid, err)
	}
	sources, err := normalizeSources(q.Sources)
	if err != nil {
		return nil, err
	}
	limit := clamp(q.Limit, DefaultLimit, MaxLimit)
	depth := 0
	if e.reranker != nil {
		depth = max(e.rerankDepth, limit)
	}
	k := max(clamp(q.Candidates, DefaultCandidates, MaxCandidates), limit, depth)

	vectors, err := e.embed(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	dense, lexical, err := run(ctx, scope{
		tenant:  tenant,
		sources: sources,
		vector:  vectors[0],
		terms:   queryTerms(text),
		k:       k,
	})
	if err != nil {
		return nil, err
	}
	hits := fuse(dense, lexical)
	if e.reranker != nil {
		if hits, err = rerank(ctx, e.reranker, text, hits, depth); err != nil {
			return nil, err
		}
	}
	if len(hits) > limit {
		hits = hits[:limit]
	}
	return hits, nil
}

func checkTenant(tenant string) error {
	if strings.TrimSpace(tenant) == "" {
		return fmt.Errorf("%w: no tenant", ErrInvalid)
	}
	return nil
}

// checkText refuses what Postgres text cannot hold, in both backends, so a
// document Memory accepts is one Postgres accepts.
func checkText(s string) error {
	if !utf8.ValidString(s) {
		return errors.New("text is not valid UTF-8")
	}
	if strings.ContainsRune(s, 0) {
		return errors.New("text contains a NUL byte")
	}
	return nil
}

// checkKey validates the (source, uri) of a delete.
func checkKey(tenant, source, uri string) (string, string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", "", err
	}
	source, uri = strings.TrimSpace(source), strings.TrimSpace(uri)
	if source == "" || uri == "" {
		return "", "", fmt.Errorf("%w: a document is named by a source and a uri", ErrInvalid)
	}
	return source, uri, nil
}

// normalizeSources trims, deduplicates and sorts a source filter. An empty
// name is refused rather than dropped: dropping it could turn a filter into no
// filter, which widens the search to every corpus the tenant has.
func normalizeSources(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("%w: empty source name in query", ErrInvalid)
		}
		out = append(out, s)
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

func clamp(n, def, hi int) int {
	switch {
	case n <= 0:
		return def
	case n > hi:
		return hi
	}
	return n
}
