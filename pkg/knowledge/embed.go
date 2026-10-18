package knowledge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"
)

// Embedder turns text into vectors.
//
// A store holds one embedder for its lifetime. Vectors from two models do not
// live in the same space, so Model is part of every document's hash and the
// dense arm only compares a query against chunks its own model embedded.
//
// The interface does not distinguish a query from a passage. Models that want
// a different instruction for each are not served well by it; that is a known
// limit of this version.
type Embedder interface {
	// Embed returns one vector per text, in order, each Dimensions wide.
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	// Dimensions is the width of every vector Embed returns.
	Dimensions() int
	// Model identifies the embedding space. Two embedders that return the
	// same Model must return comparable vectors.
	Model() string
}

// embed calls the embedder and refuses anything a backend could not store. An
// embedder is the one part of this package that is somebody else's code or
// somebody else's service, so its output is checked rather than trusted.
func (e *engine) embed(ctx context.Context, texts []string) ([][]float32, error) {
	vectors, err := e.embedder.Embed(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEmbedding, err)
	}
	if len(vectors) != len(texts) {
		return nil, fmt.Errorf("%w: %s returned %d vectors for %d texts", ErrEmbedding, e.embedder.Model(), len(vectors), len(texts))
	}
	want := e.embedder.Dimensions()
	for i, v := range vectors {
		if len(v) != want {
			return nil, fmt.Errorf("%w: %s returned a vector of %d dimensions, not %d", ErrEmbedding, e.embedder.Model(), len(v), want)
		}
		for _, x := range v {
			if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
				return nil, fmt.Errorf("%w: %s returned a vector with a non-finite value for text %d", ErrEmbedding, e.embedder.Model(), i)
			}
		}
	}
	return vectors, nil
}

// DefaultHashingDimensions is the width of a Hashing embedder built with no
// explicit size. It is wide enough that unrelated short texts rarely collide
// and narrow enough to scan without an index.
const DefaultHashingDimensions = 256

// Hashing is a deterministic embedder that needs no model and no network.
//
// It hashes each word, and each three-character piece of each word, into a
// fixed number of buckets and normalizes the result. Two texts are similar when
// they share words or share word stems, so "rotating keys" lands near "key
// rotation" although neither is a substring of the other. That is the property
// that makes it a useful second opinion next to the lexical arm, which matches
// whole words only.
//
// It is what the tests and the demo run on, and it is a real embedder in the
// sense that everything downstream of it is exercised honestly. It is not a
// semantic model: it knows nothing about synonyms, and "car" is no closer to
// "automobile" than to "banana". Use HTTP for retrieval that has to understand
// meaning.
type Hashing struct{ dims int }

// NewHashing returns a hashing embedder of the given width. Zero or less takes
// DefaultHashingDimensions.
func NewHashing(dims int) *Hashing {
	if dims <= 0 {
		dims = DefaultHashingDimensions
	}
	return &Hashing{dims: dims}
}

var _ Embedder = (*Hashing)(nil)

// Dimensions is the width chosen at construction.
func (h *Hashing) Dimensions() int { return h.dims }

// Model names the algorithm and its width, both of which decide the space.
func (h *Hashing) Model() string { return fmt.Sprintf("fiber-hashing-v1/%d", h.dims) }

// Embed hashes each text. It never fails except on a cancelled context.
func (h *Hashing) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([][]float32, len(texts))
	for i, text := range texts {
		out[i] = h.one(text)
	}
	return out, nil
}

// trigramWeight is how much one character trigram counts next to a whole word.
// Below one, so an exact word match always outweighs a shared fragment.
const trigramWeight = 0.5

func (h *Hashing) one(text string) []float32 {
	acc := make([]float64, h.dims)
	add := func(kind byte, feature string, weight float64) {
		f := fnv.New64a()
		f.Write([]byte{kind})
		f.Write([]byte(feature))
		sum := f.Sum64()
		// The top bit picks a sign, so two features that collide in a
		// bucket cancel as often as they add and the expected error of a
		// dot product is zero.
		if sum>>63 == 1 {
			weight = -weight
		}
		acc[sum%uint64(h.dims)] += weight
	}
	for _, tok := range tokenize(text) {
		add('w', tok, 1)
		runes := []rune("^" + tok + "$")
		for j := 0; j+3 <= len(runes); j++ {
			add('g', string(runes[j:j+3]), trigramWeight)
		}
	}
	var norm float64
	for _, x := range acc {
		norm += x * x
	}
	out := make([]float32, h.dims)
	if norm == 0 {
		return out
	}
	norm = math.Sqrt(norm)
	for j, x := range acc {
		out[j] = float32(x / norm)
	}
	return out
}

// Base URLs for the providers the HTTP embedder is known to work against. Any
// server that implements the same endpoint works; these are conveniences.
const (
	OpenAIBaseURL    = "https://api.openai.com/v1"
	FireworksBaseURL = "https://api.fireworks.ai/inference/v1"
	OllamaBaseURL    = "http://localhost:11434/v1"
)

// HTTPConfig configures an HTTP embedder.
type HTTPConfig struct {
	// BaseURL is the API root; "/embeddings" is appended to it. Empty takes
	// OpenAIBaseURL.
	BaseURL string
	// Model is the embedding model's name as the provider knows it.
	Model string
	// APIKey is sent as a bearer token. Empty sends no Authorization
	// header, which is what a local Ollama expects.
	APIKey string
	// Dimensions is the width of the vectors the model returns. It is
	// required because a store has to know it before the first request
	// (Postgres declares its column with it), and every response is checked
	// against it.
	Dimensions int
	// RequestDimensions also sends Dimensions in the request, for models
	// that can shorten their output (OpenAI's text-embedding-3 family).
	// Leave it false for a model with a fixed width: some servers reject
	// the field.
	RequestDimensions bool
	// BatchSize caps how many texts go in one request. Zero takes
	// DefaultHTTPBatchSize.
	BatchSize int
	// Client is the HTTP client. Nil takes one with DefaultHTTPTimeout.
	Client *http.Client
}

// HTTP embedder defaults.
const (
	DefaultHTTPBatchSize = 64
	DefaultHTTPTimeout   = 60 * time.Second
	// maxEmbeddingResponse bounds how much of a response is read. A batch
	// of 64 vectors of 3072 floats is under 3 MiB as JSON.
	maxEmbeddingResponse = 64 << 20
)

// HTTP embeds through an OpenAI-compatible /v1/embeddings endpoint. That one
// request shape is served by OpenAI, Fireworks and Ollama among others, so one
// client written against the standard library covers all of them and adds no
// dependency.
//
// It does not retry. A failed batch fails the upsert, which writes nothing, and
// the caller decides whether to try again; a retry policy that is right for a
// rate-limited hosted API is wrong for a local server that is simply down.
type HTTP struct {
	url    string
	cfg    HTTPConfig
	client *http.Client
}

var _ Embedder = (*HTTP)(nil)

// NewHTTP validates cfg and returns the embedder. It makes no request.
func NewHTTP(cfg HTTPConfig) (*HTTP, error) {
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		return nil, fmt.Errorf("%w: http embedder needs a model", ErrInvalid)
	}
	if cfg.Dimensions <= 0 {
		return nil, fmt.Errorf("%w: http embedder for %q needs the model's dimensions", ErrInvalid, cfg.Model)
	}
	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = OpenAIBaseURL
	}
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		return nil, fmt.Errorf("%w: http embedder base url %q is not http or https", ErrInvalid, base)
	}
	if !strings.HasSuffix(base, "/embeddings") {
		base += "/embeddings"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultHTTPBatchSize
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: DefaultHTTPTimeout}
	}
	return &HTTP{url: base, cfg: cfg, client: client}, nil
}

// Dimensions is the configured width.
func (h *HTTP) Dimensions() int { return h.cfg.Dimensions }

// Model is the provider's model name. The endpoint is not part of it: the same
// model served from two places is the same space.
func (h *HTTP) Model() string { return h.cfg.Model }

// Embed sends texts in batches and returns their vectors in order.
func (h *HTTP) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, 0, len(texts))
	for batch := range slices.Chunk(texts, h.cfg.BatchSize) {
		vectors, err := h.batch(ctx, batch)
		if err != nil {
			return nil, err
		}
		out = append(out, vectors...)
	}
	return out, nil
}

type embeddingRequest struct {
	Model          string   `json:"model"`
	Input          []string `json:"input"`
	EncodingFormat string   `json:"encoding_format"`
	Dimensions     int      `json:"dimensions,omitempty"`
}

type embeddingResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (h *HTTP) batch(ctx context.Context, texts []string) ([][]float32, error) {
	body := embeddingRequest{Model: h.cfg.Model, Input: texts, EncodingFormat: "float"}
	if h.cfg.RequestDimensions {
		body.Dimensions = h.cfg.Dimensions
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("knowledge: encode embedding request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("knowledge: embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if h.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.cfg.APIKey)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("knowledge: embedding request to %s: %w", h.url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxEmbeddingResponse))
	if err != nil {
		return nil, fmt.Errorf("knowledge: read embedding response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// The provider's message is the only thing that says whether this
		// was a bad key, an unknown model or a rate limit, so it is kept,
		// shortened. The request, which holds the key, is not.
		return nil, fmt.Errorf("knowledge: embedding request to %s: %s: %s", h.url, resp.Status, snippet(payload))
	}

	var decoded embeddingResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, fmt.Errorf("knowledge: decode embedding response: %w", err)
	}
	if len(decoded.Data) != len(texts) {
		return nil, fmt.Errorf("knowledge: embedding response has %d vectors for %d inputs", len(decoded.Data), len(texts))
	}
	// The index field, not the array order, says which input a vector
	// belongs to. A vector attached to the wrong chunk is a wrong citation
	// that nothing downstream can detect, so a response that does not
	// account for every input exactly once is refused.
	out := make([][]float32, len(texts))
	for _, d := range decoded.Data {
		if d.Index < 0 || d.Index >= len(out) || out[d.Index] != nil {
			return nil, errors.New("knowledge: embedding response indexes do not match the inputs")
		}
		if len(d.Embedding) != h.cfg.Dimensions {
			return nil, fmt.Errorf("knowledge: model %q returned %d dimensions, configured for %d", h.cfg.Model, len(d.Embedding), h.cfg.Dimensions)
		}
		out[d.Index] = d.Embedding
	}
	return out, nil
}

func snippet(b []byte) string {
	const limit = 300
	s := strings.Join(strings.Fields(string(b)), " ")
	if len(s) > limit {
		s = strings.ToValidUTF8(s[:limit], "") + "..."
	}
	if s == "" {
		return "(empty body)"
	}
	return s
}
