package knowledge_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/urmzd/fiber/pkg/knowledge"
)

func cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / math.Sqrt(na*nb)
}

// The offline embedder is what makes the tests and the demo need no key. It
// has to be a function of its input and nothing else, or an index built in one
// process could not be queried from another.
func TestHashingIsDeterministicAndUnitLength(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	texts := []string{"Rotate the API key", "", "!!!", "日本語 text"}
	a, err := knowledge.NewHashing(64).Embed(ctx, texts)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := knowledge.NewHashing(64).Embed(ctx, texts)
	for i := range texts {
		if len(a[i]) != 64 {
			t.Fatalf("vector %d has %d dimensions; want 64", i, len(a[i]))
		}
		if !slices.Equal(a[i], b[i]) {
			t.Errorf("%q embedded differently by two embedders", texts[i])
		}
		var norm float64
		for _, x := range a[i] {
			norm += float64(x) * float64(x)
		}
		norm = math.Sqrt(norm)
		wordless := strings.TrimSpace(strings.Trim(texts[i], "!")) == ""
		if wordless && norm != 0 {
			t.Errorf("%q has no words but a vector of length %v", texts[i], norm)
		}
		if !wordless && math.Abs(norm-1) > 1e-5 {
			t.Errorf("%q has a vector of length %v; want 1", texts[i], norm)
		}
	}
	if knowledge.NewHashing(0).Dimensions() != knowledge.DefaultHashingDimensions {
		t.Error("zero dimensions did not take the default")
	}
	if knowledge.NewHashing(64).Model() == knowledge.NewHashing(128).Model() {
		t.Error("two widths report the same model, so their vectors would be mixed")
	}
}

// What makes it worth fusing with the lexical arm: it puts inflections of a
// word near each other, which whole-word matching cannot.
func TestHashingPlacesSharedWordsAndSharedStemsCloser(t *testing.T) {
	t.Parallel()
	v, _ := knowledge.NewHashing(0).Embed(context.Background(), []string{
		"rotating credentials",           // 0: the query
		"how to rotate a credential",     // 1: same stems, no shared word
		"rotating credentials quarterly", // 2: shared words
		"penguins of the southern ocean", // 3: unrelated
	})
	stems, words, unrelated := cosine(v[0], v[1]), cosine(v[0], v[2]), cosine(v[0], v[3])
	if !(words > stems && stems > unrelated+0.1) {
		t.Errorf("similarities: shared words %.3f, shared stems %.3f, unrelated %.3f; want that order with a clear gap", words, stems, unrelated)
	}
}

// recorder is an OpenAI-compatible embeddings endpoint that remembers what it
// was sent. The vector for an input is [length of the input, batch number].
type recorder struct {
	mu       sync.Mutex
	requests []map[string]any
	auth     []string
	paths    []string
	reverse  bool
}

func (rec *recorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	rec.mu.Lock()
	rec.requests = append(rec.requests, body)
	rec.auth = append(rec.auth, r.Header.Get("Authorization"))
	rec.paths = append(rec.paths, r.Method+" "+r.URL.Path)
	batch := len(rec.requests)
	rec.mu.Unlock()

	inputs, _ := body["input"].([]any)
	type item struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	}
	data := make([]item, len(inputs))
	for i, in := range inputs {
		data[i] = item{Index: i, Embedding: []float32{float32(len(in.(string))), float32(batch)}}
	}
	if rec.reverse {
		slices.Reverse(data)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
}

// One request shape serves OpenAI, Fireworks and Ollama. This pins that shape:
// the path, the body, the bearer token, and batches reassembled in order.
func TestHTTPEmbedderSpeaksTheOpenAIEmbeddingsShape(t *testing.T) {
	t.Parallel()
	rec := &recorder{reverse: true}
	srv := httptest.NewServer(rec)
	defer srv.Close()

	e, err := knowledge.NewHTTP(knowledge.HTTPConfig{
		BaseURL: srv.URL + "/v1/", Model: "nomic-embed-text", APIKey: "sk-secret", Dimensions: 2, BatchSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	texts := []string{"a", "bb", "ccc", "dddd", "eeeee"}
	vectors, err := e.Embed(context.Background(), texts)
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vectors) != len(texts) {
		t.Fatalf("got %d vectors for %d texts", len(vectors), len(texts))
	}
	for i, v := range vectors {
		// The server answered each batch in reverse; the index field,
		// not the array order, decides which text a vector belongs to.
		if int(v[0]) != len(texts[i]) || int(v[1]) != i/2+1 {
			t.Errorf("vector %d = %v; want the one for %q from batch %d", i, v, texts[i], i/2+1)
		}
	}
	if len(rec.requests) != 3 {
		t.Fatalf("%d requests for 5 texts in batches of 2; want 3", len(rec.requests))
	}
	first := rec.requests[0]
	if rec.paths[0] != "POST /v1/embeddings" || first["model"] != "nomic-embed-text" || first["encoding_format"] != "float" {
		t.Errorf("request = %s %v", rec.paths[0], first)
	}
	if _, sent := first["dimensions"]; sent {
		t.Error("dimensions was sent without RequestDimensions; servers with fixed-width models reject it")
	}
	if rec.auth[0] != "Bearer sk-secret" {
		t.Errorf("Authorization = %q", rec.auth[0])
	}
	if e.Model() != "nomic-embed-text" || e.Dimensions() != 2 {
		t.Errorf("identity = %q/%d", e.Model(), e.Dimensions())
	}
}

func TestHTTPEmbedderSendsOnlyWhatItIsConfiguredTo(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	srv := httptest.NewServer(rec)
	defer srv.Close()

	// No key, as for a local Ollama, and a model that can be shortened.
	e, err := knowledge.NewHTTP(knowledge.HTTPConfig{BaseURL: srv.URL, Model: "text-embedding-3-small", Dimensions: 2, RequestDimensions: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Embed(context.Background(), []string{"hello"}); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if rec.auth[0] != "" {
		t.Errorf("an Authorization header %q was sent with no key configured", rec.auth[0])
	}
	if rec.requests[0]["dimensions"] != float64(2) {
		t.Errorf("dimensions = %v; want 2 when RequestDimensions is set", rec.requests[0]["dimensions"])
	}
	if rec.paths[0] != "POST /embeddings" {
		t.Errorf("path = %q", rec.paths[0])
	}
}

// A response that cannot be trusted to line up with its inputs is an error,
// never a best effort: a vector on the wrong chunk is a wrong citation.
func TestHTTPEmbedderRefusesResponsesItCannotTrust(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		status int
		body   string
		want   string
	}{
		"provider error":   {401, `{"error":{"message":"Incorrect API key provided"}}`, "Incorrect API key provided"},
		"not json":         {200, `<html>gateway</html>`, "decode"},
		"too few vectors":  {200, `{"data":[]}`, "0 vectors for 1 inputs"},
		"wrong width":      {200, `{"data":[{"index":0,"embedding":[1,2,3]}]}`, "returned 3 dimensions"},
		"index out of set": {200, `{"data":[{"index":4,"embedding":[1,2]}]}`, "indexes do not match"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			e, err := knowledge.NewHTTP(knowledge.HTTPConfig{BaseURL: srv.URL, Model: "m", APIKey: "sk-secret", Dimensions: 2})
			if err != nil {
				t.Fatal(err)
			}
			_, err = e.Embed(context.Background(), []string{"hello"})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Embed = %v; want an error mentioning %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "sk-secret") {
				t.Errorf("the API key leaked into the error: %v", err)
			}
		})
	}
}

func TestHTTPEmbedderConfigurationIsCheckedUpFront(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]knowledge.HTTPConfig{
		"no model":      {Dimensions: 8},
		"no dimensions": {Model: "m"},
		"not a url":     {Model: "m", Dimensions: 8, BaseURL: "ftp://example.com"},
	} {
		if _, err := knowledge.NewHTTP(cfg); !errors.Is(err, knowledge.ErrInvalid) {
			t.Errorf("NewHTTP with %s = %v; want ErrInvalid", name, err)
		}
	}
}

// End to end through a store, so the HTTP embedder is known to satisfy what a
// backend asks of an Embedder and a provider outage surfaces as ErrEmbedding.
func TestHTTPEmbedderFailureSurfacesAsAnEmbeddingError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
	}))
	defer srv.Close()
	e, err := knowledge.NewHTTP(knowledge.HTTPConfig{BaseURL: srv.URL, Model: "m", Dimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	s := knowledge.NewMemory(e)
	_, err = s.Upsert(context.Background(), tenantA, knowledge.Document{Source: "kb", URI: "a", Content: "alpha"})
	if !errors.Is(err, knowledge.ErrEmbedding) || !strings.Contains(err.Error(), "429") {
		t.Errorf("Upsert = %v; want ErrEmbedding carrying the provider's status", err)
	}
}
