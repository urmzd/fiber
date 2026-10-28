package knowledge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	ragtypes "github.com/urmzd/fiber/pkg/turn/native/ragtypes"
)

// Retriever makes a Store the retrieval source for one agent's turn.
//
// It has the method pkg/turn/native's Retriever interface asks for, so it is
// passed to native.WithRetriever as it is. This package does not import
// native to say so: the dependency runs one way, from the runtime that
// composes the two.
//
// A Retriever is bound, at construction, to one tenant and to the source names
// one agent declared. That binding is the access rule. native's interface
// carries only the query text, so nothing a model or a user writes can widen a
// search to another tenant or to a corpus the agent's pinned revision does not
// name. It also means a Retriever is built per run, from the run's tenant and
// its revision's Sources, and never shared across tenants.
//
// A policy corpus (definition.Corpus) is matched to a corpus by its Name: documents indexed
// with Document.Source equal to that name are what the agent may read. The
// source's URI and Options describe where the corpus is ingested from and are
// not interpreted here.
type Retriever struct {
	store   Store
	tenant  string
	sources []string
	limit   int
}

// RetrieverOption configures a Retriever.
type RetrieverOption func(*Retriever)

// WithLimit sets how many passages a turn is grounded in. Zero or less keeps
// DefaultLimit.
func WithLimit(n int) RetrieverOption {
	return func(r *Retriever) {
		if n > 0 {
			r.limit = n
		}
	}
}

// NewRetriever returns a retriever over store for tenant, restricted to the
// named sources.
//
// With no sources it retrieves nothing. It does not fall back to searching
// every source: an agent that declares no corpus has been granted none, and
// Query's "empty means all" is a convenience for operators that must not
// become a default for agents.
func NewRetriever(store Store, tenant string, sources []string, opts ...RetrieverOption) *Retriever {
	r := &Retriever{store: store, tenant: tenant, limit: DefaultLimit}
	for _, s := range sources {
		if s = strings.TrimSpace(s); s != "" {
			r.sources = append(r.sources, s)
		}
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
}

// preamble tells the model what the passages are and how to refer to them.
// The labels it names are the ones the citation events carry, which is what
// lets a sink turn "[2]" in the answer into a link.
const preamble = "Answer using the numbered passages below. Cite a passage by its label, such as [1], after the claim it supports. If the passages do not contain the answer, say so instead of guessing."

// Retrieve searches the agent's sources and assembles what was found.
//
// The result is used twice by native: Prompt goes in front of the user's
// text, and Blocks become citation events. Nothing found is a nil context and
// no error, which native reads as "answer from the model alone".
func (r *Retriever) Retrieve(ctx context.Context, query string) (*ragtypes.AssembledContext, error) {
	if len(r.sources) == 0 || strings.TrimSpace(query) == "" {
		return nil, nil
	}
	hits, err := r.store.Search(ctx, r.tenant, Query{Text: query, Sources: r.sources, Limit: r.limit})
	if err != nil {
		return nil, fmt.Errorf("knowledge: retrieve: %w", err)
	}
	return Assemble(hits), nil
}

// Assemble turns hits into the context a turn is grounded in: a prompt that
// shows each passage under a numbered label, and one block per passage with
// the provenance a citation needs. It returns nil for no hits.
//
// It is exported for callers that search with their own Query and still want
// the standard assembly.
func Assemble(hits []Hit) *ragtypes.AssembledContext {
	if len(hits) == 0 {
		return nil
	}
	var prompt strings.Builder
	prompt.WriteString(preamble)
	blocks := make([]ragtypes.ContextBlock, len(hits))
	for i, h := range hits {
		label := "[" + strconv.Itoa(i+1) + "]"
		doc := DocumentID(h.Source, h.URI)
		blocks[i] = ragtypes.ContextBlock{
			Text:     h.Text,
			Citation: label,
			Provenance: ragtypes.Provenance{
				DocumentUUID:   doc,
				DocumentTitle:  h.Title,
				SourceURI:      h.URI,
				SectionUUID:    doc + ":" + strconv.Itoa(h.Locator.Chunk),
				SectionHeading: section(h.Locator),
				SectionIndex:   h.Locator.Chunk,
			},
		}
		prompt.WriteString("\n\n")
		prompt.WriteString(label)
		prompt.WriteByte(' ')
		if h.Title != "" {
			prompt.WriteString(h.Title)
			prompt.WriteString(" (")
			prompt.WriteString(h.URI)
			prompt.WriteString(")")
		} else {
			prompt.WriteString(h.URI)
		}
		prompt.WriteByte('\n')
		prompt.WriteString(h.Text)
	}
	text := prompt.String()
	return &ragtypes.AssembledContext{
		Prompt: text,
		Blocks: blocks,
		// A rough count, four bytes to a token, for callers that budget
		// context. It is not a tokenizer's answer.
		TokenCount: (len(text) + 3) / 4,
	}
}

// section renders a locator into the one free-text field provenance has for
// "where in the document". The line range is always there, because a heading
// alone does not locate a passage in a long section.
func section(l Locator) string {
	if l.Heading == "" {
		return l.String()
	}
	return l.Heading + ", " + l.String()
}

// DocumentID is a stable identifier for a document within its tenant, derived
// from its source and URI. It fills the provenance fields that expect an
// opaque id; it is not a UUID and is not secret.
func DocumentID(source, uri string) string {
	sum := sha256.Sum256([]byte(source + "\x00" + uri))
	return hex.EncodeToString(sum[:16])
}
