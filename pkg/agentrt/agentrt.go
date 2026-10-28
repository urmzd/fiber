// Package agentrt is a temporary bridge to pkg/turn/native, which now holds
// the agent runner. It keeps the old names and the Chunk shape (an
// envelope.Kind and a marshalled payload) for callers that have not moved to
// event bodies yet. It is removed once they have.
package agentrt

import (
	"context"
	"encoding/json"
	"fmt"

	saigetypes "github.com/urmzd/saige/agent/types"
	ragtypes "github.com/urmzd/saige/rag/types"

	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/event/legacy"
	"github.com/urmzd/fiber/pkg/turn/native"
)

// Chunk is one event body a turn produced: a kind and its marshalled payload.
type Chunk struct {
	Kind    envelope.Kind
	Payload []byte
}

// kindOf maps each type back to the old kind it replaced.
var kindOf = func() map[string]envelope.Kind {
	out := make(map[string]envelope.Kind)
	for _, k := range legacy.KeptKinds() {
		t, err := legacy.TypeOf(k)
		if err != nil {
			panic(err)
		}
		out[t] = envelope.Kind(k)
	}
	return out
}()

// ChunkOf converts a body to a Chunk. A body with no old kind, or one that
// does not marshal, has no chunk.
func ChunkOf(b event.Body) (Chunk, bool) {
	if b == nil {
		return Chunk{}, false
	}
	kind, ok := kindOf[b.Type()]
	if !ok {
		return Chunk{}, false
	}
	p, err := json.Marshal(b)
	if err != nil {
		return Chunk{}, false
	}
	return Chunk{Kind: kind, Payload: p}, true
}

// BodyOf converts a Chunk back to its body.
func BodyOf(c Chunk) (event.Body, error) {
	t, err := legacy.TypeOf(string(c.Kind))
	if err != nil {
		return nil, err
	}
	return event.Decode(t, c.Payload)
}

// Emitter adapts a Chunk callback to a body callback. A body with no old
// kind is skipped.
func Emitter(emit func(context.Context, Chunk) error) func(context.Context, event.Body) error {
	if emit == nil {
		return nil
	}
	return func(ctx context.Context, b event.Body) error {
		c, ok := ChunkOf(b)
		if !ok {
			return nil
		}
		return emit(ctx, c)
	}
}

// Runner runs turns of one pinned revision, emitting chunks.
type Runner struct {
	*native.Runner
}

// New builds a runner for one pinned revision.
func New(rev Revision, opts ...Option) (*Runner, error) {
	r, err := native.New(rev, opts...)
	if err != nil {
		return nil, err
	}
	return &Runner{Runner: r}, nil
}

// Run executes one turn, handing every chunk to emit as it is produced.
func (r *Runner) Run(ctx context.Context, t Turn, emit func(context.Context, Chunk) error) (Result, error) {
	if emit == nil {
		return r.Runner.Run(ctx, t, nil)
	}
	return r.Runner.Run(ctx, t, Emitter(emit))
}

// Translator converts saige deltas into chunks. A zero Translator works.
type Translator struct {
	t native.Translator
}

// NewTranslator returns a translator for one turn.
func NewTranslator() *Translator { return &Translator{} }

// Delta maps one saige delta to a chunk.
func (t *Translator) Delta(d saigetypes.Delta) (Chunk, bool) {
	b, ok := t.t.Delta(d)
	if !ok {
		return Chunk{}, false
	}
	return ChunkOf(b)
}

// Ran records the arguments a tool call is about to run with.
func (t *Translator) Ran(callID, tool string, args map[string]any, path ...string) {
	t.t.Ran(callID, tool, args, path...)
}

// Citations converts an assembled RAG context into citation chunks.
func Citations(ac *ragtypes.AssembledContext) []Chunk {
	bodies := native.Citations(ac)
	out := make([]Chunk, 0, len(bodies))
	for _, b := range bodies {
		if c, ok := ChunkOf(b); ok {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// MustChunk is ChunkOf for a body that has a kind.
func MustChunk(b event.Body) Chunk {
	c, ok := ChunkOf(b)
	if !ok {
		panic(fmt.Sprintf("agentrt: %s has no chunk", b.Type()))
	}
	return c
}
