// Package ragtypes names the saige retrieval types and the text extractor the
// rest of fiber uses, so that saige is imported only under pkg/turn/native.
package ragtypes

import (
	"github.com/urmzd/saige/rag/extractor"
	"github.com/urmzd/saige/rag/types"

	"github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

// Retrieved context.
type (
	AssembledContext = types.AssembledContext
	ContextBlock     = types.ContextBlock
	Provenance       = types.Provenance
)

// ErrNoText is returned by an extractor that found no text.
var ErrNoText = extractor.ErrNoText

// AutoExtractor extracts text from a document by its media type.
func AutoExtractor() saigetypes.Extractor { return extractor.NewAuto().PartExtractor() }
