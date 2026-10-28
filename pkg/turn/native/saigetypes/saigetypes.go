// Package saigetypes names the saige agent types the rest of fiber uses, so
// that saige is imported only under pkg/turn/native. Every name is an alias:
// values pass to and from the native runtime unchanged.
package saigetypes

import "github.com/urmzd/saige/agent/types"

// Tools and their schemas.
type (
	ParameterSchema = types.ParameterSchema
	PropertyDef     = types.PropertyDef
	Tool            = types.Tool
	ToolCallPart    = types.ToolCallPart
	ToolCapability  = types.ToolCapability
	ToolDef         = types.ToolDef
	ToolFunc        = types.ToolFunc
)

// Tool constants.
const (
	SchemaObject        = types.SchemaObject
	ToolCapabilityRead  = types.ToolCapabilityRead
	ToolCapabilityWrite = types.ToolCapabilityWrite
)

// ValidateJSON checks a value against a tool parameter schema.
func ValidateJSON(schema ParameterSchema, value any) error { return types.ValidateJSON(schema, value) }

// Providers and the deltas they stream.
type (
	Delta     = types.Delta
	PartDelta = types.PartDelta
	PartEnd   = types.PartEnd
	PartKind  = types.PartKind
	PartStart = types.PartStart
	Provider  = types.Provider
)

// KindText is the text part kind.
const KindText = types.KindText

// Messages, parts and media.
type (
	AssistantMessage = types.AssistantMessage
	AssistantPart    = types.AssistantPart
	AudioPart        = types.AudioPart
	ContentSupport   = types.ContentSupport
	DocumentMeta     = types.DocumentMeta
	DocumentPart     = types.DocumentPart
	Extractor        = types.Extractor
	ExtractorFunc    = types.ExtractorFunc
	ImageMeta        = types.ImageMeta
	ImagePart        = types.ImagePart
	MediaType        = types.MediaType
	Message          = types.Message
	Part             = types.Part
	Source           = types.Source
	TextPart         = types.TextPart
	UserMessage      = types.UserMessage
	UserPart         = types.UserPart
	VideoPart        = types.VideoPart
)

// MediaPNG is the PNG media type.
const MediaPNG = types.MediaPNG

// AssistantMsg builds an assistant message.
func AssistantMsg(parts ...AssistantPart) AssistantMessage { return types.AssistantMsg(parts...) }

// Bytes is a source holding data inline.
func Bytes(mt MediaType, b []byte) Source { return types.Bytes(mt, b) }

// Document is a document part.
func Document(src Source, meta ...DocumentMeta) DocumentPart { return types.Document(src, meta...) }

// Image is an image part.
func Image(src Source, meta ...ImageMeta) ImagePart { return types.Image(src, meta...) }

// IsMedia reports whether a part carries media.
func IsMedia(p Part) bool { return types.IsMedia(p) }

// PartsOf returns the parts of a message.
func PartsOf(m Message) []Part { return types.PartsOf(m) }

// SourceOf returns the source of a media part.
func SourceOf(p Part) (Source, bool) { return types.SourceOf(p) }

// UserMsg builds a user message.
func UserMsg(parts ...UserPart) UserMessage { return types.UserMsg(parts...) }

// Media is a media part.
func Media(src Source) UserPart { return types.Media(src) }

// Text is a text part.
func Text(s string) TextPart { return types.Text(s) }
