// Package agenttest names saige's scripted provider and mock tool for tests
// and examples, so that saige is imported only under pkg/turn/native.
package agenttest

import (
	"github.com/urmzd/saige/agent/agenttest"

	"github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

// Fakes.
type (
	MockTool         = agenttest.MockTool
	ScriptedProvider = agenttest.ScriptedProvider
)

// TextResponse is a model response that streams one text part.
func TextResponse(text string) []saigetypes.Delta { return agenttest.TextResponse(text) }

// ToolCallResponse is a model response that calls one tool.
func ToolCallResponse(id, name string, args map[string]any) []saigetypes.Delta {
	return agenttest.ToolCallResponse(id, name, args)
}
