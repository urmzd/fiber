package author

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/urmzd/fiber/pkg/connector"
)

// KnowledgeToolName is the action a configured Knowledge contributes. It is
// not part of the built-in agent's default grant: a deployment that sets
// Config.Knowledge adds this name to the spec it seeds.
const KnowledgeToolName = "author_search_knowledge"

// Passage is one retrieved piece of reference material.
type Passage struct {
	// Title names the document the passage came from.
	Title string `json:"title,omitempty"`
	// URI locates it.
	URI string `json:"uri,omitempty"`
	// Text is the passage itself.
	Text string `json:"text"`
}

// Knowledge is the seam for a knowledge search tool: prompt-writing guidance,
// house conventions, examples of agents that worked.
//
// It is declared here instead of imported so that this package depends on the
// control plane and nothing else. Whatever implements search adapts to this
// interface at the composition root. The tenant is passed by this package from
// the run, in the same way it is for the registry, and is never the model's to
// choose.
type Knowledge interface {
	Search(ctx context.Context, tenant, query string, limit int) ([]Passage, error)
}

// maxKnowledgeResults bounds what one search may return into a prompt.
const maxKnowledgeResults = 10

func knowledgeAction() actionDef {
	return actionDef{
		name:   KnowledgeToolName,
		effect: EffectRead,
		description: "Search this tenant's reference material on writing agents and prompts. " +
			"Use it before drafting when a convention or an example would help.",
		schema: `{
  "type": "object",
  "properties": {
    "query": {"type": "string", "description": "What to look for."},
    "limit": {"type": "integer", "description": "Maximum passages to return, at most 10. Defaults to 5."}
  },
  "required": ["query"],
  "additionalProperties": false
}`,
		run: (*Connector).searchKnowledge,
	}
}

func (c *Connector) searchKnowledge(ctx context.Context, who call, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Query string `json:"query"`
		Limit int    `json:"limit"`
	}
	if err := decode(raw, &args); err != nil {
		return connector.Result{}, err
	}
	if strings.TrimSpace(args.Query) == "" {
		return connector.Result{}, fmt.Errorf("%w: search needs a query", ErrInvalid)
	}
	if c.knowledge == nil {
		return connector.Result{}, fmt.Errorf("%w: no knowledge search is configured", connector.ErrUnsupported)
	}
	limit := args.Limit
	if limit <= 0 {
		limit = 5
	}
	limit = min(limit, maxKnowledgeResults)
	passages, err := c.knowledge.Search(ctx, who.Tenant, args.Query, limit)
	if err != nil {
		return connector.Result{}, fmt.Errorf("author: knowledge search: %w", err)
	}
	if len(passages) > limit {
		passages = passages[:limit]
	}
	if len(passages) == 0 {
		return result("no passages matched", passages)
	}
	var b strings.Builder
	for i, p := range passages {
		if i > 0 {
			b.WriteString("\n\n")
		}
		fmt.Fprintf(&b, "[%d] %s %s\n%s", i+1, p.Title, p.URI, p.Text)
	}
	return result(b.String(), passages)
}
