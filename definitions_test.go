package fiber

import (
	"context"

	"github.com/urmzd/fiber/pkg/definition"
)

// createAgent stores spec as version 1.0.0 of a definition, as setup.
func createAgent(src definition.Source, ctx context.Context, tenant string, spec definition.AgentSpec, by string) (definition.Record, error) {
	rec, err := definition.FromAgentSpec(spec, 1)
	if err != nil {
		return definition.Record{}, err
	}
	rec.Tenant, rec.CreatedBy = tenant, by
	return src.Put(ctx, rec)
}
