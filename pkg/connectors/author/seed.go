package author

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/urmzd/mandatum/pkg/agentspec"
	"github.com/urmzd/mandatum/pkg/registry"
)

// AgentName is the built-in agent's name. It matches the connector's, so the
// agent people tag and the tools it holds read as one thing.
const AgentName = Name

// SeededBy is the author recorded on revisions Seed writes. Seed uses it to
// tell its own revisions from a person's.
const SeededBy = "system:author-seed"

//go:embed prompt.md
var systemPrompt string

// Spec returns the built-in agent's definition for a model and provider.
//
// The model and provider are parameters because they are the deployment's
// choice; everything else is the package's. With agentspec.ProviderOffline the
// definition builds and runs with no API key, which is how the tests exercise
// it.
//
// It grants every core action and nothing else: no delegation, no workspace
// area. The agent's authority is its tools, and its tools are gated by the
// two-step approval described in the package comment, not by this prompt.
func Spec(model, provider string) agentspec.AgentSpec {
	return agentspec.AgentSpec{
		Name:         AgentName,
		Description:  "Creates, reviews and manages this workspace's agents, one approved change at a time.",
		Model:        model,
		Provider:     provider,
		SystemPrompt: systemPrompt,
		Tools:        ToolNames(),
	}.Normalize()
}

// Seed makes sure tenant has the built-in agent, and reports the revision now
// in effect. It is safe to call at every startup and from several replicas at
// once.
//
// It never takes a decision away from a person:
//
//   - No agent of that name has ever existed: it is created.
//   - The current revision was written by Seed: it is revised to spec. The
//     registry treats identical content as a no-op, so an unchanged binary
//     burns no revision number, and a new binary upgrades the prompt.
//   - The current revision was written by anyone else: it is left alone. A
//     person edited the built-in agent and their edit wins.
//   - The agent was deleted: it stays deleted, and Seed returns the zero
//     Revision with no error. Someone retired it on purpose, and an agent that
//     reappears on every restart cannot be retired at all.
func Seed(ctx context.Context, store registry.Store, tenant string, spec agentspec.AgentSpec) (agentspec.Revision, error) {
	if store == nil {
		return agentspec.Revision{}, fmt.Errorf("%w: no registry store", ErrInvalid)
	}
	cur, err := store.Get(ctx, tenant, spec.Name, 0)
	switch {
	case err == nil:
		if cur.CreatedBy != SeededBy {
			return cur, nil
		}
		rev, err := store.Revise(ctx, tenant, spec, cur.Rev, SeededBy)
		if errors.Is(err, registry.ErrConflict) {
			// Another replica or a person got there between the read and
			// the write. Either way the agent exists and is current.
			return current(ctx, store, tenant, spec.Name)
		}
		if err != nil {
			return agentspec.Revision{}, fmt.Errorf("author: seed: %w", err)
		}
		return rev, nil
	case !errors.Is(err, registry.ErrNotFound):
		return agentspec.Revision{}, fmt.Errorf("author: seed: %w", err)
	}

	// Not found by name is either "never existed" or "retired". History
	// tells them apart, because a tombstone keeps its past.
	switch hist, err := store.History(ctx, tenant, spec.Name); {
	case err == nil && len(hist) > 0:
		return agentspec.Revision{}, nil
	case err != nil && !errors.Is(err, registry.ErrNotFound):
		return agentspec.Revision{}, fmt.Errorf("author: seed: %w", err)
	}

	rev, err := store.Create(ctx, tenant, spec, SeededBy)
	if errors.Is(err, registry.ErrExists) {
		return current(ctx, store, tenant, spec.Name)
	}
	if err != nil {
		return agentspec.Revision{}, fmt.Errorf("author: seed: %w", err)
	}
	return rev, nil
}

func current(ctx context.Context, store registry.Store, tenant, name string) (agentspec.Revision, error) {
	rev, err := store.Get(ctx, tenant, name, 0)
	if err != nil {
		return agentspec.Revision{}, fmt.Errorf("author: seed: %w", err)
	}
	return rev, nil
}
