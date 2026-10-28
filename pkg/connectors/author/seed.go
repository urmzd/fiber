package author

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/urmzd/fiber/pkg/definition"
)

// AgentName is the built-in agent's name. It matches the connector's, so the
// agent people tag and the tools it holds read as one thing.
const AgentName = Name

// SeededBy is the author recorded on versions Seed writes. Seed uses it to
// tell its own versions from a person's.
const SeededBy = "system:author-seed"

//go:embed prompt.md
var systemPrompt string

// Spec returns the built-in agent for a model and provider, in the shape
// FromAgentSpec converts.
//
// The model and provider are parameters because they are the deployment's
// choice; everything else is the package's. With definition.ProviderOffline
// the agent builds and runs with no API key, which is how the tests exercise
// it.
//
// It grants every core action and nothing else: no sub-agents, no workspace
// area. The agent's authority is its tools, and its tools are gated by the
// two-step approval described in the package comment, not by this prompt.
func Spec(model, provider string) definition.AgentSpec {
	return definition.AgentSpec{
		Name:         AgentName,
		Description:  "Creates, reviews and manages this workspace's agents, one approved change at a time.",
		Model:        model,
		Provider:     provider,
		SystemPrompt: systemPrompt,
		Tools:        ToolNames(),
	}
}

// Seed makes sure tenant has the built-in agent, and reports the version now
// in effect. It is safe to call at every startup and from several replicas at
// once.
//
// It never takes a decision away from a person:
//
//   - No agent of that name has ever existed: it is written as 1.0.0.
//   - The latest version was written by Seed: when spec renders differently,
//     it is written as the next major version. An unchanged binary writes
//     nothing, and a new binary upgrades the prompt.
//   - The latest version was written by anyone else: it is left alone. A
//     person edited the built-in agent and their edit wins.
//   - The agent was retired: it stays retired, and Seed returns the zero
//     Record with no error. Someone retired it on purpose, and an agent that
//     reappears on every restart cannot be retired at all.
func Seed(ctx context.Context, src definition.Source, tenant string, spec definition.AgentSpec) (definition.Record, error) {
	if src == nil {
		return definition.Record{}, fmt.Errorf("%w: no definition source", ErrInvalid)
	}
	name := definition.NormalizeName(spec.Name)
	major := 1
	cur, err := src.Latest(ctx, tenant, name)
	switch {
	case errors.Is(err, definition.ErrRetired):
		return definition.Record{}, nil
	case errors.Is(err, definition.ErrNotFound):
	case err != nil:
		return definition.Record{}, fmt.Errorf("author: seed: %w", err)
	default:
		if cur.CreatedBy != SeededBy {
			return cur, nil
		}
		curMajor, err := definition.Major(cur.Version)
		if err != nil {
			return definition.Record{}, fmt.Errorf("author: seed: %s@%s: %w", cur.Name, cur.Version, err)
		}
		same, err := definition.FromAgentSpec(spec, curMajor)
		if err != nil {
			return definition.Record{}, fmt.Errorf("author: seed: %w", err)
		}
		if same.Version == cur.Version && same.Content == cur.Content && same.Policy.Hash() == cur.PolicyHash {
			return cur, nil
		}
		major = curMajor + 1
	}
	rec, err := definition.FromAgentSpec(spec, major)
	if err != nil {
		return definition.Record{}, fmt.Errorf("author: seed: %w", err)
	}
	rec.Tenant, rec.CreatedBy = tenant, SeededBy
	out, err := src.Put(ctx, rec)
	if errors.Is(err, definition.ErrExists) || errors.Is(err, definition.ErrRetired) {
		// Another replica or a person got there between the read and the
		// write. Either way what is there now is in effect.
		return current(ctx, src, tenant, name)
	}
	if err != nil {
		return definition.Record{}, fmt.Errorf("author: seed: %w", err)
	}
	return out, nil
}

func current(ctx context.Context, src definition.Source, tenant, name string) (definition.Record, error) {
	rec, err := src.Latest(ctx, tenant, name)
	if errors.Is(err, definition.ErrRetired) {
		return definition.Record{}, nil
	}
	if err != nil {
		return definition.Record{}, fmt.Errorf("author: seed: %w", err)
	}
	return rec, nil
}
