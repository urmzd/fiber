package registry

import (
	"context"
	"fmt"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentspec"
)

// Specs resolves agent definitions for pkg/runtime. It satisfies runtime.Specs
// structurally.
//
// The two methods are the two moments a run has an opinion about time. Latest
// is asked once, at accept time, and is the only place "current" is ever
// resolved; At is asked on every execution with the number the run pinned, and
// must answer identically forever — after the agent has been revised past that
// number and after it has been deleted. The registry's tombstone is what makes
// the second promise keepable, and this type is where the promise is spent.
type Specs struct{ store Store }

// NewSpecs returns the revision resolver for store.
func NewSpecs(store Store) *Specs { return &Specs{store: store} }

// Latest resolves the current definition of an agent. It is a control-plane
// read: a deleted agent has no current definition and reports ErrNotFound.
func (s *Specs) Latest(ctx context.Context, tenant, agent string) (agentrt.Revision, error) {
	return s.at(ctx, tenant, agent, 0)
}

// At resolves one pinned revision, deleted or not.
func (s *Specs) At(ctx context.Context, tenant, agent string, rev int) (agentrt.Revision, error) {
	if rev < agentspec.FirstRev {
		return agentrt.Revision{}, fmt.Errorf("%w: a pinned revision counts from %d, got %d", ErrInvalid, agentspec.FirstRev, rev)
	}
	return s.at(ctx, tenant, agent, rev)
}

func (s *Specs) at(ctx context.Context, tenant, agent string, rev int) (agentrt.Revision, error) {
	got, err := s.store.Get(ctx, tenant, agent, rev)
	if err != nil {
		return agentrt.Revision{}, err
	}
	return toRuntimeRevision(got), nil
}

func toRuntimeRevision(r agentspec.Revision) agentrt.Revision {
	spec := agentrt.Spec{
		Name:         r.Spec.Name,
		Description:  r.Spec.Description,
		Model:        r.Spec.Model,
		Provider:     r.Spec.Provider,
		SystemPrompt: r.Spec.SystemPrompt,
		Tools:        r.Spec.Tools,
		Approvals:    r.Spec.Approvals,
		Approvers:    r.Spec.Approvers,
		SelfApproval: r.Spec.SelfApproval,
		Answerers:    r.Spec.Answerers,
		TaggedOnly:   r.Spec.TaggedOnly,
		FallbackOn:   r.Spec.FallbackOn,
		Access: agentrt.Access{
			Spawn:          r.Spec.Access.Spawn,
			WorkspaceAreas: r.Spec.Access.WorkspaceAreas,
		},
	}
	for _, src := range r.Spec.Sources {
		spec.Sources = append(spec.Sources, agentrt.Source{
			Name:    src.Name,
			URI:     src.URI,
			Options: src.Options,
		})
	}
	for _, fb := range r.Spec.Fallbacks {
		spec.Fallbacks = append(spec.Fallbacks, agentrt.ModelRef{Provider: fb.Provider, Model: fb.Model})
	}
	return agentrt.Revision{
		Spec:      spec,
		Rev:       r.Rev,
		Hash:      r.Hash,
		CreatedAt: r.CreatedAt,
		CreatedBy: r.CreatedBy,
	}
}
