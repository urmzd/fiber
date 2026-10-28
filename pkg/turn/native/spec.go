package native

import (
	"errors"
	"fmt"
	"time"

	"github.com/urmzd/fiber/pkg/turn"
)

// ErrInvalid reports a revision this package cannot build an agent from.
// Callers match with errors.Is.
var ErrInvalid = errors.New("native: invalid")

// Spec is what an agent is: see turn.Spec. A Spec is immutable: editing an
// agent makes the next Revision.
type Spec = turn.Spec

// ModelRef names one model behind one provider: one of a spec's fallbacks.
type ModelRef = turn.ModelRef

// Source is one retrieval corpus the agent may ground answers in.
type Source = turn.Source

// Access is what the agent may do beyond talking: see turn.Policy.
type Access = turn.Policy

// Revision is one immutable version of an agent: the spec, plus which version
// it is and who put it there.
//
// Rev is the value a run pins and an event reports. Hash is the content hash of
// the spec, which lets a replaying run assert that the definition it is about
// to execute is the one it originally pinned. An immutable revision whose
// content changed is corruption, and it must fail loudly rather than quietly
// run something else.
type Revision struct {
	Spec      Spec      `json:"spec"`
	Rev       int       `json:"rev"`
	Hash      string    `json:"hash,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	CreatedBy string    `json:"created_by,omitempty"`
}

// Validate checks the fields this package needs to construct an agent. It does
// not judge the fields only the control plane cares about (description,
// authorship), and it does not resolve tools or sources: those depend on what a
// deployment has registered, and a spec naming a tool this process has not
// loaded is a deployment error, not an invalid definition.
func (r Revision) Validate() error {
	switch {
	case r.Rev <= 0:
		return fmt.Errorf("%w: revision number %d is not positive; a pinned revision counts from 1", ErrInvalid, r.Rev)
	case r.Spec.Name == "":
		return fmt.Errorf("%w: revision %d has no agent name", ErrInvalid, r.Rev)
	case r.Spec.Provider == "":
		return fmt.Errorf("%w: agent %q revision %d names no provider", ErrInvalid, r.Spec.Name, r.Rev)
	}
	return nil
}
