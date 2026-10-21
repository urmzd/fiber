package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/urmzd/anchor"

	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/internal/pgrename"
)

// errAcceptedBeforeRename is why a run accepted before fiber's rename ends
// failed. It reaches the ledger and lifecycle.failed as text.
var errAcceptedBeforeRename = errors.New("runtime: run accepted before fiber's rename")

// failRunFromBeforeRename is the workflow registered under the turn's name
// from before the rename (pgrename.LegacyActivityTurn). It is the only code
// that runs for such a run, and all it does is end it.
//
// That run recorded its activities, its questions and their answers under the
// old names, and the current turn reads only the new ones, so replaying it
// would re-run the model or wait on a question nothing can answer any more.
// Without this registration anchor's worker would retry the unknown name and
// then fail the run as "unknown workflow" with nothing published, so a surface
// showing the run would show it in progress forever. Here it fails once,
// terminally, with a reason on the ledger, on the run's topic and in the log.
// A parked run is reached the same way: the worker's janitor re-enqueues every
// pending run.
func (r *Runtime) failRunFromBeforeRename(ctx context.Context, in Input) (Output, error) {
	attempt := attemptOf(ctx)
	runID := RunID(in.Tag.Tenant, in.Tag.ID)
	err := anchor.NonRetryable(fmt.Errorf("%w: run %s was accepted as %q, and its records cannot be resumed under the current names; raise it again as a new tag (see docs/renaming.md)",
		errAcceptedBeforeRename, runID, pgrename.LegacyActivityTurn))
	r.logger.Error("fiber/runtime: failing a run accepted before the rename",
		"run_id", runID, "agent", in.Tag.Agent, "tenant", in.Tag.Tenant, "workflow", pgrename.LegacyActivityTurn)
	if b, berr := newBand(r.stride, attempt); berr == nil {
		at := &attemptLog{r: r, in: in, band: b, attempt: attempt}
		at.report(ctx, envelope.KindFailed, payload.Lifecycle{
			Attempt: attempt, Rev: in.Rev, Error: err.Error(), Terminal: true,
		})
	}
	// A child run from before the rename has a parent from before it, which
	// is failed the same way when this wakes it.
	r.answerParent(ctx, in, "", nil, err)
	return Output{}, err
}
