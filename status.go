package fiber

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	drun "github.com/urmzd/anchor/pkg/run"

	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/runtime"
)

// ErrNotFound reports a run that does not exist or belongs to another
// tenant. The two are one error on purpose: telling them apart would confirm
// existence across a tenant boundary.
var ErrNotFound = errors.New("fiber: not found")

// Status is where a run is in its lifecycle. It mirrors the lifecycle.*
// event kinds: status answers "where is it now", the events answer "how did
// it get there".
type Status string

// Run statuses.
const (
	StatusAccepted  Status = "accepted"
	StatusParked    Status = "parked"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	// StatusStopped is a run that ended because it was asked to. It is
	// terminal, and it is neither completed nor failed.
	StatusStopped Status = "stopped"
)

// Terminal reports whether a run in s has ended.
func (s Status) Terminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusStopped
}

// Run is the durable record of one invocation. It is a projection of the
// ledger, so reading one never touches a worker and polling it is cheap.
type Run struct {
	ID     string
	Tenant string
	Agent  string
	Rev    int
	Origin string
	// Channel is where the run's events are published.
	Channel string
	Status  Status
	Tag     envelope.Tag
	LastSeq uint64
	Error   string
	// StopReason and StoppedBy say why a stopped run was stopped and who
	// asked. Empty unless Status is StatusStopped.
	StopReason string
	StoppedBy  string
	// Usage is what the run spent, nil while nothing is recorded.
	Usage *payload.RunUsage
	// Question is what the run is waiting on a person for, set while
	// Status is StatusParked.
	Question *payload.Question

	CreatedAt time.Time
	EndedAt   time.Time
}

// Status reads a run's durable record, scoped to tenant. A run of another
// tenant is ErrNotFound, exactly like one that does not exist.
//
// The record trails the run's terminal event briefly. A run publishes
// lifecycle.completed, failed or stopped from inside its turn, and the worker
// records how it ended only once the turn has returned, so a Status read the
// moment that event arrives can still report accepted. The ledger is the
// authority: a worker that dies in between leaves the run pending, and the
// next attempt replays it. A caller that needs the two to agree polls until
// the status is Terminal.
func (a *App) Status(ctx context.Context, tenant, runID string) (Run, error) {
	rec, err := a.ledger.GetRun(ctx, runID)
	if err != nil {
		if errors.Is(err, drun.ErrNotFound) {
			return Run{}, fmt.Errorf("%w: run %q", ErrNotFound, runID)
		}
		return Run{}, fmt.Errorf("fiber: read run %q: %w", runID, err)
	}
	var in runtime.Input
	if err := json.Unmarshal(rec.Input, &in); err != nil {
		return Run{}, fmt.Errorf("fiber: decode run %q input: %w", runID, err)
	}
	if in.Tag.Tenant != tenant {
		return Run{}, fmt.Errorf("%w: run %q", ErrNotFound, runID)
	}
	out := Run{
		ID:        rec.ID,
		Tenant:    in.Tag.Tenant,
		Agent:     in.Tag.Agent,
		Rev:       in.Rev,
		Origin:    in.Tag.Origin,
		Status:    statusOf(rec.Status),
		Tag:       in.Tag,
		Error:     rec.Error,
		CreatedAt: rec.CreatedAt,
		EndedAt:   rec.CompletedAt,
	}
	out.Channel = runtime.ChannelOf(in.Tag)
	// LastSeq is the run's own answer: the sequence of its final event,
	// which only a finished run has.
	if rec.Status == drun.StatusSucceeded && len(rec.Output) > 0 {
		var result runtime.Output
		if err := json.Unmarshal(rec.Output, &result); err == nil {
			out.LastSeq = result.LastSeq
			// The ledger knows two endings. A stopped run is the third,
			// told apart by what the run returned.
			if result.Stopped != nil {
				out.Status = StatusStopped
				out.StopReason = result.Stopped.Reason
				out.StoppedBy = result.Stopped.By
			}
			out.Usage = result.Usage
		}
	}
	if rec.Status == drun.StatusPending {
		if q, ok, err := a.rt.PendingQuestion(ctx, tenant, runID); err == nil && ok {
			out.Status = StatusParked
			out.Question = &q
		}
	}
	// A failed run has no output; what its attempts spent is on its records.
	if rec.Status == drun.StatusFailed {
		if _, records, err := a.ledger.Load(ctx, runID); err == nil {
			if u, ok := runtime.RecordedUsage(records); ok {
				out.Usage = &u
			}
		}
	}
	return out, nil
}

// statusOf maps a ledger status onto a run's. The ledger has no word for
// stopped (Status reads that from the output) and no "running": assignment
// is flow state in the queue, so a pending run is reported as accepted,
// which is the only thing the ledger knows about it.
func statusOf(s drun.RunStatus) Status {
	switch s {
	case drun.StatusSucceeded:
		return StatusCompleted
	case drun.StatusFailed:
		return StatusFailed
	default:
		return StatusAccepted
	}
}
