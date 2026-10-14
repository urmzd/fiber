package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/urmzd/duraturo"
	"github.com/urmzd/duraturo/pkg/run"

	"github.com/urmzd/mandatum/pkg/agentrt"
	"github.com/urmzd/mandatum/pkg/agentrt/payload"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// # Usage
//
// A run's usage is counted from the usage.call and usage.child events its
// attempts publish, never from a figure an executor returns: the events are
// reported as each call completes, so they exist for a turn that fails or is
// stopped halfway, and a total built from them always equals the sum of the
// calls a subscriber saw.
//
// What is counted, across duraturo's retries and replays:
//
//   - Every model call any attempt made, once. An attempt that fails after
//     spending tokens did spend them, so its calls are written to the ledger
//     (usageRecordName) before the failure is returned, and the attempt that
//     finally runs the agent to an outcome reads them and counts them with its
//     own. The record is per attempt and write-once.
//   - Nothing for a replay. The agent activity's outcome is memoized, so a
//     replay of the workflow body reads the recorded total and calls no model.
//   - A child run once, however many attempts of the parent asked for it: its
//     report is keyed by the child's run id.
//
// Keys come from saige (UsageDelta.AccountingID) when it reports one, and are
// otherwise "<run>/a<attempt>[/<agent>.<sub>...]/<n>", which no other call of
// any attempt can share. payload.Tally counts each key once, which is what
// makes a report delivered or relayed twice harmless.
//
// The failed attempts' records need the ledger the runs execute from
// (WithStopLedger). Without it the total counts the attempt that produced the
// outcome and the per-call events still report every call.

// usageRecordName is the ledger record an attempt that failed after spending
// tokens writes its calls to: "mandatum.usage.a<attempt>", as an event record
// "event:mandatum.usage.a<attempt>#0", the shape the ledger expects from
// outside a call. The workflow body claims the ones the outcome counted.
const usageRecordName = "mandatum.usage.a"

func usageRecordKey(attempt int) string {
	return "event:" + usageRecordName + strconv.Itoa(attempt) + "#0"
}

// spent is what one attempt reported, as its usage record holds it.
type spent struct {
	Attempt  int                  `json:"attempt"`
	Calls    []payload.UsageCall  `json:"calls,omitempty"`
	Children []payload.UsageChild `json:"children,omitempty"`
}

func (s *spent) empty() bool { return len(s.Calls) == 0 && len(s.Children) == 0 }

// observe files a usage chunk the attempt relayed, and ignores every other
// kind. A body that does not decode is ignored too: it was published as it
// came, and a malformed report cannot be counted.
func (s *spent) observe(c agentrt.Chunk) {
	switch c.Kind {
	case envelope.KindUsageCall:
		var call payload.UsageCall
		if json.Unmarshal(c.Payload, &call) == nil {
			s.Calls = append(s.Calls, call)
		}
	case envelope.KindUsageChild:
		var child payload.UsageChild
		if json.Unmarshal(c.Payload, &child) == nil {
			s.Children = append(s.Children, child)
		}
	}
}

func (s *spent) into(t *payload.Tally) {
	for _, c := range s.Calls {
		t.Call(c)
	}
	for _, c := range s.Children {
		t.Child(c)
	}
}

// meter is spent, safe to observe from the executor's goroutines.
type meter struct {
	mu sync.Mutex
	s  spent
}

func (m *meter) observe(c agentrt.Chunk) {
	if c.Kind.Family() != "usage" {
		return
	}
	m.mu.Lock()
	m.s.observe(c)
	m.mu.Unlock()
}

func (m *meter) snapshot() spent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return spent{
		Attempt:  m.s.Attempt,
		Calls:    append([]payload.UsageCall(nil), m.s.Calls...),
		Children: append([]payload.UsageChild(nil), m.s.Children...),
	}
}

// priorUsage reads what earlier attempts of a run spent and failed: their
// usage records, in attempt order, and the attempts they belong to.
//
// It runs inside the agent activity, whose result is memoized, so what it read
// is recorded with the outcome and a replay does not read again. A ledger that
// cannot be read is logged and the earlier attempts are left out: a run is not
// failed over its accounting, and their calls are still on the bus.
func (r *Runtime) priorUsage(ctx context.Context, runID string, attempt int) ([]spent, []int) {
	if r.stops == nil || attempt <= 1 {
		return nil, nil
	}
	_, records, err := r.stops.Load(ctx, runID)
	if err != nil {
		r.logger.Warn("mandatum/runtime: could not read the usage of earlier attempts; the total counts this one",
			"run_id", runID, "error", err)
		return nil, nil
	}
	prior := usageRecords(records)
	var out []spent
	var attempts []int
	for _, s := range prior {
		if s.Attempt < attempt {
			out = append(out, s)
			attempts = append(attempts, s.Attempt)
		}
	}
	return out, attempts
}

// usageRecords decodes the usage records among a run's records, in attempt
// order.
func usageRecords(records []run.Record) []spent {
	var out []spent
	for _, rec := range records {
		if !strings.HasPrefix(rec.Key, "event:"+usageRecordName) || rec.Status != run.RecordOK {
			continue
		}
		var s spent
		if err := json.Unmarshal(rec.Output, &s); err != nil || usageRecordKey(s.Attempt) != rec.Key {
			continue
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Attempt < out[j].Attempt })
	return out
}

// recordSpent writes what a failed attempt spent on the run, so the attempt
// that finally produces the outcome counts it. Write-once: a second write for
// the same attempt keeps the first.
func (r *Runtime) recordSpent(ctx context.Context, runID string, s spent) {
	if r.stops == nil || s.empty() {
		return
	}
	body, err := json.Marshal(s)
	if err == nil {
		_, err = r.stops.Record(context.WithoutCancel(ctx), run.Record{
			RunID: runID, Key: usageRecordKey(s.Attempt), Kind: run.KindEvent,
			Name: usageRecordName + strconv.Itoa(s.Attempt), Codec: "application/json",
			Status: run.RecordOK, Output: body, Attempt: s.Attempt,
		})
	}
	if err != nil && !errors.Is(err, run.ErrAlreadyRecorded) {
		r.logger.Warn("mandatum/runtime: could not record the usage of a failed attempt; the run's total will not count it",
			"run_id", runID, "attempt", s.Attempt, "calls", len(s.Calls), "error", err)
	}
}

// claimUsage reads, from the workflow body, the usage records the agent's
// outcome counted, so the ledger keeps no record that no call read. Which ones
// is decided by the memoized outcome, so every replay claims the same set. The
// claim is advisory: the total is already decided.
func (r *Runtime) claimUsage(ctx context.Context, in Input, attempts []int) {
	for _, a := range attempts {
		_, err := duraturo.Event[json.RawMessage](ctx, usageRecordName+strconv.Itoa(a))
		switch {
		case err == nil, errors.Is(err, run.ErrParked):
		default:
			r.logger.Warn("mandatum/runtime: could not claim a usage record",
				"run_id", RunID(in.Tag.Tenant, in.Tag.ID), "attempt", a, "error", err)
		}
	}
}

// RecordedUsage is what a run spent, as its ledger records hold it. It is for
// a run that has no output to read it from: one that failed. For a run that
// ended any other way, Output.Usage is the same figure.
//
// The agent's recorded outcome holds the total when the agent finished and
// something after it failed the run. Otherwise the total is the usage records
// of the attempts that failed, which is every model call the run made. It
// reports false when the records hold neither.
func RecordedUsage(records []run.Record) (payload.RunUsage, bool) {
	for _, rec := range records {
		if rec.Name != ActivityAgent || rec.Kind != run.KindActivity || rec.Status != run.RecordOK {
			continue
		}
		var out Outcome
		if json.Unmarshal(rec.Output, &out) == nil && out.Usage != nil {
			return *out.Usage, true
		}
	}
	prior := usageRecords(records)
	if len(prior) == 0 {
		return payload.RunUsage{}, false
	}
	var t payload.Tally
	for i := range prior {
		prior[i].into(&t)
	}
	return t.Usage(), true
}

// usageReporterKey carries, on a Delegator's context, how a child run's usage
// is put on the parent's stream.
type usageReporterKey struct{}

// ReportChildUsage rolls a child run up into the run whose turn delegated to
// it. A Delegator calls it with the context it was given once the run it
// started for the delegation has ended, with that run's total
// (Output.Usage.Total, or RecordedUsage for a run that failed):
//
//	out, err := rt.Result(ctx, childRunID)
//	...
//	err = runtime.ReportChildUsage(ctx, childRunID, target, out.Usage.Total)
//
// It publishes a usage.child event on the parent's stream, and the parent's
// total counts it in Children. A child is counted once however many times it
// is reported, keyed by its run id, so a parent attempt that delegates again
// after a retry, and joins the same child run, does not count it twice.
//
// It returns ErrInvalid outside a Delegator's context.
func ReportChildUsage(ctx context.Context, runID, agent string, total payload.Usage) error {
	report, ok := ctx.Value(usageReporterKey{}).(func(payload.UsageChild) error)
	if !ok || report == nil {
		return fmt.Errorf("%w: ReportChildUsage is called with the context a Delegator was given", ErrInvalid)
	}
	if strings.TrimSpace(runID) == "" {
		return fmt.Errorf("%w: a child run's usage needs its run id", ErrInvalid)
	}
	return report(payload.UsageChild{
		AccountingID: payload.ChildRunAccountingID(runID),
		RunID:        runID,
		Agent:        agent,
		Depth:        1,
		Usage:        total,
	})
}

// relayUsage rewrites a usage chunk that came up from a delegated sub-task so
// that it reads one level deeper, and reports false for any other kind: a
// sub-task's conversation is its parent's tool result, never its stream.
func relayUsage(c agentrt.Chunk) (agentrt.Chunk, bool) {
	switch c.Kind {
	case envelope.KindUsageCall:
		var call payload.UsageCall
		if json.Unmarshal(c.Payload, &call) != nil {
			return agentrt.Chunk{}, false
		}
		call.Depth++
		b, err := payload.Encode(call)
		if err != nil {
			return agentrt.Chunk{}, false
		}
		return agentrt.Chunk{Kind: c.Kind, Payload: b}, true
	case envelope.KindUsageChild:
		var child payload.UsageChild
		if json.Unmarshal(c.Payload, &child) != nil {
			return agentrt.Chunk{}, false
		}
		child.Depth++
		b, err := payload.Encode(child)
		if err != nil {
			return agentrt.Chunk{}, false
		}
		return agentrt.Chunk{Kind: c.Kind, Payload: b}, true
	default:
		return agentrt.Chunk{}, false
	}
}

// accountingOf is the key prefix a turn's calls are reported under: the run
// and the attempt, or for a sub-task the prefix its parent handed it.
func accountingOf(req Request) string {
	if req.Accounting != "" {
		return req.Accounting
	}
	return req.RunID + "/a" + strconv.Itoa(max(1, req.Attempt))
}
