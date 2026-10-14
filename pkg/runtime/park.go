package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/urmzd/duraturo"
	"github.com/urmzd/duraturo/pkg/run"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/mandatum/pkg/agentrt"
	"github.com/urmzd/mandatum/pkg/agentrt/payload"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// Parking a run on a person.
//
// A turn can need a person: a tool call that needs approval, or the agent's
// ask_person tool (see pkg/agentrt's park.go for how a turn ends at the
// question and how a later turn continues it). This file makes that wait
// durable.
//
// # The wait is a duraturo event
//
// The agent activity returns a parked outcome: the question, and everything a
// continuation needs. Its result is memoized like any other. The workflow body
// then
//
//  1. announces the question once, as a duraturo step that publishes
//     lifecycle.parked with the question, so the step's record is the question
//     as the run asked it ("mandatum.question.<id>#0"), and a replay neither
//     asks again nor announces again;
//  2. reads the question's resolution, a duraturo event record
//     ("event:mandatum.answer.<id>#0"). With none, the body returns
//     run.ErrParked: the worker settles the run, it holds no goroutine and no
//     lease, it costs no retry budget, and it survives any restart, because
//     the question and the turn are in the ledger;
//  3. with one, continues the turn with the answer as the
//     mandatum.turn.resume activity, whose input is the parked outcome and
//     the answer, both read from records.
//
// The resolution is write-once and the first write is the only one that
// counts, which is what makes an answer exactly-once: Answer writes it, a stop
// writes it, and an expiry writes it, and every one of them reads back
// whichever was first. The body decides only from that record, so a replay
// decides the same way whatever has been asked since.
//
// # Waking
//
// Answer and Stop enqueue the run once they have written the resolution
// (WithWaker), and the next attempt replays to the question and continues.
// Without a waker, or if the enqueue is lost, the worker's janitor re-enqueues
// every pending run on its own interval, and the run continues then.
//
// # Expiry
//
// A question has a deadline (WithQuestionTTL). Nothing wakes a run at that
// moment: the next attempt to find the question unanswered and past its
// deadline (the janitor's poll, or an answer that arrives late) writes the
// question's default as its resolution: a denial for an approval, the stated
// default for a choice or a reply, or "nobody answered", which the agent is
// told. Reading the clock only decides whether to write that record; what the
// run does is decided by the record, as for any answer. A question is never
// expired in the attempt that asked it, so a person always sees it first.
//
// # Stopping a parked run
//
// Stop records the request and then resolves the pending question as
// stopped, and wakes the run, which ends lifecycle.stopped without resuming
// its turn: nothing waits for a worker to pick a turn up that is not running.
// A stop recorded while the turn was still running, before it parked, is found
// the same way when the body reaches the question.

// Record names. A question is a duraturo step the body records; its
// resolution is an event record anything outside the run may write.
const (
	questionStepPrefix = "mandatum.question."
	answerRecordPrefix = "mandatum.answer."
)

func questionStepKey(id string) string  { return questionStepPrefix + id + "#0" }
func answerRecordName(id string) string { return answerRecordPrefix + id }
func answerRecordKey(id string) string  { return "event:" + answerRecordName(id) + "#0" }
func isQuestionRecord(rec run.Record) bool {
	return rec.Kind == run.KindStep && strings.HasPrefix(rec.Name, questionStepPrefix)
}

// Continue is the input of the activity that continues a parked turn.
type Continue struct {
	// Input is the run's pinned input.
	Input Input `json:"input"`
	// Seq numbers the continuation within the run, from 1.
	Seq int `json:"seq"`
	// Parked is the turn to continue, with the answers its pending calls
	// have, the new one included.
	Parked agentrt.Parked `json:"parked"`
}

// resolution is how a question was resolved, as its event record holds it.
type resolution struct {
	QuestionID string          `json:"question_id"`
	Answer     *payload.Answer `json:"answer,omitempty"`
	Stopped    *agentrt.Stop   `json:"stopped,omitempty"`
}

// Waker enqueues a run so a worker replays it. Every duraturo queue
// implements it.
type Waker interface {
	Enqueue(ctx context.Context, runID string, delay time.Duration) error
}

// WithWaker wakes a parked run as soon as its question is answered or it is
// stopped. Pass the queue the runtime's duraturo client uses. Without it a
// parked run continues when the worker's janitor next polls it.
func WithWaker(w Waker) Option {
	return func(r *Runtime) { r.waker = w }
}

// WithQuestionTTL sets how long a question waits for an answer before its
// default applies (default agentrt.DefaultQuestionTTL).
func WithQuestionTTL(d time.Duration) Option {
	return func(r *Runtime) {
		if d > 0 {
			r.questionTTL = d
		}
	}
}

// Errors an answer can meet.
var (
	// ErrNoQuestion reports a question that does not exist, on a run that
	// does not exist or belongs to another tenant. The three are one error
	// on purpose: telling them apart would confirm a run across tenants.
	ErrNoQuestion = errors.New("runtime: no such question")
	// ErrQuestionClosed reports a question that can no longer be answered:
	// the run was stopped while it waited, or it ended.
	ErrQuestionClosed = errors.New("runtime: the question is closed")
	// ErrAnswerUnavailable reports a runtime that cannot record an answer:
	// it has no ledger (WithStopLedger).
	ErrAnswerUnavailable = errors.New("runtime: answers are not available")
)

// awaitAnswer announces a parked turn's question once and returns its
// resolution, or run.ErrParked while there is none.
func (r *Runtime) awaitAnswer(ctx context.Context, at *attemptLog, out Outcome) (resolution, error) {
	q := out.Parked.Question
	runID := RunID(at.in.Tag.Tenant, at.in.Tag.ID)
	fresh := false
	if _, err := duraturo.Step(ctx, questionStepPrefix+q.ID, func(ctx context.Context) (payload.Question, error) {
		fresh = true
		if err := at.publish(ctx, envelope.KindParked, payload.Lifecycle{
			Attempt: at.attempt, Rev: at.in.Rev, Reason: "waiting on a person", Question: &q,
		}); err != nil {
			return payload.Question{}, err
		}
		return q, nil
	}); err != nil {
		return resolution{}, err
	}

	raw, err := duraturo.Event[json.RawMessage](ctx, answerRecordName(q.ID))
	switch {
	case err == nil:
		return decodeResolution(raw)
	case !errors.Is(err, run.ErrParked):
		return resolution{}, err
	}

	// No resolution in this attempt's records. Three things can still
	// decide it now, and each does so by writing the record and adopting
	// whatever the record then holds.
	if r.stops != nil {
		// A stop recorded on the run ends the wait.
		var stop *agentrt.Stop
		if raw, err := duraturo.Event[json.RawMessage](ctx, stopRecordName); err == nil {
			var body payload.StopRequest
			if json.Unmarshal(raw, &body) == nil && body.RunID == runID {
				stop = &agentrt.Stop{Reason: body.Reason, By: body.By}
			}
		} else if s, ok := r.recordedStop(ctx, runID); ok {
			stop = &s
		}
		if stop != nil {
			return r.resolve(ctx, runID, resolution{QuestionID: q.ID, Stopped: stop})
		}
		// The rest only in an attempt that replayed the turn rather than ran
		// it: an attempt executes at most one turn, because its sequence
		// band, its usage record and its accounting keys are the attempt's.
		// The attempt that asked parks, and the answer's wake-up starts the
		// next one.
		if !fresh {
			// A resolution written after this attempt loaded its records is
			// read rather than waited for.
			if res, ok := r.storedResolution(ctx, runID, q.ID); ok {
				return res, nil
			}
			// A question past its deadline takes its default. Never in the
			// attempt that asked it: a person sees every question first.
			if !q.ExpiresAt.IsZero() && !r.now().Before(q.ExpiresAt) {
				return r.resolve(ctx, runID, resolution{QuestionID: q.ID, Answer: expired(q, r.now())})
			}
		}
	}
	return resolution{}, fmt.Errorf("runtime: run %s is waiting on a person for question %s: %w", runID, q.ID, err)
}

// expired is a question's default, as the answer it resolves to.
func expired(q payload.Question, now time.Time) *payload.Answer {
	a := payload.Answer{QuestionID: q.ID, Expired: true, At: now.UTC()}
	if q.Default != nil {
		a = *q.Default
		a.QuestionID, a.Expired, a.At = q.ID, true, now.UTC()
	}
	if q.Kind == payload.QuestionApproval && a.Decision == "" {
		a.Decision = payload.DecisionDeny
	}
	return &a
}

// resolve writes a question's resolution, once, and returns what the record
// holds: this one, or whichever was written first.
func (r *Runtime) resolve(ctx context.Context, runID string, res resolution) (resolution, error) {
	body, err := json.Marshal(res)
	if err != nil {
		return resolution{}, fmt.Errorf("runtime: resolve question %s: %w", res.QuestionID, err)
	}
	stored, err := r.stops.Record(context.WithoutCancel(ctx), run.Record{
		RunID: runID, Key: answerRecordKey(res.QuestionID), Kind: run.KindEvent,
		Name: answerRecordName(res.QuestionID), Codec: "application/json",
		Status: run.RecordOK, Output: body,
	})
	switch {
	case err == nil:
		return res, nil
	case errors.Is(err, run.ErrAlreadyRecorded):
		return decodeResolution(stored.Output)
	default:
		return resolution{}, fmt.Errorf("runtime: resolve question %s: %w", res.QuestionID, err)
	}
}

// storedResolution reads a question's resolution from the ledger.
func (r *Runtime) storedResolution(ctx context.Context, runID, questionID string) (resolution, bool) {
	_, records, err := r.stops.Load(ctx, runID)
	if err != nil {
		return resolution{}, false
	}
	for _, rec := range records {
		if rec.Key == answerRecordKey(questionID) && rec.Status == run.RecordOK {
			res, err := decodeResolution(rec.Output)
			return res, err == nil
		}
	}
	return resolution{}, false
}

func decodeResolution(raw []byte) (resolution, error) {
	var res resolution
	if err := json.Unmarshal(raw, &res); err != nil {
		return resolution{}, duraturo.NonRetryable(fmt.Errorf("runtime: decode a question's resolution: %w", err))
	}
	if res.Answer == nil && res.Stopped == nil {
		return resolution{}, duraturo.NonRetryable(errors.New("runtime: a question's resolution holds neither an answer nor a stop"))
	}
	return res, nil
}

// stoppedWhileParked ends a run that was stopped while it waited on a person.
// Its turn is not resumed: the run ends with what it had said before it asked,
// and what it had spent.
func (r *Runtime) stoppedWhileParked(ctx context.Context, at *attemptLog, out Outcome, stop agentrt.Stop, parks int) (Output, error) {
	r.claimStop(ctx, at.in)
	stop.Reason = clip(stop.Reason)
	if stop.Reason == "" {
		stop.Reason = agentrt.DefaultStopReason
	}
	final := Outcome{Text: out.Text, Prompt: out.Prompt, Stopped: &stop, Usage: out.Usage}
	if err := r.remember(ctx, at.in, final); err != nil {
		at.report(ctx, envelope.KindFailed, payload.Lifecycle{Attempt: at.attempt, Rev: at.in.Rev, Error: err.Error()})
		return Output{}, err
	}
	if err := at.publish(ctx, envelope.KindStopped, payload.Stopped{
		Attempt: at.attempt, Rev: at.in.Rev, Reason: stop.Reason, By: stop.By, Text: out.Text, Usage: out.Usage,
	}); err != nil {
		return Output{}, err
	}
	return Output{
		Rev: at.in.Rev, Text: out.Text, LastSeq: at.band.close, Attempt: at.attempt,
		Events: out.Chunks + 3 + 2*parks + 1, Stopped: &stop, Usage: out.Usage,
	}, nil
}

// AnswerRequest answers one question a run is parked on.
type AnswerRequest struct {
	// Tenant is the caller's, from its credential. A question of another
	// tenant's run is reported as ErrNoQuestion.
	Tenant string
	// RunID is the run.
	RunID string
	// QuestionID is the question, as payload.Question.ID reported it.
	QuestionID string
	// Answer is the answer: Decision and optionally Arguments for an
	// approval, Choice for a choice, Text for a reply, and an optional Note.
	// Its QuestionID, By, At and Expired are set here.
	Answer payload.Answer
	// By is who answered, from the caller's credential.
	By string
}

// Answer answers a question a run is parked on, and wakes the run.
//
// It is idempotent: the first answer recorded is the answer, and Answer
// returns it to every caller, with first reporting whether it was this call's.
// An answer that arrives after the question expired records the question's
// default instead, and returns that. An answer is checked against the
// question before it is recorded: a decision must be approve or deny, edited
// arguments must satisfy the tool's schema, a choice must be one of the
// options offered, and a reply must not be empty.
//
// It returns ErrNoQuestion for a question this tenant has no run with,
// ErrQuestionClosed when the run was stopped while it waited or has ended
// without the question being answered, an error matching ErrInvalid for an
// answer that does not fit the question, and ErrAnswerUnavailable without
// WithStopLedger.
func (r *Runtime) Answer(ctx context.Context, req AnswerRequest) (payload.Answer, bool, error) {
	if r.stops == nil {
		return payload.Answer{}, false, fmt.Errorf("%w: no ledger to record it in", ErrAnswerUnavailable)
	}
	req.RunID, req.QuestionID = strings.TrimSpace(req.RunID), strings.TrimSpace(req.QuestionID)
	if req.RunID == "" || req.QuestionID == "" {
		return payload.Answer{}, false, fmt.Errorf("%w: an answer names a run and a question", ErrInvalid)
	}
	rec, records, err := r.stops.Load(ctx, req.RunID)
	switch {
	case errors.Is(err, run.ErrNotFound):
		return payload.Answer{}, false, ErrNoQuestion
	case err != nil:
		return payload.Answer{}, false, fmt.Errorf("runtime: answer %s: read the run: %w", req.RunID, err)
	}
	var in Input
	if err := json.Unmarshal(rec.Input, &in); err != nil || in.Tag.Tenant != req.Tenant {
		return payload.Answer{}, false, ErrNoQuestion
	}
	q, ok := findQuestion(records, req.QuestionID)
	if !ok {
		return payload.Answer{}, false, ErrNoQuestion
	}
	for _, rr := range records {
		if rr.Key == answerRecordKey(q.ID) && rr.Status == run.RecordOK {
			return answered(rr.Output, false)
		}
	}
	if rec.Status.Terminal() {
		return payload.Answer{}, false, ErrQuestionClosed
	}

	now := r.now().UTC()
	res := resolution{QuestionID: q.ID}
	if !q.ExpiresAt.IsZero() && !now.Before(q.ExpiresAt) {
		res.Answer = expired(q, now)
	} else {
		a, err := checkAnswer(q, req.Answer)
		if err != nil {
			return payload.Answer{}, false, err
		}
		a.QuestionID, a.By, a.At, a.Expired = q.ID, clip(req.By), now, false
		res.Answer = &a
	}
	stored, err := r.resolve(ctx, req.RunID, res)
	if err != nil {
		return payload.Answer{}, false, err
	}
	r.wake(ctx, req.RunID)
	raw, _ := json.Marshal(stored)
	mine, _ := json.Marshal(res)
	return answered(raw, bytes.Equal(raw, mine) && !res.Answer.Expired)
}

// answered returns the answer a resolution record holds.
func answered(raw []byte, first bool) (payload.Answer, bool, error) {
	res, err := decodeResolution(raw)
	if err != nil {
		return payload.Answer{}, false, err
	}
	if res.Answer == nil {
		return payload.Answer{}, false, ErrQuestionClosed
	}
	return *res.Answer, first, nil
}

// findQuestion returns a question a run recorded.
func findQuestion(records []run.Record, id string) (payload.Question, bool) {
	for _, rec := range records {
		if rec.Key != questionStepKey(id) || rec.Status != run.RecordOK {
			continue
		}
		var q payload.Question
		if json.Unmarshal(rec.Output, &q) == nil && q.ID == id {
			return q, true
		}
	}
	return payload.Question{}, false
}

// CheckAnswer reports whether an answer fits its question, as Answer checks
// it: a decision is approve or deny and only an approval carries arguments,
// which must satisfy the tool's schema; a choice is one of the options; a
// reply has text. A host can check an answer before it submits one.
func CheckAnswer(q payload.Question, a payload.Answer) error {
	_, err := checkAnswer(q, a)
	return err
}

// checkAnswer checks an answer against its question and returns the part of
// it that is recorded.
func checkAnswer(q payload.Question, a payload.Answer) (payload.Answer, error) {
	out := payload.Answer{Note: clipQuestion(a.Note)}
	switch q.Kind {
	case payload.QuestionApproval:
		switch a.Decision {
		case payload.DecisionApprove:
		case payload.DecisionDeny:
			if len(bytes.TrimSpace(a.Arguments)) > 0 {
				return out, fmt.Errorf("%w: a denial cannot carry arguments", ErrInvalid)
			}
		default:
			return out, fmt.Errorf("%w: an approval is answered %q or %q, not %q", ErrInvalid, payload.DecisionApprove, payload.DecisionDeny, a.Decision)
		}
		out.Decision = a.Decision
		if args := bytes.TrimSpace(a.Arguments); len(args) > 0 {
			var value map[string]any
			if err := json.Unmarshal(args, &value); err != nil || value == nil {
				return out, fmt.Errorf("%w: edited arguments must be a JSON object", ErrInvalid)
			}
			if q.Tool != nil && len(q.Tool.Schema) > 0 {
				var schema saigetypes.ParameterSchema
				if err := json.Unmarshal(q.Tool.Schema, &schema); err == nil {
					if err := saigetypes.ValidateJSON(schema, value); err != nil {
						return out, fmt.Errorf("%w: edited arguments do not fit %s: %w", ErrInvalid, q.Tool.Name, err)
					}
				}
			}
			compact := &bytes.Buffer{}
			if err := json.Compact(compact, args); err != nil {
				return out, fmt.Errorf("%w: edited arguments: %w", ErrInvalid, err)
			}
			out.Arguments = compact.Bytes()
		}
	case payload.QuestionChoice:
		if !slices.Contains(q.Options, a.Choice) {
			return out, fmt.Errorf("%w: %q is not one of the options offered", ErrInvalid, a.Choice)
		}
		out.Choice = a.Choice
	case payload.QuestionReply:
		text := clipQuestion(a.Text)
		if text == "" {
			return out, fmt.Errorf("%w: a reply needs text", ErrInvalid)
		}
		out.Text = text
	default:
		return out, fmt.Errorf("%w: question %s has kind %q, which this runtime cannot answer", ErrInvalid, q.ID, q.Kind)
	}
	return out, nil
}

// clipQuestion trims an answer's free text and bounds it, on a rune boundary.
func clipQuestion(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= agentrt.MaxQuestionText {
		return s
	}
	end := agentrt.MaxQuestionText
	for end > 0 && s[end]&0xC0 == 0x80 {
		end--
	}
	return s[:end]
}

// wake enqueues a run whose question was resolved. A worker may be settling
// the attempt that parked it at this moment, and an enqueue of a run a worker
// still holds is dropped, so it is repeated twice more shortly after. A run
// already woken ignores the repeats.
func (r *Runtime) wake(ctx context.Context, runID string) {
	if r.waker == nil {
		return
	}
	ctx = context.WithoutCancel(ctx)
	if err := r.waker.Enqueue(ctx, runID, 0); err != nil {
		r.logger.Warn("mandatum/runtime: could not wake a parked run; the janitor will",
			"run_id", runID, "error", err)
	}
	go func() {
		for _, d := range []time.Duration{200 * time.Millisecond, time.Second} {
			time.Sleep(d)
			_ = r.waker.Enqueue(ctx, runID, 0)
		}
	}()
}

// PendingQuestion returns the question a run of tenant's is parked on, for a
// host that shows it. A run that is not parked, does not exist or is another
// tenant's reports false. It returns ErrAnswerUnavailable without
// WithStopLedger.
func (r *Runtime) PendingQuestion(ctx context.Context, tenant, runID string) (payload.Question, bool, error) {
	if r.stops == nil {
		return payload.Question{}, false, fmt.Errorf("%w: no ledger", ErrAnswerUnavailable)
	}
	rec, records, err := r.stops.Load(ctx, runID)
	switch {
	case errors.Is(err, run.ErrNotFound):
		return payload.Question{}, false, nil
	case err != nil:
		return payload.Question{}, false, fmt.Errorf("runtime: questions of %s: %w", runID, err)
	}
	var in Input
	if err := json.Unmarshal(rec.Input, &in); err != nil || in.Tag.Tenant != tenant || rec.Status.Terminal() {
		return payload.Question{}, false, nil
	}
	q, ok := pendingQuestion(records)
	return q, ok, nil
}

// pendingQuestion is the latest question among a run's records that has no
// resolution.
func pendingQuestion(records []run.Record) (payload.Question, bool) {
	resolved := map[string]bool{}
	for _, rec := range records {
		if rec.Kind == run.KindEvent && strings.HasPrefix(rec.Name, answerRecordPrefix) {
			resolved[strings.TrimPrefix(rec.Name, answerRecordPrefix)] = true
		}
	}
	var out payload.Question
	found := false
	for _, rec := range records {
		if !isQuestionRecord(rec) || rec.Status != run.RecordOK {
			continue
		}
		var q payload.Question
		if json.Unmarshal(rec.Output, &q) != nil || q.ID == "" || resolved[q.ID] {
			continue
		}
		if !found || q.At.After(out.At) {
			out, found = q, true
		}
	}
	return out, found
}

// stopParked resolves the question a run is parked on as stopped and wakes
// the run, which then ends without resuming its turn. It is called by Stop
// once the request is recorded.
func (r *Runtime) stopParked(ctx context.Context, runID string, body []byte) error {
	_, records, err := r.stops.Load(ctx, runID)
	if err != nil {
		return fmt.Errorf("runtime: stop %s: read its questions: %w", runID, err)
	}
	q, ok := pendingQuestion(records)
	if !ok {
		return nil
	}
	var req payload.StopRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("runtime: stop %s: %w", runID, err)
	}
	if _, err := r.resolve(ctx, runID, resolution{QuestionID: q.ID, Stopped: &agentrt.Stop{Reason: req.Reason, By: req.By}}); err != nil {
		return err
	}
	r.wake(ctx, runID)
	return nil
}

// childQuestionPoll is how often a Delegator's watch reads a child run for a
// question.
const childQuestionPoll = 100 * time.Millisecond

type childQuestionKey struct{}

// childQuestion returns how a child run a Delegator started is checked and
// read for a pending question: a run of the parent's tenant, other than the
// parent, in the ledger.
func (r *Runtime) childQuestion(tenant, parentRunID string) func(context.Context, string) (payload.ChildQuestion, bool, bool, error) {
	return func(ctx context.Context, runID string) (payload.ChildQuestion, bool, bool, error) {
		runID = strings.TrimSpace(runID)
		if r.stops == nil {
			return payload.ChildQuestion{}, false, false, fmt.Errorf("%w: watching a child run needs the run ledger (WithStopLedger)", ErrNotAChild)
		}
		if runID == "" || runID == parentRunID {
			return payload.ChildQuestion{}, false, false, fmt.Errorf("%w: %q", ErrNotAChild, runID)
		}
		rec, records, err := r.stops.Load(ctx, runID)
		switch {
		case errors.Is(err, run.ErrNotFound):
			return payload.ChildQuestion{}, false, false, fmt.Errorf("%w: no run %q", ErrNotAChild, runID)
		case err != nil:
			return payload.ChildQuestion{}, false, false, fmt.Errorf("runtime: read child run %s: %w", runID, err)
		}
		var in Input
		if err := json.Unmarshal(rec.Input, &in); err != nil || in.Tag.Tenant != tenant {
			return payload.ChildQuestion{}, false, false, fmt.Errorf("%w: no run %q", ErrNotAChild, runID)
		}
		if rec.Status.Terminal() {
			return payload.ChildQuestion{}, false, true, nil
		}
		q, ok := pendingQuestion(records)
		return payload.ChildQuestion{RunID: runID, Agent: in.Tag.Agent, Question: q}, ok, false, nil
	}
}

// WatchChildQuestions shows a child run's questions on the stream of the run
// that delegated to it, for as long as the delegation lasts. A Delegator that
// starts a run of its own for a delegation and waits on it calls it with the
// context it was given:
//
//	acc, err := rt.Accept(ctx, childTag)
//	...
//	err = runtime.WatchChildQuestions(ctx, acc.RunID)
//	out, err := rt.Result(ctx, acc.RunID)
//
// Each question the child parks on is published once on the parent's stream
// as lifecycle.child.parked, with the child's run id, so a host showing the
// parent shows it waiting on that question, and answers it by answering the
// child run. The watch reads the ledger, and ends when the child ends or the
// delegation returns.
//
// Only the run id comes from the host: the runtime refuses it with
// ErrNotAChild unless it is a run of the parent's tenant other than the
// parent. It needs the run ledger (WithStopLedger) and the Sandbox executor,
// and returns ErrInvalid outside a Delegator's context.
func WatchChildQuestions(ctx context.Context, runID string) error {
	watch, ok := ctx.Value(childQuestionKey{}).(func(context.Context, string) error)
	if !ok || watch == nil {
		return fmt.Errorf("%w: WatchChildQuestions is called with the context a Delegator was given", ErrInvalid)
	}
	return watch(ctx, runID)
}

// watchChild polls a child run for questions and reports each new one, until
// the child ends or ctx does.
func watchChild(ctx context.Context, read func(context.Context, string) (payload.ChildQuestion, bool, bool, error), runID string, report func(payload.ChildQuestion) error) error {
	if _, _, _, err := read(ctx, runID); err != nil {
		return err
	}
	go func() {
		seen := map[string]bool{}
		tick := time.NewTicker(childQuestionPoll)
		defer tick.Stop()
		for {
			cq, ok, ended, err := read(ctx, runID)
			if ended {
				return
			}
			if err == nil && ok && !seen[cq.Question.ID] {
				seen[cq.Question.ID] = true
				if report(cq) != nil {
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	return nil
}
