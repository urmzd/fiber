package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/agentrt/payload"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// fakeAnswerer records what the edge asked the runtime to record.
type fakeAnswerer struct {
	mu   sync.Mutex
	got  []AnswerRequest
	fail error
}

func (f *fakeAnswerer) Answer(_ context.Context, req AnswerRequest) (payload.Answer, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return payload.Answer{}, false, f.fail
	}
	f.got = append(f.got, req)
	a := req.Answer
	a.QuestionID, a.By = req.QuestionID, req.By
	return a, len(f.got) == 1, nil
}

func (f *fakeAnswerer) requests() []AnswerRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]AnswerRequest(nil), f.got...)
}

func answerRequest(runID, qid string, a *mandatumv1.Answer) *connect.Request[mandatumv1.AnswerRequest] {
	return connect.NewRequest(&mandatumv1.AnswerRequest{RunId: runID, QuestionId: qid, Answer: a})
}

// The tenant of an answer is the credential's, and so is who answered: only a
// credential trusted to act for others may name the person. What the caller
// says reaches the runtime; what the server owns does not.
func TestAnAnswerCarriesTheCredentialsTenantAndAnswerer(t *testing.T) {
	for _, tc := range []struct {
		name, token string
		by          string
		want        AnswerRequest
	}{
		{"an ordinary credential is itself", tokenAcme, "someone-else",
			AnswerRequest{Tenant: tenantAcme, RunID: "r1", QuestionID: "q1", By: subjectAcme, Via: subjectAcme}},
		{"a surface names the person", tokenSurface, "slack:U1",
			AnswerRequest{Tenant: tenantAcme, RunID: "r1", QuestionID: "q1", By: "slack:U1", Via: "slack-surface"}},
		{"another tenant is itself", tokenOther, "",
			AnswerRequest{Tenant: tenantOther, RunID: "r1", QuestionID: "q1", By: "user-2", Via: "user-2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			answerer := &fakeAnswerer{}
			h := newHarness(t, func(c *Config) { c.Answerer = answerer })
			res, err := h.invokeClient(tc.token).Answer(context.Background(), answerRequest("r1", "q1", &mandatumv1.Answer{
				Decision: payload.DecisionApprove, ArgumentsJson: `{"env":"staging"}`, Note: "ok",
				By: tc.by, QuestionId: "forged", Expired: true,
			}))
			if err != nil {
				t.Fatalf("Answer: %v", err)
			}
			got := answerer.requests()
			if len(got) != 1 {
				t.Fatalf("the runtime was asked %d times", len(got))
			}
			want := tc.want
			want.Answer = payload.Answer{Decision: payload.DecisionApprove, Arguments: []byte(`{"env":"staging"}`), Note: "ok"}
			if fmt.Sprintf("%+v", got[0]) != fmt.Sprintf("%+v", want) {
				t.Fatalf("the runtime was asked %+v, want %+v", got[0], want)
			}
			if !res.Msg.GetFirst() || res.Msg.GetAnswer().GetQuestionId() != "q1" || res.Msg.GetAnswer().GetBy() != want.By {
				t.Errorf("response = %v", res.Msg)
			}
		})
	}
}

// The runtime's refusals reach the caller as codes that say what to do: a
// question that is not there (or is another tenant's) is not found, an answer
// that does not fit is invalid, a closed question is a failed precondition,
// and a deployment without a runtime does not implement the method.
func TestAnAnswerTheRuntimeRefusesIsClassified(t *testing.T) {
	answerer := &fakeAnswerer{}
	h := newHarness(t, func(c *Config) { c.Answerer = answerer })
	client := h.invokeClient(tokenAcme)
	for _, tc := range []struct {
		fail error
		want connect.Code
	}{
		{fmt.Errorf("%w: question", ErrNotFound), connect.CodeNotFound},
		{fmt.Errorf("%w: not an option", ErrInvalid), connect.CodeInvalidArgument},
		{fmt.Errorf("%w: stopped", ErrClosed), connect.CodeFailedPrecondition},
		{fmt.Errorf("%w: self-approval", ErrForbidden), connect.CodePermissionDenied},
	} {
		answerer.fail = tc.fail
		_, err := client.Answer(context.Background(), answerRequest("r1", "q1", &mandatumv1.Answer{Choice: "a"}))
		if connect.CodeOf(err) != tc.want {
			t.Errorf("%v: code %v, want %v", tc.fail, connect.CodeOf(err), tc.want)
		}
	}
	answerer.fail = nil
	if _, err := client.Answer(context.Background(), answerRequest("", "q1", nil)); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("no run id: %v", err)
	}
	if _, err := newHarness(t, nil).invokeClient(tokenAcme).Answer(context.Background(), answerRequest("r1", "q1", nil)); connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Errorf("no answerer: %v", err)
	}
	if _, err := h.invokeClient("").Answer(context.Background(), answerRequest("r1", "q1", nil)); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("no credential: %v", err)
	}
}

// A run waiting on a person reads as parked, with the question it asked.
func TestGetRunReportsAParkedRunAndItsQuestion(t *testing.T) {
	h := newHarness(t, nil)
	h.runs.put(Run{ID: "run_parked", Tenant: tenantAcme, Agent: "ops", Status: StatusParked, Question: &payload.Question{
		ID: "q1", Kind: payload.QuestionApproval, Prompt: "Deploy to prod?", AskedOf: "oncall",
		Tool:      &payload.QuestionTool{Name: "deploy", CallID: "c1", Arguments: []byte(`{"env":"prod"}`)},
		ExpiresAt: fixedTime.Add(time.Hour),
		Default:   &payload.Answer{QuestionID: "q1", Decision: payload.DecisionDeny, Expired: true},
	}})
	got, err := h.invokeClient(tokenAcme).GetRun(context.Background(), connect.NewRequest(&mandatumv1.GetRunRequest{RunId: "run_parked"}))
	if err != nil {
		t.Fatal(err)
	}
	q := got.Msg.GetPendingQuestion()
	if got.Msg.GetStatus() != mandatumv1.RunStatus_RUN_STATUS_PARKED || q.GetId() != "q1" || q.GetTool().GetArgumentsJson() != `{"env":"prod"}` ||
		q.GetDefaultAnswer().GetDecision() != payload.DecisionDeny || q.GetExpiresAt() == nil || q.GetAskedOf() != "oncall" {
		t.Fatalf("run = %v", got.Msg)
	}
}

// A run's lifecycle is the run's to report: a client cannot publish a
// lifecycle event (a forged question a host might answer, a fake ending) or a
// control event on a run's topic.
func TestPublishRefusesLifecycleAndControlEvents(t *testing.T) {
	h := newHarness(t, nil)
	for _, kind := range []envelope.Kind{envelope.KindParked, envelope.KindResumed, envelope.KindCompleted, envelope.KindChildParked, envelope.KindAgentCreated} {
		_, err := h.busClient(tokenAcme).Publish(context.Background(), connect.NewRequest(&mandatumv1.PublishRequest{
			Event: &mandatumv1.Event{Topic: "agent:docs-bot:run_1", Kind: string(kind),
				Payload: []byte(`{"question":{"id":"q1","kind":"approval","prompt":"Approve?"}}`)},
		}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("publishing %s: %v, want invalid argument", kind, err)
		}
	}
}
