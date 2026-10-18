package server

import (
	"context"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	mandatumv1 "github.com/urmzd/fiber/gen/mandatum/v1"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
)

// AnswerRequest answers one question a run is parked on, as the edge hands it
// to the runtime. It mirrors runtime.AnswerRequest for the reason StopRequest
// mirrors runtime.StopRequest.
type AnswerRequest struct {
	// Tenant is the caller's, from its credential. A question of another
	// tenant's run is ErrNotFound.
	Tenant string
	// RunID and QuestionID name the question.
	RunID      string
	QuestionID string
	// Answer is what the caller said: a decision and edited arguments, a
	// choice, or a reply, and a note.
	Answer payload.Answer
	// By is who answered: the credential's subject, unless the credential
	// may act for others and named someone.
	By string
	// Via is the credential's own subject, whoever it named.
	Via string
}

// Answerer answers the questions runs are parked on. It is separate from
// Stopper and Invoker because answering for an agent is a privilege of its
// own, and a deployment may wire it alone.
//
// Answer returns the answer the question has, which is this one only when
// first is true. It reports a question that does not exist, or belongs to
// another tenant's run, as ErrNotFound; an answer that does not fit the
// question as ErrInvalid; a caller who may not answer it (the requester
// approving their own call, someone outside the spec's approvers) as
// ErrForbidden; and a question whose run was stopped or ended as ErrClosed.
type Answerer interface {
	Answer(ctx context.Context, req AnswerRequest) (payload.Answer, bool, error)
}

// Answer answers a question a run is parked on.
//
// The tenant and who answered come from the credential, never from the body:
// a body may name the person only when the credential is trusted to act for
// others, which is what a surface that authenticated the person itself holds.
func (s *invokeService) Answer(ctx context.Context, req *connect.Request[mandatumv1.AnswerRequest]) (*connect.Response[mandatumv1.AnswerResponse], error) {
	id, err := s.identity(ctx)
	if err != nil {
		return nil, err
	}
	if s.answerer == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			fmt.Errorf("answer: %w: no runtime is configured", ErrUnimplemented))
	}
	runID, qid := strings.TrimSpace(req.Msg.GetRunId()), strings.TrimSpace(req.Msg.GetQuestionId())
	if runID == "" || qid == "" {
		return nil, fail(s.log, "answer", fmt.Errorf("%w: an answer names a run and a question", ErrInvalid))
	}
	by := id.Subject
	if named := strings.TrimSpace(req.Msg.GetAnswer().GetBy()); id.ActAs && named != "" {
		by = named
	}
	got, first, err := s.answerer.Answer(ctx, AnswerRequest{
		Tenant: id.Tenant, RunID: runID, QuestionID: qid,
		Answer: answerFromProto(req.Msg.GetAnswer()), By: by, Via: id.Subject,
	})
	if err != nil {
		return nil, fail(s.log, "answer", err)
	}
	return connect.NewResponse(&mandatumv1.AnswerResponse{Answer: answerToProto(&got), First: first}), nil
}
