package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/event"
	turnrt "github.com/urmzd/fiber/pkg/turn"
)

// RuntimeExecutor runs each turn on rt. It is a temporary bridge: the
// request's text and meta become a message.posted.v1 trigger, whose
// rendering is the request text an Executor was always sent, and every body
// rt emits becomes a chunk.
//
// The stop and compact signals and the actor reach rt on ctx, where the
// runtime put them.
func RuntimeExecutor(rt turnrt.Runtime) Executor {
	return &runtimeExecutor{rt: rt}
}

type runtimeExecutor struct {
	rt turnrt.Runtime
}

func (x *runtimeExecutor) Execute(ctx context.Context, req Request, emit func(context.Context, agentrt.Chunk) error) (Outcome, error) {
	if x.rt == nil {
		return Outcome{}, fmt.Errorf("%w: no turn runtime", ErrInvalid)
	}
	def := turnrt.Pinned{
		Name:    req.Revision.Spec.Name,
		Version: strconv.Itoa(req.Revision.Rev),
		Digest:  req.Revision.Hash,
		Spec:    req.Revision.Spec,
	}
	in := turnrt.Input{
		Trigger:      triggerOf(req),
		History:      req.History,
		Instructions: req.Instructions,
		Tenant:       req.Tenant,
		Accounting:   req.Accounting,
		Park:         req.Park,
		QuestionTTL:  req.QuestionTTL,
	}
	if req.Resume != nil {
		p, err := parkedTo(*req.Resume)
		if err != nil {
			return Outcome{}, err
		}
		in.Resume = &p
	}
	chunks := 0
	res, err := x.rt.Turn(ctx, def, in, func(ctx context.Context, b event.Body) error {
		c, ok := agentrt.ChunkOf(b)
		if !ok {
			return nil
		}
		chunks++
		if emit == nil {
			return nil
		}
		return emit(ctx, c)
	})
	out := Outcome{Text: res.Text, Prompt: res.Prompt, Chunks: chunks, Citations: res.Citations, Stopped: res.Stopped}
	if err != nil {
		return out, err
	}
	if res.Parked != nil {
		p, err := parkedOf(*res.Parked)
		if err != nil {
			return out, err
		}
		out.Parked = &p
	}
	return out, nil
}

// triggerOf is the request as the event that triggered it.
func triggerOf(req Request) event.Envelope {
	return event.Envelope{
		ID:      req.RunID,
		Source:  firstNonEmpty(req.Source, "fiber:runtime"),
		Type:    event.TypeMessagePosted,
		Channel: firstNonEmpty(req.Source, "fiber:runtime"),
		Tenant:  req.Tenant,
		Run:     req.RunID,
		Agent:   req.Revision.Spec.Name,
		Version: strconv.Itoa(req.Revision.Rev),
		Digest:  req.Revision.Hash,
		Data: event.MessagePosted{Request: event.Request{
			Text:        req.Text,
			Attachments: req.Attachments,
			Context:     req.Meta,
		}},
	}
}

// parkedTo carries a parked turn the bridge recorded into the turn API, with
// the whole record as its state.
func parkedTo(p agentrt.Parked) (turnrt.Parked, error) {
	out := turnrt.Parked{Prompt: p.Prompt}
	state, err := json.Marshal(p)
	if err != nil {
		return out, fmt.Errorf("runtime: encode parked turn: %w", err)
	}
	out.State = state
	if err := recode(p.Question, &out.Question); err != nil {
		return out, err
	}
	if len(p.Answers) > 0 {
		if err := recode(p.Answers, &out.Answers); err != nil {
			return out, err
		}
	}
	return out, nil
}

// parkedOf reads a parked turn back from its state, with the question and
// prompt the turn API reports.
func parkedOf(p turnrt.Parked) (agentrt.Parked, error) {
	var out agentrt.Parked
	if len(p.State) > 0 {
		if err := json.Unmarshal(p.State, &out); err != nil {
			return out, fmt.Errorf("runtime: decode parked turn: %w", err)
		}
	}
	out.Prompt = p.Prompt
	if err := recode(p.Question, &out.Question); err != nil {
		return out, err
	}
	return out, nil
}

func recode(v, out any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("runtime: recode %T: %w", v, err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("runtime: recode %T to %T: %w", v, out, err)
	}
	return nil
}
