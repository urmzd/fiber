package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// StopRequest asks one run to stop, as the edge hands it to the runtime.
//
// It mirrors runtime.StopRequest rather than importing it for the reason
// Invoker is declared here: the edge is built and tested against its own
// types, and the composition root converts. The other stop types are
// different things: payload.StopRequest is the body on the bus and in the
// ledger, and agentrt.Stop is what a single turn is told.
type StopRequest struct {
	// Tenant is the caller's, from its credential. A run of another tenant
	// never hears the request.
	Tenant string
	// RunID is the run, as InvokeResponse.run_id reported it.
	RunID string
	// Reason is why, for people. Optional.
	Reason string
	// By is who asked: the credential's subject, unless the credential may
	// act for others and named someone.
	By string
}

// Stopper asks runs to stop. It is separate from Invoker for the reason
// RunReader is: starting a run and ending one are different privileges, and a
// deployment may wire only one of them.
//
// Stop publishes a request and does not report what the run did with it. It
// must behave identically for a run that is unknown, finished, already asked,
// or another tenant's, so that the answer does not say whether a
// run exists.
type Stopper interface {
	Stop(ctx context.Context, req StopRequest) error
}

// stopBody is what POST /v1/runs/{id}/stop accepts. Both fields are optional,
// and so is the body.
type stopBody struct {
	Reason string `json:"reason"`
	By     string `json:"by"`
}

// stopReply is what it answers.
type stopReply struct {
	RunID string `json:"run_id"`
	// Status is always "requested". The run's own stream says what happened:
	// lifecycle.stopped if it was stopped, and whatever it would have ended
	// with if it finished first.
	Status string `json:"status"`
}

// handleStop asks a run to stop.
//
//	POST /v1/runs/{id}/stop
//	{"reason": "Stopped by a user"}
//
// It answers 202 with the same body for every run id, whether or not such a run
// exists, is still working, or belongs to the caller. A stop is a request that
// the run's executing attempt obeys if it is there to hear it, so there is
// nothing truer to say synchronously, and saying anything else would confirm a
// run's existence across the tenant boundary.
//
// It is plain HTTP rather than an RPC for a practical reason and an honest one:
// every surface with a stop button can POST, and the protobuf contract is
// regenerated with a tool this change did not have. The route is the contract.
//
// The tenant is the credential's, as everywhere else, and so is who asked: a
// body may name someone only when the credential is trusted to act for others,
// which is what a surface that authenticated the person itself holds.
func (s *Server) handleStop(stopper Stopper) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if stopper == nil {
			s.httpFail(w, "stop", fmt.Errorf("%w: no runtime is configured", ErrUnimplemented))
			return
		}
		id, err := s.auth.Authenticate(r.Context(), r.Header)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="fiber"`)
			s.httpFail(w, "stop", err)
			return
		}
		runID := strings.TrimSpace(r.PathValue("id"))
		if runID == "" {
			s.httpFail(w, "stop", fmt.Errorf("%w: no run id", ErrInvalid))
			return
		}
		var body stopBody
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBody))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				http.Error(w, "stop: body exceeds the configured limit", http.StatusRequestEntityTooLarge)
				return
			}
			s.httpFail(w, "stop", fmt.Errorf("%w: read body: %w", ErrInvalid, err))
			return
		}
		if len(strings.TrimSpace(string(raw))) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				s.httpFail(w, "stop", fmt.Errorf("%w: body is not JSON: %w", ErrInvalid, err))
				return
			}
		}
		by := id.Subject
		if id.ActAs && strings.TrimSpace(body.By) != "" {
			by = body.By
		}
		if err := stopper.Stop(r.Context(), StopRequest{Tenant: id.Tenant, RunID: runID, Reason: body.Reason, By: by}); err != nil {
			s.httpFail(w, "stop", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(stopReply{RunID: runID, Status: "requested"})
	}
}
