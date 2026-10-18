package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// CompactRequest asks one run to compact its history, as the edge hands it to
// the runtime. It mirrors runtime.CompactRequest for the reason StopRequest
// mirrors runtime.StopRequest.
type CompactRequest struct {
	// Tenant is the caller's, from its credential.
	Tenant string
	// RunID is the run, as InvokeResponse.run_id reported it.
	RunID string
	// By is who asked: the credential's subject, unless the credential may
	// act for others and named someone.
	By string
}

// Compactor asks running turns to compact their history. Like Stopper it
// publishes a request and does not report what the run did with it, and it
// must behave identically for a run that is unknown, idle, finished or
// another tenant's.
type Compactor interface {
	Compact(ctx context.Context, req CompactRequest) error
}

// compactBody is what POST /v1/runs/{id}/compact accepts. The body is
// optional.
type compactBody struct {
	By string `json:"by"`
}

// handleCompact asks a run's running turn to compact its history before its
// next model call.
//
//	POST /v1/runs/{id}/compact
//
// It answers 202 with the same body for every run id, as POST
// /v1/runs/{id}/stop does and for the same reasons. Whether the turn
// compacted is on the run's stream, as context.compacted. It is plain HTTP
// beside the stop route, which is where a surface with a stop button already
// posts.
func (s *Server) handleCompact(compactor Compactor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if compactor == nil {
			s.httpFail(w, "compact", fmt.Errorf("%w: no runtime is configured", ErrUnimplemented))
			return
		}
		var body compactBody
		id, runID, ok := s.runRequest(w, r, "compact", &body)
		if !ok {
			return
		}
		if err := compactor.Compact(r.Context(), CompactRequest{Tenant: id.Tenant, RunID: runID, By: actorOf(id, body.By)}); err != nil {
			s.httpFail(w, "compact", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(stopReply{RunID: runID, Status: "requested"})
	}
}
