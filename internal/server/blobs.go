package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/urmzd/fiber/pkg/attachment"
	"github.com/urmzd/fiber/pkg/attachment/intake"
	"github.com/urmzd/fiber/pkg/blob"
)

// handleBlobPut stores an uploaded file and answers with the reference a tag
// carries.
//
// Uploading is separate from invoking because a tag is a run's durable input
// and is replayed on every attempt: it can hold a reference, not megabytes. It
// is plain HTTP rather than an RPC because the body is the file, and every
// client that can raise a tag can also PUT bytes.
//
// The tenant is the credential's, as everywhere else. The media type is
// detected from the content; whatever Content-Type the client sent is ignored,
// since the type decides how a model is shown the file and the uploader does
// not get to pick that.
func (s *Server) handleBlobPut(w http.ResponseWriter, r *http.Request) {
	if s.blobs == nil {
		s.httpFail(w, "blobs", fmt.Errorf("%w: no blob store is configured", ErrUnimplemented))
		return
	}
	id, err := s.auth.Authenticate(r.Context(), r.Header)
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Bearer realm="fiber"`)
		s.httpFail(w, "blobs", err)
		return
	}
	att, err := intake.Store(r.Context(), s.blobs, id.Tenant, r.Body, intake.Meta{
		Name: r.URL.Query().Get("name"),
		Alt:  r.URL.Query().Get("alt"),
	})
	switch {
	case errors.Is(err, blob.ErrTooLarge):
		http.Error(w, "blobs: the file exceeds the configured limit", http.StatusRequestEntityTooLarge)
		return
	case errors.Is(err, blob.ErrInvalid):
		s.httpFail(w, "blobs", fmt.Errorf("%w: %w", ErrInvalid, err))
		return
	case err != nil:
		s.httpFail(w, "blobs", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(att)
}

// checkAttachments confirms that every file a tag names is one this tenant
// holds.
//
// A digest is not a capability. Without this check a caller who learned the
// hash of another tenant's file could attach it to its own tag, and although
// the blob store would refuse the read later, the refusal would arrive as a
// failed run rather than as a rejected request. Checking at the door also keeps
// a tag that can never be executed out of the ledger.
func (s *Server) checkAttachments(ctx context.Context, tenant string, atts []attachment.Attachment) error {
	if len(atts) == 0 {
		return nil
	}
	if s.blobs == nil {
		return fmt.Errorf("%w: the tag carries attachments and no blob store is configured", ErrInvalid)
	}
	for i, a := range atts {
		d, err := blob.ParseDigest(a.Digest)
		if err != nil {
			return fmt.Errorf("%w: attachment %d: %w", ErrInvalid, i, err)
		}
		if _, err := s.blobs.Stat(ctx, tenant, d); err != nil {
			if errors.Is(err, blob.ErrNotFound) {
				return fmt.Errorf("%w: attachment %d names a file that has not been uploaded", ErrInvalid, i)
			}
			return fmt.Errorf("server: attachment %d: %w", i, err)
		}
	}
	return nil
}
