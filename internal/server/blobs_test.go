package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"connectrpc.com/connect"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/attachment"
	"github.com/urmzd/mandatum/pkg/blob"
)

func withBlobs(cfg *Config) { cfg.Blobs = blob.NewMemory() }

// upload stores body under the token's tenant and returns the reference.
func (h *harness) upload(token, name, body string) (attachment.Attachment, int) {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.http.URL+"/v1/blobs?name="+name, strings.NewReader(body))
	if err != nil {
		h.t.Fatalf("request: %v", err)
	}
	// A lie, on purpose: the server must type the file from its content.
	req.Header.Set("Content-Type", "image/png")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("upload: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	var att attachment.Attachment
	if res.StatusCode == http.StatusCreated {
		if err := json.NewDecoder(res.Body).Decode(&att); err != nil {
			h.t.Fatalf("decode reference: %v", err)
		}
	}
	return att, res.StatusCode
}

func protoAttachment(a attachment.Attachment) *mandatumv1.Attachment {
	return &mandatumv1.Attachment{Digest: a.Digest, MediaType: a.MediaType, Name: a.Name, Size: a.Size}
}

// An uploaded file comes back as a reference typed from its content, and a tag
// carrying that reference reaches the runtime with it intact.
func TestAnUploadedFileTravelsOnTheTagAsAReference(t *testing.T) {
	h := newHarness(t, withBlobs)
	att, status := h.upload(tokenAcme, "notes.txt", "the deploy failed at step three\n")
	if status != http.StatusCreated {
		t.Fatalf("upload status: got %d, want 201", status)
	}
	if att.Digest != string(blob.Sum([]byte("the deploy failed at step three\n"))) {
		t.Errorf("digest %q is not the SHA-256 of the content", att.Digest)
	}
	if !strings.HasPrefix(att.MediaType, "text/") {
		t.Errorf("media type: got %q; the claimed image/png must not be believed for text content", att.MediaType)
	}

	msg := tagMsg()
	msg.Attachments = []*mandatumv1.Attachment{protoAttachment(att)}
	if _, err := h.invokeClient(tokenAcme).Invoke(context.Background(), connect.NewRequest(msg)); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	accepted := h.invoker.accepted()
	if len(accepted) != 1 || len(accepted[0].Attachments) != 1 {
		t.Fatalf("want one accepted tag with one attachment, got %+v", accepted)
	}
	if got := accepted[0].Attachments[0]; got.Digest != att.Digest || got.Name != "notes.txt" {
		t.Errorf("attachment changed in transit: %+v", got)
	}
}

// A digest is not a capability: knowing the hash of another tenant's file does
// not let a caller attach it, and neither does inventing one.
func TestATagCannotAttachAFileItsTenantDoesNotHold(t *testing.T) {
	h := newHarness(t, withBlobs)
	theirs, status := h.upload(tokenOther, "secret.txt", "other tenant's notes\n")
	if status != http.StatusCreated {
		t.Fatalf("upload status: got %d, want 201", status)
	}
	never := attachment.Attachment{Digest: string(blob.Sum([]byte("never uploaded"))), MediaType: "text/plain", Size: 14}

	for name, att := range map[string]attachment.Attachment{"another tenant's file": theirs, "a file never uploaded": never} {
		t.Run(name, func(t *testing.T) {
			msg := tagMsg()
			msg.Attachments = []*mandatumv1.Attachment{protoAttachment(att)}
			_, err := h.invokeClient(tokenAcme).Invoke(context.Background(), connect.NewRequest(msg))
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("invoke: got %v, want invalid argument", err)
			}
		})
	}
	if n := len(h.invoker.accepted()); n != 0 {
		t.Errorf("%d tags reached the runtime; none should have", n)
	}
}

// Uploading is authenticated like everything else, and a server with no blob
// store refuses both the upload and a tag that names a file.
func TestUploadsNeedACredentialAndABlobStore(t *testing.T) {
	h := newHarness(t, withBlobs)
	if _, status := h.upload("", "a.txt", "x"); status != http.StatusUnauthorized {
		t.Errorf("no credential: got %d, want 401", status)
	}

	bare := newHarness(t, nil)
	if _, status := bare.upload(tokenAcme, "a.txt", "x"); status != http.StatusNotImplemented {
		t.Errorf("no blob store: got %d, want 501", status)
	}
	msg := tagMsg()
	msg.Attachments = []*mandatumv1.Attachment{{Digest: string(blob.Sum([]byte("x"))), MediaType: "text/plain", Size: 1}}
	if _, err := bare.invokeClient(tokenAcme).Invoke(context.Background(), connect.NewRequest(msg)); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("tag with attachments and no blob store: got %v, want invalid argument", err)
	}
}
