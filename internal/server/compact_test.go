package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// fakeCompactor records what the edge asked the runtime to compact.
type fakeCompactor struct {
	mu   sync.Mutex
	got  []CompactRequest
	fail error
}

func (f *fakeCompactor) Compact(_ context.Context, req CompactRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	f.got = append(f.got, req)
	return nil
}

func (f *fakeCompactor) requests() []CompactRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]CompactRequest(nil), f.got...)
}

func (h *harness) postCompact(t *testing.T, runID, token, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		h.http.URL+"/v1/runs/"+runID+"/compact", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set(HeaderAuthorization, "Bearer "+token)
	}
	res, err := h.http.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(raw)
}

// A request to compact carries the credential's tenant and actor, as a stop
// does, and is answered the same way for every run.
func TestACompactRequestCarriesTheCredentialAndSaysNothingAboutTheRun(t *testing.T) {
	compactor := &fakeCompactor{}
	h := newHarness(t, func(c *Config) { c.Compactor = compactor })

	answer := func(runID, token, body string) string {
		t.Helper()
		status, reply := h.postCompact(t, runID, token, body)
		if status != http.StatusAccepted {
			t.Fatalf("status = %d (%s), want 202", status, reply)
		}
		var got map[string]string
		if err := json.Unmarshal([]byte(reply), &got); err != nil || got["run_id"] != runID || got["status"] != "requested" {
			t.Fatalf("reply = %q", reply)
		}
		delete(got, "run_id")
		return fmt.Sprint(got)
	}
	mine := answer("r-mine", tokenAcme, `{"by":"someone-else","tenant":"other"}`)
	theirs := answer("r-mine", tokenOther, ``)
	surface := answer("r-nobody", tokenSurface, `{"by":"slack:U1"}`)
	if mine != theirs || mine != surface {
		t.Fatalf("answers differ: %q, %q, %q", mine, theirs, surface)
	}
	want := []CompactRequest{
		{Tenant: tenantAcme, RunID: "r-mine", By: subjectAcme},
		{Tenant: tenantOther, RunID: "r-mine", By: compactorSubject(t, h, tokenOther)},
		{Tenant: tenantAcme, RunID: "r-nobody", By: "slack:U1"},
	}
	got := compactor.requests()
	if len(got) != len(want) {
		t.Fatalf("the runtime was asked %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("request %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// compactorSubject is the subject a token authenticates as.
func compactorSubject(t *testing.T, h *harness, token string) string {
	t.Helper()
	hdr := http.Header{}
	hdr.Set(HeaderAuthorization, "Bearer "+token)
	id, err := h.server.auth.Authenticate(context.Background(), hdr)
	if err != nil {
		t.Fatal(err)
	}
	return id.Subject
}

// A request to compact is refused without a credential, with a body that is
// not JSON, and when the deployment has no runtime to carry it.
func TestACompactRequestThatCannotBeCarriedIsRefused(t *testing.T) {
	compactor := &fakeCompactor{}
	h := newHarness(t, func(c *Config) { c.Compactor = compactor })
	if status, _ := h.postCompact(t, "r1", "", `{}`); status != http.StatusUnauthorized {
		t.Errorf("no credential: status %d, want 401", status)
	}
	if status, _ := h.postCompact(t, "r1", tokenAcme, `not json`); status != http.StatusBadRequest {
		t.Errorf("malformed body: status %d, want 400", status)
	}
	if got := compactor.requests(); len(got) != 0 {
		t.Errorf("refused requests reached the runtime: %+v", got)
	}
	compactor.fail = fmt.Errorf("%w: run id", ErrInvalid)
	if status, _ := h.postCompact(t, "r1", tokenAcme, `{}`); status != http.StatusBadRequest {
		t.Errorf("runtime refusal: status %d, want 400", status)
	}
	if status, _ := newHarness(t, nil).postCompact(t, "r1", tokenAcme, `{}`); status != http.StatusNotImplemented {
		t.Errorf("no compactor: status %d, want 501", status)
	}
}
