package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// fakeStopper records what the edge asked the runtime to stop.
type fakeStopper struct {
	mu   sync.Mutex
	got  []StopRequest
	fail error
}

func (f *fakeStopper) Stop(_ context.Context, req StopRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	f.got = append(f.got, req)
	return nil
}

func (f *fakeStopper) requests() []StopRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]StopRequest(nil), f.got...)
}

func (h *harness) postStop(t *testing.T, runID, token, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		h.http.URL+"/v1/runs/"+runID+"/stop", strings.NewReader(body))
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
	var raw strings.Builder
	buf := make([]byte, 4096)
	for {
		n, rerr := res.Body.Read(buf)
		raw.Write(buf[:n])
		if rerr != nil {
			break
		}
	}
	return res.StatusCode, raw.String()
}

// The tenant of a stop is the credential's, whatever the body says, and so is
// who asked. Only a credential trusted to act for others may name someone
// else, which is what a surface that authenticated the person itself holds.
func TestAStopCarriesTheCredentialsTenantAndActor(t *testing.T) {
	tests := []struct {
		name, token, body string
		want              StopRequest
	}{
		{
			name: "an ordinary credential is itself", token: tokenAcme,
			body: `{"reason":"wrong repo","by":"someone-else","tenant":"other"}`,
			want: StopRequest{Tenant: tenantAcme, RunID: "r0123", Reason: "wrong repo", By: subjectAcme},
		},
		{
			name: "a surface names the person", token: tokenSurface,
			body: `{"reason":"Stopped by a user","by":"slack:U1"}`,
			want: StopRequest{Tenant: tenantAcme, RunID: "r0123", Reason: "Stopped by a user", By: "slack:U1"},
		},
		{
			name: "a surface that names nobody is itself", token: tokenSurface, body: ``,
			want: StopRequest{Tenant: tenantAcme, RunID: "r0123", By: "slack-surface"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stopper := &fakeStopper{}
			h := newHarness(t, func(c *Config) { c.Stopper = stopper })
			status, body := h.postStop(t, "r0123", tc.token, tc.body)
			if status != http.StatusAccepted {
				t.Fatalf("status = %d (%s), want 202", status, body)
			}
			got := stopper.requests()
			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("the runtime was asked %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The answer to a stop says nothing about the run: the same status and the same
// body for any run id and any tenant, so it cannot be used to learn whether a
// run exists.
func TestAStopIsAnsweredTheSameWayForEveryRun(t *testing.T) {
	stopper := &fakeStopper{}
	h := newHarness(t, func(c *Config) { c.Stopper = stopper })

	answer := func(runID, token string) string {
		status, body := h.postStop(t, runID, token, `{}`)
		var reply map[string]string
		if err := json.Unmarshal([]byte(body), &reply); err != nil {
			t.Fatalf("reply %q is not JSON: %v", body, err)
		}
		if reply["run_id"] != runID || reply["status"] != "requested" {
			t.Fatalf("reply = %v", reply)
		}
		delete(reply, "run_id")
		return fmt.Sprintf("%d %v", status, reply)
	}
	mine := answer("r-mine", tokenAcme)
	theirs := answer("r-mine", tokenOther)
	nobody := answer("r-nobody", tokenAcme)
	if mine != theirs || mine != nobody {
		t.Fatalf("answers differ: own run %q, another tenant %q, unknown run %q", mine, theirs, nobody)
	}
	if got := stopper.requests(); len(got) != 3 || got[1].Tenant != tenantOther {
		t.Fatalf("requests = %+v: the second must carry the other tenant, which is what keeps it from being honoured", got)
	}
}

// A stop is refused without a credential, with a body that is not JSON, and
// when the deployment has no runtime to carry it; a runtime's own refusal
// reaches the caller classified.
func TestAStopThatCannotBeCarriedIsRefused(t *testing.T) {
	stopper := &fakeStopper{}
	h := newHarness(t, func(c *Config) { c.Stopper = stopper })
	if status, _ := h.postStop(t, "r1", "", `{}`); status != http.StatusUnauthorized {
		t.Errorf("no credential: status %d, want 401", status)
	}
	if status, _ := h.postStop(t, "r1", tokenAcme, `not json`); status != http.StatusBadRequest {
		t.Errorf("malformed body: status %d, want 400", status)
	}
	if got := stopper.requests(); len(got) != 0 {
		t.Errorf("refused requests reached the runtime: %+v", got)
	}

	stopper.fail = fmt.Errorf("%w: run id", ErrInvalid)
	if status, _ := h.postStop(t, "r1", tokenAcme, `{}`); status != http.StatusBadRequest {
		t.Errorf("runtime refusal: status %d, want 400", status)
	}
	stopper.fail = errors.New("broker down")
	if status, body := h.postStop(t, "r1", tokenAcme, `{}`); status != http.StatusInternalServerError || strings.Contains(body, "broker down") {
		t.Errorf("runtime failure: status %d body %q, want an opaque 500", status, body)
	}

	bare := newHarness(t, nil)
	if status, _ := bare.postStop(t, "r1", tokenAcme, `{}`); status != http.StatusNotImplemented {
		t.Errorf("no stopper: status %d, want 501", status)
	}
}

// The wire enum has no member for a stopped run yet. It must still read as
// over, and as not failed, to a client polling GetRun.
func TestAStoppedRunReadsAsTerminalAndNotFailedOnTheWire(t *testing.T) {
	got := statusToProto(StatusStopped)
	if got != statusToProto(StatusCompleted) || got == statusToProto(StatusFailed) {
		t.Fatalf("StatusStopped maps to %v", got)
	}
}
