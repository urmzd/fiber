package mcptools_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/urmzd/fiber/pkg/mcptools"
)

// redirector answers every request with a 307 to target+path, so the client
// repeats the same method and body at the new location.
func redirector(t *testing.T, target string, tls bool) *httptest.Server {
	t.Helper()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target+r.URL.Path, http.StatusTemporaryRedirect)
	})
	var srv *httptest.Server
	if tls {
		srv = httptest.NewTLSServer(h)
	} else {
		srv = httptest.NewServer(h)
	}
	t.Cleanup(srv.Close)
	return srv
}

func sawBearer(seen *sync.Map) bool {
	found := false
	seen.Range(func(k, _ any) bool {
		if strings.Contains(k.(string), token) {
			found = true
		}
		return !found
	})
	return found
}

func TestTheCredentialIsNotSentAcrossARedirectToAnotherOrigin(t *testing.T) {
	// The fake MCP server is on another port, so it is another origin.
	elsewhere, seen := httpFake(t, newFake())
	front := redirector(t, elsewhere, false)

	s, err := mcptools.Connect(context.Background(), httpProvider(front.URL+"/mcp"), mcptools.WithResolver(resolver(t)))
	if err == nil {
		t.Cleanup(func() { _ = s.Close() })
	}
	if sawBearer(seen) {
		t.Fatal("the credential followed a redirect to another origin")
	}
}

func TestTheCredentialSurvivesARedirectWithinTheSameOrigin(t *testing.T) {
	f := newFake()
	h := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return f.srv }, nil)
	var redirected atomic.Bool
	var seen sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/old" {
			redirected.Store(true)
			http.Redirect(w, r, "/mcp", http.StatusTemporaryRedirect)
			return
		}
		seen.Store(r.Header.Get("Authorization"), true)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	s, err := mcptools.Connect(context.Background(), httpProvider(srv.URL+"/old"), mcptools.WithResolver(resolver(t)))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if !redirected.Load() {
		t.Fatal("the test server never redirected")
	}
	if !sawBearer(&seen) {
		t.Fatal("the credential was dropped on a same-origin redirect")
	}
}

func TestAnHTTPSProviderMayNotRedirectToPlainHTTP(t *testing.T) {
	var hits atomic.Int32
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if strings.Contains(r.Header.Get("Authorization"), token) {
			t.Error("the credential was sent over plain HTTP")
		}
		http.Error(w, "nope", http.StatusTeapot)
	}))
	t.Cleanup(plain.Close)
	secure := redirector(t, plain.URL, true)

	_, err := mcptools.Connect(context.Background(), httpProvider(secure.URL+"/mcp"),
		mcptools.WithResolver(resolver(t)), mcptools.WithHTTPClient(secure.Client()))
	if err == nil {
		t.Fatal("Connect succeeded through an https to http redirect")
	}
	if !errors.Is(err, mcptools.ErrRedirectDowngrade) && !strings.Contains(err.Error(), mcptools.ErrRedirectDowngrade.Error()) {
		t.Fatalf("err = %v, want ErrRedirectDowngrade", err)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("the plain HTTP server received %d request(s); the redirect should not be followed", n)
	}
}
