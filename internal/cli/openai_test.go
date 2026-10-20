package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/envelope"
)

// Moving to another OpenAI-compatible host is configuration: the base URL and
// the name of the key's variable in the environment, and "openai" plus a model
// in the spec. This drives that through the composition root, from a stored
// agent to a delivered answer, against a local endpoint.
func TestAnOpenAICompatibleEndpointIsSelectedByConfigurationAlone(t *testing.T) {
	var mu sync.Mutex
	var paths, auths []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		paths = append(paths, r.URL.Path)
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		if !strings.Contains(string(body), `"model":"open-model"`) {
			http.Error(w, "unexpected model: "+string(body), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"open-model","choices":[{"index":0,"delta":{"role":"assistant","content":"from the endpoint"},"finish_reason":"stop"}]}`+"\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(ts.Close)

	t.Setenv(EnvOpenAIBaseURL, ts.URL+"/v1")
	t.Setenv(EnvOpenAIKeyEnv, "TOGETHER_API_KEY")
	t.Setenv("TOGETHER_API_KEY", "tg-key")
	ep, err := openAIFromEnv()
	if err != nil {
		t.Fatalf("openAIFromEnv: %v", err)
	}
	c, err := newCore(coreConfig{Tenant: "acme", Workspace: t.TempDir(), OpenAI: ep})
	if err != nil {
		t.Fatalf("newCore: %v", err)
	}
	t.Cleanup(func() { _ = c.close() })
	box := &inbox{}
	if err := c.sinks.Register(box); err != nil {
		t.Fatalf("register sink: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var g group
	c.startDelivery(ctx, &g)
	c.startExecution(ctx, &g)
	t.Cleanup(func() {
		cancel()
		_ = g.wait(10 * time.Second)
	})

	spec := agentspec.AgentSpec{Name: "docs-bot", Model: "open-model", Provider: agentspec.ProviderOpenAI, SystemPrompt: "Answer."}
	if _, err := c.store.Create(ctx, "acme", spec, "test"); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	target, _ := address.Parse("inbox://acme/out")
	if _, err := c.Invoke(ctx, envelope.Tag{ID: "Ev-1", Tenant: "acme", Agent: "docs-bot", Origin: "test",
		Text: "hi", Deliver: []envelope.Route{{Target: target}}, At: time.Now()}); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for !box.completed(target.String()) {
		if time.Now().After(deadline) {
			t.Fatalf("the run did not complete; delivered %d events", len(box.at(target.String())))
		}
		time.Sleep(20 * time.Millisecond)
	}
	var text string
	for _, e := range box.at(target.String()) {
		if e.Kind == envelope.KindText {
			text += string(e.Payload)
		}
	}
	if !strings.Contains(text, "from the endpoint") {
		t.Errorf("delivered text %q, want the endpoint's answer", text)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) == 0 || paths[0] != "/v1/chat/completions" || auths[0] != "Bearer tg-key" {
		t.Errorf("requests %v authorized %v, want /v1/chat/completions with the key from TOGETHER_API_KEY", paths, auths)
	}
}

// A malformed endpoint stops the server at startup, as a usage error.
func TestAMalformedOpenAIEndpointIsAUsageError(t *testing.T) {
	t.Setenv(EnvOpenAIBaseURL, "not a url")
	t.Setenv(EnvOpenAIKeyEnv, "")
	if _, err := openAIFromEnv(); exitCodeFor(err) != ExitUsage {
		t.Fatalf("openAIFromEnv = %v, want a usage error", err)
	}
	t.Setenv(EnvOpenAIBaseURL, "")
	t.Setenv(EnvOpenAIKeyEnv, "sk-pasted key")
	if _, err := openAIFromEnv(); exitCodeFor(err) != ExitUsage {
		t.Fatalf("openAIFromEnv = %v, want a usage error for a key pasted as its variable's name", err)
	}
}
