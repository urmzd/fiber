package agentrt_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/envelope"
)

// chatServer is an OpenAI-compatible endpoint that answers the first request
// with a streamed tool call and every later one with streamed text. It keeps
// what reached it so a test can assert on the wire.
type chatServer struct {
	// textOnly answers every request with text, for a turn with no tools.
	textOnly bool

	mu     sync.Mutex
	paths  []string
	auths  []string
	bodies []map[string]any
}

func (s *chatServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	s.mu.Lock()
	s.paths = append(s.paths, r.URL.Path)
	s.auths = append(s.auths, r.Header.Get("Authorization"))
	s.bodies = append(s.bodies, body)
	n := len(s.bodies)
	s.mu.Unlock()

	w.Header().Set("Content-Type", "text/event-stream")
	chunk := func(delta, finish string) {
		fin := "null"
		if finish != "" {
			fin = fmt.Sprintf("%q", finish)
		}
		fmt.Fprintf(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":%s,"finish_reason":%s}]}`+"\n\n", delta, fin)
	}
	if n == 1 && !s.textOnly {
		chunk(`{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":""}}]}`, "")
		chunk(`{"tool_calls":[{"index":0,"function":{"arguments":"{\"q\":\"deploys\"}"}}]}`, "")
		chunk(`{}`, "tool_calls")
	} else {
		chunk(`{"role":"assistant","content":"deploys "}`, "")
		chunk(`{"content":"happen on merge"}`, "")
		chunk(`{}`, "stop")
	}
	fmt.Fprint(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[],"usage":{"prompt_tokens":11,"completion_tokens":5,"total_tokens":16}}`+"\n\n")
	fmt.Fprint(w, "data: [DONE]\n\n")
}

func openAIRevision(provider string) agentrt.Revision {
	rev := offlineRevision(1, "You look things up.")
	rev.Spec.Provider = provider
	rev.Spec.Model = "accounts/fireworks/models/some-open-model"
	rev.Spec.Tools = []string{"lookup"}
	return rev
}

// The point of the provider is that a deployment moves to another
// OpenAI-compatible host by configuration alone: base URL, the variable the key
// is in, and the spec's model. A whole tool-calling turn has to work against
// such a host, with the key read from the named variable.
func TestOpenAICompatibleEndpointRunsAToolCallingTurn(t *testing.T) {
	srv := &chatServer{}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Setenv("FIREWORKS_API_KEY", "fw-secret")

	r, err := agentrt.New(openAIRevision("fireworks"),
		agentrt.WithOpenAICompatible(agentrt.OpenAICompatible{
			Name: "fireworks", BaseURL: ts.URL + "/inference/v1", APIKeyEnv: "FIREWORKS_API_KEY",
		}),
		agentrt.WithTools(agentrt.StaticTools(lookupTool("lookup"))))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got, res := collect(t, r, agentrt.Turn{Text: "how do deploys work?"})

	if res.Text != "deploys happen on merge" {
		t.Errorf("answer is %q, want the streamed text", res.Text)
	}
	if !hasKind(got, envelope.KindToolCall) || !hasKind(got, envelope.KindToolDone) {
		t.Errorf("chunks %v, want the tool call and its result", kinds(got))
	}
	if len(srv.bodies) != 2 {
		t.Fatalf("%d requests reached the endpoint, want 2 (the tool call, then the answer)", len(srv.bodies))
	}
	for i := range srv.bodies {
		if srv.paths[i] != "/inference/v1/chat/completions" {
			t.Errorf("request %d went to %q, want the base URL's /chat/completions", i, srv.paths[i])
		}
		if srv.auths[i] != "Bearer fw-secret" {
			t.Errorf("request %d authorized as %q, want the key from FIREWORKS_API_KEY", i, srv.auths[i])
		}
		if srv.bodies[i]["model"] != "accounts/fireworks/models/some-open-model" {
			t.Errorf("request %d asked for model %v, want the spec's", i, srv.bodies[i]["model"])
		}
		if srv.bodies[i]["stream"] != true {
			t.Errorf("request %d did not stream", i)
		}
	}
	if tools, _ := srv.bodies[0]["tools"].([]any); len(tools) != 1 {
		t.Errorf("the first request offered %d tools, want lookup", len(tools))
	}
	// The second request carries the tool's result back to the model.
	msgs, _ := srv.bodies[1]["messages"].([]any)
	var sawResult bool
	for _, m := range msgs {
		if mm, _ := m.(map[string]any); mm["role"] == "tool" && mm["tool_call_id"] == "call_1" {
			sawResult = true
		}
	}
	if !sawResult {
		t.Errorf("the follow-up request has no tool message for call_1: %v", msgs)
	}
	if res.Usage.ModelCalls != 2 || res.Usage.PromptTokens != 22 || res.Usage.CompletionTokens != 10 {
		t.Errorf("usage is %+v, want both calls' tokens", res.Usage)
	}
}

// The built-in "openai" provider is the same adapter with OpenAI's defaults,
// and a deployment repoints it rather than adding a name.
func TestBuiltInOpenAIProviderCanBeRepointed(t *testing.T) {
	srv := &chatServer{}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Setenv("VLLM_KEY", "local")

	r, err := agentrt.New(openAIRevision(agentrt.ProviderOpenAI),
		agentrt.WithOpenAICompatible(agentrt.OpenAICompatible{Name: agentrt.ProviderOpenAI, BaseURL: ts.URL + "/v1", APIKeyEnv: "VLLM_KEY"}),
		agentrt.WithTools(agentrt.StaticTools(lookupTool("lookup"))))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	collect(t, r, agentrt.Turn{Text: "hi"})
	if len(srv.paths) == 0 || srv.paths[0] != "/v1/chat/completions" || srv.auths[0] != "Bearer local" {
		t.Fatalf("requests went to %v as %v, want the repointed endpoint", srv.paths, srv.auths)
	}
}

// A misconfigured endpoint refuses to build, naming what is wrong, rather than
// sending a request that cannot succeed. Nothing it reports may contain the key.
func TestOpenAICompatibleEndpointRefusesToBuildWhenMisconfigured(t *testing.T) {
	t.Setenv("EMPTY_KEY", "")
	t.Setenv("SET_KEY", "sk-very-secret")
	cases := []struct {
		name string
		ep   agentrt.OpenAICompatible
		spec func(*agentrt.Revision)
		want string
	}{
		{"the key's variable is unset", agentrt.OpenAICompatible{Name: "x", APIKeyEnv: "EMPTY_KEY"}, nil, "EMPTY_KEY is unset"},
		{"the default variable is unset", agentrt.OpenAICompatible{Name: "x", APIKeyEnv: "FIBER_TEST_NEVER_SET"}, nil, "FIBER_TEST_NEVER_SET is unset"},
		{"the base URL is not http", agentrt.OpenAICompatible{Name: "x", BaseURL: "ftp://example.com", APIKeyEnv: "SET_KEY"}, nil, "not an http(s) URL"},
		{"a key was pasted where its name belongs", agentrt.OpenAICompatible{Name: "x", APIKeyEnv: "sk-abc def"}, nil, "must name an environment variable"},
		{"no model anywhere", agentrt.OpenAICompatible{Name: "x", APIKeyEnv: "SET_KEY"}, func(r *agentrt.Revision) { r.Spec.Model = "" }, "no model"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rev := openAIRevision("x")
			rev.Spec.Tools = nil
			if tc.spec != nil {
				tc.spec(&rev)
			}
			_, err := agentrt.New(rev, agentrt.WithOpenAICompatible(tc.ep))
			if !errors.Is(err, agentrt.ErrInvalid) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New = %v, want ErrInvalid mentioning %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "sk-very-secret") {
				t.Fatalf("the error leaks the key: %v", err)
			}
		})
	}
}

// The endpoint's model is a default, never an override: the spec is what a run
// pins, so the spec's model is what must be sent.
func TestOpenAICompatibleSpecModelWinsOverEndpointDefault(t *testing.T) {
	srv := &chatServer{textOnly: true}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Setenv("K", "k")
	ep := agentrt.OpenAICompatible{Name: "x", BaseURL: ts.URL, APIKeyEnv: "K", Model: "endpoint-default"}

	for _, tc := range []struct{ spec, want string }{{"pinned-model", "pinned-model"}, {"", "endpoint-default"}} {
		rev := openAIRevision("x")
		rev.Spec.Model = tc.spec
		rev.Spec.Tools = nil
		r, err := agentrt.New(rev, agentrt.WithOpenAICompatible(ep))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		before := len(srv.bodies)
		collect(t, r, agentrt.Turn{Text: "hi"})
		if got := srv.bodies[before]["model"]; got != tc.want {
			t.Errorf("spec model %q sent %v, want %q", tc.spec, got, tc.want)
		}
	}
}
