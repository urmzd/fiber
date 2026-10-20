package agentrt

import (
	"fmt"
	"os"

	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/saige/agent/agenttest"
	"github.com/urmzd/saige/agent/provider/anthropic"
	"github.com/urmzd/saige/agent/provider/ollama"
	"github.com/urmzd/saige/agent/provider/openai"
	"github.com/urmzd/saige/agent/provider/retry"
)

// Provider names a spec may ask for. The set is open: a deployment adds its
// own with WithProvider, and a spec naming a provider the process has not
// registered fails at build time with a message that lists what is available,
// rather than silently falling back to a model nobody asked for.
const (
	// ProviderAnthropic reads ANTHROPIC_API_KEY (see EnvAnthropicKey).
	ProviderAnthropic = "anthropic"
	// ProviderOllama talks to a local daemon (see EnvOllamaHost).
	ProviderOllama = "ollama"
	// ProviderOpenAI speaks the OpenAI Chat Completions API: OpenAI itself,
	// or any server that implements the same API (vLLM, LM Studio, llama.cpp,
	// OpenRouter, Groq, Together, a gateway) when EnvOpenAIBaseURL points at
	// it. See OpenAICompatible.
	ProviderOpenAI = "openai"
	// ProviderOffline replays a canned script instead of calling a model.
	// It is what makes an fiber deployment demonstrable with no API key,
	// no daemon, and no network: the whole path from tag to delivered event
	// runs, and only the token source is fake. Tests use it with WithScript.
	ProviderOffline = "offline"
)

// Environment variables the built-in providers read. They are read at build
// time, not at init, so a process can set them after start (a config loader, a
// secret fetch) and so a test can set one per case.
const (
	EnvAnthropicKey = "ANTHROPIC_API_KEY"
	EnvOllamaHost   = "OLLAMA_HOST"
	// EnvOpenAIKey authenticates the openai provider. It is required for
	// the OpenAI API and optional for a compatible server, many of which
	// run locally with no key.
	EnvOpenAIKey = "OPENAI_API_KEY"
	// EnvOpenAIBaseURL points the openai provider at an OpenAI-compatible
	// server, for example http://localhost:8000/v1. Unset is OpenAI.
	EnvOpenAIBaseURL = "OPENAI_BASE_URL"
)

// DefaultOllamaHost is used when EnvOllamaHost is unset.
const DefaultOllamaHost = "http://localhost:11434"

// DefaultOpenAIBaseURL is the OpenAI API, used when no base URL is given.
const DefaultOpenAIBaseURL = "https://api.openai.com/v1"

// ProviderFunc builds the LLM provider a spec asks for.
//
// It takes the whole Spec rather than just the model name because a provider
// may need more of the definition than the model: a thinking budget derived
// from the agent's purpose, a per-agent endpoint, a fake keyed to the agent's
// name. It returns an error rather than panicking or falling back, since a run
// against the wrong model is worse than a run that does not start.
type ProviderFunc func(spec Spec) (saigetypes.Provider, error)

// defaultProviders returns the built-in provider table. It is a function, not a
// package-level map, so no two Runners can mutate each other's table and so the
// zero configuration is not process-global state.
func defaultProviders() map[string]ProviderFunc {
	return map[string]ProviderFunc{
		ProviderAnthropic: nil, // resolved per Runner: it needs the cache lifetime
		ProviderOllama:    ollamaProvider,
		ProviderOpenAI:    openaiFromEnv,
		ProviderOffline:   nil, // resolved per Runner: it needs the script
	}
}

// anthropicProvider builds the Anthropic provider with prompt caching at the
// given lifetime (see WithPromptCache).
func anthropicProvider(cacheTTL string) ProviderFunc {
	return func(spec Spec) (saigetypes.Provider, error) {
		key := os.Getenv(EnvAnthropicKey)
		if key == "" {
			return nil, fmt.Errorf("%w: agent %q asks for the %s provider but %s is unset", ErrInvalid, spec.Name, ProviderAnthropic, EnvAnthropicKey)
		}
		if spec.Model == "" {
			return nil, fmt.Errorf("%w: agent %q asks for the %s provider with no model", ErrInvalid, spec.Name, ProviderAnthropic)
		}
		a, err := newAnthropic(key, spec.Model, cacheTTL)
		if err != nil {
			return nil, fmt.Errorf("%w: agent %q: %v", ErrInvalid, spec.Name, err)
		}
		return withRetry(a)
	}
}

// newAnthropic builds the adapter, with cache breakpoints when the model
// supports them.
//
// The capability is checked here rather than left to the adapter because the
// adapter checks it per request and fails the request: an agent pinned to a
// model without cache breakpoints would build cleanly and then fail every
// call. Caching is an optimization, so a model that cannot do it runs
// uncached instead.
//
// The policy is saige's default: a breakpoint after the tools, after the
// system prompt, and on the trailing message. Anthropic orders a request
// tools, system, messages, which is the stable-to-volatile order this package
// lays a turn out in, so each breakpoint ends a longer stable prefix than the
// one before it.
func newAnthropic(key, model, cacheTTL string, opts ...anthropic.Option) (*anthropic.Adapter, error) {
	cfg := anthropic.Config{APIKey: key, Model: saigetypes.ModelID(model)}
	a, err := anthropic.New(cfg, opts...)
	if err != nil {
		return nil, err
	}
	if cacheTTL == PromptCacheOff || !a.Capabilities().Supports(saigetypes.CapPromptCacheMarkers) {
		return a, nil
	}
	return anthropic.New(cfg, append(opts[:len(opts):len(opts)],
		anthropic.WithPromptCachePolicy(anthropic.DefaultPromptCachePolicy(cacheTTL)))...)
}

func ollamaProvider(spec Spec) (saigetypes.Provider, error) {
	if spec.Model == "" {
		return nil, fmt.Errorf("%w: agent %q asks for the %s provider with no model", ErrInvalid, spec.Name, ProviderOllama)
	}
	host := os.Getenv(EnvOllamaHost)
	if host == "" {
		host = DefaultOllamaHost
	}
	// The embedding model is empty: an agent turn generates, it does not
	// embed. Retrieval brings its own embedder (see Retriever).
	a, err := ollama.New(ollama.Config{Host: host, Model: saigetypes.ModelID(spec.Model)})
	if err != nil {
		return nil, fmt.Errorf("%w: agent %q: %v", ErrInvalid, spec.Name, err)
	}
	return withRetry(a)
}

// openaiFromEnv builds the openai provider from EnvOpenAIBaseURL and
// EnvOpenAIKey, read at build time like every built-in provider.
func openaiFromEnv(spec Spec) (saigetypes.Provider, error) {
	base := os.Getenv(EnvOpenAIBaseURL)
	key := os.Getenv(EnvOpenAIKey)
	if base == "" && key == "" {
		return nil, fmt.Errorf("%w: agent %q asks for the %s provider but %s is unset (set %s to use a compatible server instead)",
			ErrInvalid, spec.Name, ProviderOpenAI, EnvOpenAIKey, EnvOpenAIBaseURL)
	}
	return OpenAICompatible(base, key)(spec)
}

// OpenAICompatible returns a ProviderFunc for a server that speaks the OpenAI
// Chat Completions API at baseURL, authenticated with key. An empty baseURL is
// DefaultOpenAIBaseURL; an empty key sends none, which is what a local server
// expects.
//
// The arguments are the whole configuration. OpenAI's SDK also reads
// OPENAI_BASE_URL and OPENAI_API_KEY from the environment, and both are
// overridden here, so a provider registered for one server never sends the
// OpenAI key to it or follows the environment somewhere else. The SDK's other
// environment settings (OPENAI_ORG_ID, OPENAI_PROJECT_ID,
// OPENAI_CUSTOM_HEADERS) still apply to every client in the process; leave
// them unset in a deployment that also talks to third-party servers.
//
// The built-in "openai" provider is this function configured from the
// environment. A deployment that talks to more than one such server registers
// each under its own name:
//
//	agentrt.WithProvider("vllm", agentrt.OpenAICompatible("http://gpu-box:8000/v1", ""))
//
// Two things differ from the Anthropic provider and are worth knowing. There
// are no cache breakpoints to place: OpenAI caches a request's prefix on its
// own, which the stable-to-volatile layout already serves, and a compatible
// server caches or not as it chooses. And a model saige's catalog does not
// know (most models a compatible server hosts) resolves to conservative
// capabilities and no price, so its usage reports tokens and no cost.
func OpenAICompatible(baseURL, key string) ProviderFunc {
	return func(spec Spec) (saigetypes.Provider, error) {
		if spec.Model == "" {
			return nil, fmt.Errorf("%w: agent %q asks for an OpenAI-compatible provider with no model", ErrInvalid, spec.Name)
		}
		if baseURL == "" {
			baseURL = DefaultOpenAIBaseURL
		}
		a, err := openai.New(openai.Config{APIKey: key, Model: saigetypes.ModelID(spec.Model)}, openai.WithBaseURL(baseURL))
		if err != nil {
			return nil, fmt.Errorf("%w: agent %q: %v", ErrInvalid, spec.Name, err)
		}
		return withRetry(a)
	}
}

// withRetry wraps a model adapter in saige's retry decorator. saige's adapters
// make one attempt per call (the Anthropic SDK's own retries are off), so this
// is the only retry layer: transient failures such as rate limits and
// overloads are retried with jittered backoff that honors Retry-After, and
// everything else surfaces at once.
func withRetry(p saigetypes.Provider) (saigetypes.Provider, error) {
	r, err := retry.New(p, retry.DefaultConfig())
	if err != nil {
		return nil, fmt.Errorf("agentrt: retry: %w", err)
	}
	return r, nil
}

// offlineProvider replays script, one entry per model call.
//
// It is saige's own agenttest.ScriptedProvider rather than something written
// here, so the offline path exercises the same provider seam the real adapters
// implement and a test cannot accidentally be given a more forgiving fake than
// production uses.
func offlineProvider(script [][]saigetypes.Delta) ProviderFunc {
	return func(spec Spec) (saigetypes.Provider, error) {
		responses := script
		if len(responses) == 0 {
			responses = [][]saigetypes.Delta{agenttest.TextResponse(defaultOfflineAnswer(spec))}
		}
		return &agenttest.ScriptedProvider{Responses: responses}, nil
	}
}

// defaultOfflineAnswer is what the offline provider says when no script was
// given. It names itself: an operator reading a Slack thread must never mistake
// it for a model's answer.
func defaultOfflineAnswer(spec Spec) string {
	return fmt.Sprintf("%s is running with the offline provider, so no model was called. "+
		"Set the agent's provider to %q or %q for real inference.",
		spec.Name, ProviderAnthropic, ProviderOllama)
}
