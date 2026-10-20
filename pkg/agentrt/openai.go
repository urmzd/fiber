package agentrt

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/urmzd/saige/agent/provider/openai"
	saigetypes "github.com/urmzd/saige/agent/types"
)

// ProviderOpenAI is the built-in OpenAI-compatible provider. Out of the box it
// talks to OpenAI with the key in EnvOpenAIKey; a deployment points it at any
// endpoint that speaks the Chat Completions API (Fireworks, vLLM, Together, a
// gateway) with WithOpenAICompatible, and registers more of them under their
// own names the same way.
const ProviderOpenAI = "openai"

// EnvOpenAIKey is where the built-in openai provider reads its key unless an
// endpoint names another variable.
const EnvOpenAIKey = "OPENAI_API_KEY"

// OpenAICompatible is one Chat Completions endpoint a spec can name as its
// provider.
//
// It holds the NAME of the environment variable the key is in, never the key:
// an endpoint is configuration that is logged, printed and committed to a
// file, and a key in any of those places is a leaked key. The variable is read
// when a runner is built, so a rotated key takes effect on the next run
// without a restart.
type OpenAICompatible struct {
	// Name is what a spec's provider field says to select this endpoint.
	Name string `json:"name" yaml:"name"`
	// BaseURL is the API root, the part before /chat/completions, for
	// example https://api.fireworks.ai/inference/v1. Empty is OpenAI's.
	BaseURL string `json:"base_url,omitempty" yaml:"base_url,omitempty"`
	// APIKeyEnv names the environment variable that holds the key. Empty
	// takes EnvOpenAIKey.
	APIKeyEnv string `json:"api_key_env,omitempty" yaml:"api_key_env,omitempty"`
	// Model is used when a spec names none. A spec's model always wins: the
	// spec is what a run pins and hashes, so it must say what ran.
	Model string `json:"model,omitempty" yaml:"model,omitempty"`
}

// Validate reports an endpoint that could never serve a request, so a typo
// fails at startup rather than on the first tag. It does not read the key:
// whether it is set is a property of the process at build time.
func (e OpenAICompatible) Validate() error {
	if strings.TrimSpace(e.Name) == "" {
		return fmt.Errorf("%w: an OpenAI-compatible endpoint has no name", ErrInvalid)
	}
	if e.BaseURL != "" {
		u, err := url.Parse(e.BaseURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("%w: endpoint %q: base_url %q is not an http(s) URL", ErrInvalid, e.Name, e.BaseURL)
		}
	}
	if e.APIKeyEnv != "" && strings.ContainsAny(e.APIKeyEnv, "= \t\n") {
		// A value that looks like an assignment or has whitespace is
		// almost certainly a key pasted where its variable's name belongs.
		return fmt.Errorf("%w: endpoint %q: api_key_env must name an environment variable, not hold a value", ErrInvalid, e.Name)
	}
	return nil
}

func (e OpenAICompatible) keyEnv() string {
	if e.APIKeyEnv == "" {
		return EnvOpenAIKey
	}
	return e.APIKeyEnv
}

// WithOpenAICompatible registers an OpenAI-compatible endpoint under its
// Name, replacing whatever was there, the built-in openai included. An
// invalid endpoint is registered as a provider that refuses to build, so the
// failure names the endpoint at the run that asked for it.
func WithOpenAICompatible(e OpenAICompatible) Option {
	return WithProvider(e.Name, OpenAICompatibleProvider(e))
}

// OpenAICompatibleProvider builds the provider for one endpoint: saige's
// Chat Completions adapter pointed at BaseURL, wrapped in the same retry
// decorator as the other built-ins. Streaming, tool calls and usage are the
// adapter's; models the saige catalog does not know get its conservative
// baseline, which streams and calls tools and claims no reasoning or price.
func OpenAICompatibleProvider(e OpenAICompatible) ProviderFunc {
	return func(spec Spec) (saigetypes.Provider, error) {
		if err := e.Validate(); err != nil {
			return nil, err
		}
		model := spec.Model
		if model == "" {
			model = e.Model
		}
		if model == "" {
			return nil, fmt.Errorf("%w: agent %q asks for the %s provider with no model", ErrInvalid, spec.Name, e.Name)
		}
		env := e.keyEnv()
		key := os.Getenv(env)
		if key == "" {
			return nil, fmt.Errorf("%w: agent %q asks for the %s provider but %s is unset", ErrInvalid, spec.Name, e.Name, env)
		}
		var opts []openai.Option
		if e.BaseURL != "" {
			opts = append(opts, openai.WithBaseURL(e.BaseURL))
		}
		a, err := openai.New(openai.Config{APIKey: key, Model: saigetypes.ModelID(model)}, opts...)
		if err != nil {
			return nil, fmt.Errorf("%w: agent %q: %v", ErrInvalid, spec.Name, err)
		}
		return withRetry(a)
	}
}
