package agentrt_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/pkg/agentrt"
)

// models registers one scripted model per provider name and records the spec
// each ProviderFunc was handed.
type models struct {
	mu    sync.Mutex
	specs map[string]agentrt.Spec
	opts  []agentrt.Option
}

func newModels(byProvider map[string]saigetypes.Provider) *models {
	m := &models{specs: map[string]agentrt.Spec{}}
	for name, p := range byProvider {
		m.opts = append(m.opts, agentrt.WithProvider(name, func(spec agentrt.Spec) (saigetypes.Provider, error) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.specs[name] = spec
			return p, nil
		}))
	}
	return m
}

func fallbackRevision() agentrt.Revision {
	rev := offlineRevision(1, "You answer.")
	rev.Spec.Provider, rev.Spec.Model = "primary", "big-model"
	rev.Spec.Fallbacks = []agentrt.ModelRef{{Provider: "backup", Model: "small-model"}}
	return rev
}

func providerError(kind saigetypes.ErrorKind) error {
	return &saigetypes.ProviderError{Provider: "primary", Model: "big-model", Kind: kind, Err: errors.New(kind.String())}
}

// A call the primary model cannot serve for a reason another model could fix
// is answered by the next model in the spec's order, whether the primary
// failed to start or failed on its stream before saying anything. Each model
// is built from a spec that names it alone.
func TestATransientFailureFallsThroughToTheNextModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		primary *agenttest.ScriptedProvider
	}{
		{
			name:    "an outage before streaming",
			primary: &agenttest.ScriptedProvider{Errors: []error{providerError(saigetypes.ErrorKindUnavailable)}},
		},
		{
			name:    "a rate limit before streaming",
			primary: &agenttest.ScriptedProvider{Errors: []error{providerError(saigetypes.ErrorKindRateLimit)}},
		},
		{
			name: "an outage on the stream before any output",
			primary: &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{
				{saigetypes.ErrorDelta{Error: providerError(saigetypes.ErrorKindUnavailable)}},
			}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backup := &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{agenttest.TextResponse("from the backup")}}
			m := newModels(map[string]saigetypes.Provider{"primary": tc.primary, "backup": backup})
			r, err := agentrt.New(fallbackRevision(), m.opts...)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			_, res := collect(t, r, agentrt.Turn{Text: "hi"})
			if res.Text != "from the backup" {
				t.Fatalf("answer is %q, want the backup's", res.Text)
			}
			if tc.primary.CallCount() != 1 || backup.CallCount() != 1 {
				t.Fatalf("primary called %d times, backup %d; want once each", tc.primary.CallCount(), backup.CallCount())
			}
			for provider, model := range map[string]string{"primary": "big-model", "backup": "small-model"} {
				spec := m.specs[provider]
				if spec.Provider != provider || spec.Model != model || spec.Fallbacks != nil {
					t.Errorf("the %s model was built from provider %q model %q fallbacks %v, want %q %q and none",
						provider, spec.Provider, spec.Model, spec.Fallbacks, provider, model)
				}
				if spec.Name != "docs-bot" || spec.SystemPrompt != "You answer." {
					t.Errorf("the %s model was built from a spec that lost the agent: %+v", provider, spec)
				}
			}
		})
	}
}

// A failure no other model can fix ends the turn on the primary: the next
// model is never asked, and the caller gets the primary's reason.
func TestATerminalFailureDoesNotFallThrough(t *testing.T) {
	t.Parallel()

	primary := &agenttest.ScriptedProvider{Errors: []error{providerError(saigetypes.ErrorKindInvalidRequest)}}
	backup := &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{agenttest.TextResponse("from the backup")}}
	m := newModels(map[string]saigetypes.Provider{"primary": primary, "backup": backup})
	r, err := agentrt.New(fallbackRevision(), m.opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = r.Run(context.Background(), agentrt.Turn{Text: "hi"}, nil)
	if err == nil {
		t.Fatal("Run succeeded although the request was invalid")
	}
	if saigetypes.KindOf(err) != saigetypes.ErrorKindInvalidRequest {
		t.Errorf("error %v is of kind %v, want the primary's invalid_request", err, saigetypes.KindOf(err))
	}
	if backup.CallCount() != 0 {
		t.Fatalf("the backup was called %d times for a request no model can serve", backup.CallCount())
	}
}

// A fallback is built with the agent, so one this process cannot build fails
// the build as the primary would, naming which fallback it was.
func TestAFallbackThatCannotBeBuiltFailsTheBuild(t *testing.T) {
	t.Parallel()

	primary := &agenttest.ScriptedProvider{}
	m := newModels(map[string]saigetypes.Provider{"primary": primary})

	unknown := fallbackRevision()
	if _, err := agentrt.New(unknown, m.opts...); !errors.Is(err, agentrt.ErrInvalid) || !strings.Contains(err.Error(), "fallback 1") {
		t.Fatalf("New with an unregistered fallback provider returned %v, want ErrInvalid naming fallback 1", err)
	}

	broken := errors.New("no credentials")
	opts := append(m.opts, agentrt.WithProvider("backup", func(agentrt.Spec) (saigetypes.Provider, error) { return nil, broken }))
	if _, err := agentrt.New(fallbackRevision(), opts...); !errors.Is(err, broken) || !strings.Contains(err.Error(), `fallback 1 (backup "small-model")`) {
		t.Fatalf("New with a fallback that fails to build returned %v, want its error naming the fallback", err)
	}
}

// Each call is priced on the rate card of the model that answered it, so a
// run that fell back is not billed at the primary's rates.
func TestAFallbackCallIsPricedOnItsOwnModel(t *testing.T) {
	t.Parallel()

	primaryCaps := sonnetLike
	primaryCaps.Model = "big-model"
	backupCaps := sonnetLike
	backupCaps.Model = "small-model"
	backupCaps.ContextWindow = 32_000
	backupCaps.Pricing.InputPerMTok, backupCaps.Pricing.OutputPerMTok = 1, 5

	usage := saigetypes.UsageDelta{ResponseModel: "small-model-20260101", PromptTokens: 1_000, CompletionTokens: 100}
	primary := &priced{caps: primaryCaps, ScriptedProvider: &agenttest.ScriptedProvider{Errors: []error{providerError(saigetypes.ErrorKindUnavailable)}}}
	backup := &priced{caps: backupCaps, ScriptedProvider: &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{
		withUsage(agenttest.TextResponse("from the backup"), usage),
	}}}
	m := newModels(map[string]saigetypes.Provider{"primary": primary, "backup": backup})
	r, err := agentrt.New(fallbackRevision(), m.opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	chunks, _ := collect(t, r, agentrt.Turn{Text: "hi"})
	calls := usageCalls(t, chunks)
	if len(calls) != 1 {
		t.Fatalf("published %d usage.call events, want 1", len(calls))
	}
	want := int64(backupCaps.Pricing.Cost(saigetypes.UsageFromDelta(usage)))
	if c := calls[0]; c.Cost == nil || c.Cost.Micros != want || c.ContextWindow != 32_000 || c.Model != "small-model-20260101" {
		t.Fatalf("call is %+v cost %+v, want model small-model-20260101 priced %d micros on the backup's card with its window", c, c.Cost, want)
	}
}
