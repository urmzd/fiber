package agentrt

import (
	"strconv"

	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/mandatum/pkg/agentrt/payload"
)

// DefaultAccounting is the prefix a turn keys its calls under when the caller
// gave none (Turn.Accounting): unique within the turn, and nowhere else.
const DefaultAccounting = "turn"

// accountant turns saige's usage reports into usage.call bodies for one turn.
//
// What it knows about the model comes from saige: the rate card and the
// context window from the provider's declared capabilities, which saige's
// adapters resolve from its model catalog. A provider that declares nothing
// (an unknown model, a test double) is reported with its tokens and no cost
// or window, never with a guess.
type accountant struct {
	prefix string
	agent  string
	model  string
	caps   saigetypes.ModelCapabilities
	calls  int
}

func newAccountant(prefix, agent, model string, provider saigetypes.Provider) *accountant {
	if prefix == "" {
		prefix = DefaultAccounting
	}
	caps, _ := saigetypes.ProviderCapabilities(provider)
	// The provider's own name for its model first, then the catalog entry it
	// resolved to, then the name the pinned revision gave.
	model = firstNonEmpty(saigetypes.ProviderModel(provider), caps.Model, model)
	return &accountant{prefix: prefix, agent: agent, model: model, caps: caps}
}

// call reports one model call. saige reports usage once per completed provider
// request, already merged, so each delta is one call.
//
// A response saige served from its response cache repeats the counts of the
// call it replayed. Those tokens were not spent again: the call is reported
// with zero tokens, zero requests and no cost, and flagged, which is how saige
// itself settles it.
func (a *accountant) call(d saigetypes.UsageDelta) payload.UsageCall {
	a.calls++
	c := payload.UsageCall{
		// Always this turn's own key, never saige's: saige keeps its id
		// stable across a durable replay of its own, so two attempts' real
		// calls could share it and be counted once.
		AccountingID:      a.prefix + "/" + strconv.Itoa(a.calls),
		SaigeAccountingID: d.AccountingID,
		Agent:             a.agent,
		Model:             firstNonEmpty(d.ResponseModel, a.model),
		ContextWindow:     a.caps.ContextWindow,
	}
	if d.CacheHit {
		c.ResponseCacheHit = true
		return c
	}
	billed := saigetypes.UsageFromDelta(d)
	c.Requests = billed.Requests
	c.PromptTokens = d.PromptTokens
	c.CachedPromptTokens = d.CachedPromptTokens
	c.CacheWriteTokens = d.CacheWriteTokens
	c.CompletionTokens = d.CompletionTokens
	if billed.Total() == 0 {
		// The provider reported no tokens for a call it served: saige
		// still reports the call, and pricing it would claim it cost
		// nothing. It is counted, with no cost.
		return c
	}
	if p := a.caps.Pricing; !p.IsZero() && !p.Unpriced(billed) {
		currency := p.Currency
		if currency == "" {
			currency = saigetypes.DefaultCurrency
		}
		c.Cost = &payload.Cost{Micros: int64(p.Cost(billed)), Currency: currency, AsOf: p.AsOf}
	}
	return c
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
