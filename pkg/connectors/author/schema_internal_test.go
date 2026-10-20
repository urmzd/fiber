package author

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/urmzd/fiber/pkg/agentspec"
)

// The schema a model drafts against offers exactly the fallback conditions a
// definition may name, so a draft never proposes one the registry refuses.
func TestTheDraftSchemaOffersEveryFallbackCondition(t *testing.T) {
	t.Parallel()
	var schema struct {
		Properties struct {
			FallbackOn struct {
				Items struct {
					Enum []string `json:"enum"`
				} `json:"items"`
			} `json:"fallback_on"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(specSchema), &schema); err != nil {
		t.Fatalf("specSchema is not JSON: %v", err)
	}
	got := slices.Sorted(slices.Values(schema.Properties.FallbackOn.Items.Enum))
	if want := agentspec.FallbackConditions(); !slices.Equal(got, want) {
		t.Fatalf("schema offers %v, agentspec accepts %v", got, want)
	}
}
