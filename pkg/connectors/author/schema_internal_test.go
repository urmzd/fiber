package author

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/urmzd/fiber/pkg/definition"
)

// The schema a model drafts against offers exactly the fallback conditions a
// policy may name, so a draft never proposes one the source refuses.
func TestTheDraftSchemaOffersEveryFallbackCondition(t *testing.T) {
	t.Parallel()
	var schema struct {
		Properties struct {
			Policy struct {
				Properties struct {
					FallbackOn struct {
						Items struct {
							Enum []string `json:"enum"`
						} `json:"items"`
					} `json:"fallback_on"`
				} `json:"properties"`
			} `json:"policy"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(draftSchema), &schema); err != nil {
		t.Fatalf("draftSchema is not JSON: %v", err)
	}
	got := slices.Sorted(slices.Values(schema.Properties.Policy.Properties.FallbackOn.Items.Enum))
	if want := slices.Sorted(slices.Values(definition.FallbackConditions())); !slices.Equal(got, want) {
		t.Fatalf("schema offers %v, the policy accepts %v", got, want)
	}
}
