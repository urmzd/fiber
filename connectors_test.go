package fiber

import (
	"reflect"
	"sort"
	"testing"

	"github.com/urmzd/fiber/pkg/blob"
	"github.com/urmzd/fiber/pkg/registry"
)

// The runtime reconciles a thread through the connector that owns its
// surface: every configured connector that can list a thread is registered
// under its own scheme, and one that cannot (Jira, the webhook sink) is not.
func TestEveryConnectorThatCanListAThreadIsWiredAsAReconciler(t *testing.T) {
	reg, err := buildConnectors(apply([]Option{
		WithTenant("acme"),
		WithSlack(Slack{Token: "xoxb-test", SigningSecret: "secret"}),
		WithGitHub(GitHub{Token: "ghs_test", WebhookSecret: "secret"}),
		WithJira(Jira{Sites: map[string]string{"acme": "https://acme.atlassian.net"}, Email: "ops@acme.example", Token: "token", WebhookSecret: "secret"}),
	}), registry.NewMemory(), blob.NewMemory())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for name := range listers(reg) {
		got = append(got, name)
	}
	sort.Strings(got)
	if want := []string{"github", "slack"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("reconcilers for %q, want %q", got, want)
	}
}
