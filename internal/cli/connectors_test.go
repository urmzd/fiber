package cli

import (
	"reflect"
	"sort"
	"testing"

	"github.com/urmzd/mandatum/pkg/blob"
	"github.com/urmzd/mandatum/pkg/registry"
)

// The runtime reconciles a thread through the connector that owns its
// surface: every configured connector that can list a thread is registered
// under its own scheme, and one that cannot (Jira, the webhook sink) is not.
func TestEveryConnectorThatCanListAThreadIsWiredAsAReconciler(t *testing.T) {
	t.Setenv(EnvSlackToken, "xoxb-test")
	t.Setenv(EnvSlackSecret, "secret")
	t.Setenv(EnvGitHubToken, "ghs_test")
	t.Setenv(EnvGitHubSecret, "secret")
	t.Setenv(EnvJiraSites, "acme=https://acme.atlassian.net")
	t.Setenv(EnvJiraEmail, "ops@acme.example")
	t.Setenv(EnvJiraToken, "token")
	t.Setenv(EnvJiraSecret, "secret")
	reg, err := buildConnectors(coreConfig{Tenant: "acme"}, registry.NewMemory(), blob.NewMemory())
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
