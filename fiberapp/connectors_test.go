package fiberapp

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
	t.Setenv(EnvSlackToken, "xoxb-test")
	t.Setenv(EnvSlackSecret, "secret")
	t.Setenv(EnvGitHubToken, "ghs_test")
	t.Setenv(EnvGitHubSecret, "secret")
	t.Setenv(EnvJiraSites, "acme=https://acme.atlassian.net")
	t.Setenv(EnvJiraEmail, "ops@acme.example")
	t.Setenv(EnvJiraToken, "token")
	t.Setenv(EnvJiraSecret, "secret")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tenant = "acme"
	reg, err := buildConnectors(cfg, extensions{}, registry.NewMemory(), blob.NewMemory())
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
