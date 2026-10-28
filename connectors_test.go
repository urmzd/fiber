package fiber

import (
	"reflect"
	"sort"
	"testing"

	"github.com/urmzd/fiber/pkg/blob"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/membership"
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
	}), definition.NewMemory(), blob.NewMemory(), membership.NewMemory())
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

// WithGitHub hands the connector the login the app is mentioned by, so a
// login the connector cannot use fails New.
func TestWithGitHubHandsTheConnectorItsLogin(t *testing.T) {
	build := func(g GitHub) error {
		g.Token, g.WebhookSecret = "ghs_test", "secret"
		_, err := buildConnectors(apply([]Option{WithTenant("acme"), WithGitHub(g)}), definition.NewMemory(), blob.NewMemory(), membership.NewMemory())
		return err
	}
	if err := build(GitHub{Login: "fiber[bot]"}); err != nil {
		t.Fatal(err)
	}
	if err := build(GitHub{Login: "fiber bot"}); err == nil {
		t.Fatal("a login with a space was accepted")
	}
}
