package fiber

import (
	"errors"
	"fmt"
)

// ErrConfig reports a composition that cannot be what its author meant: a
// half-configured connector, a trigger connector with no tenant to publish
// events in, a tool provider declared twice, a tool pinned with no schema hash. New
// fails with it before anything runs.
var ErrConfig = errors.New("fiber: invalid configuration")

// ErrUnavailable reports a backend New could not migrate. Retrying unchanged
// is the right response.
var ErrUnavailable = errors.New("fiber: backend unavailable")

func configf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrConfig, fmt.Sprintf(format, args...))
}

// Slack configures the built-in Slack connector (WithSlack). Token and
// SigningSecret together turn it on; one without the other is ErrConfig, and
// so is either without WithTenant.
type Slack struct {
	Token, SigningSecret string
	// Team is the workspace id deliveries are confined to.
	Team string
	// AppID marks messages this deployment posted when a thread is
	// reconciled.
	AppID string
	// Bots maps a Slack user id to the agent it stands for. A mention of one
	// names that agent on the event; the members of the channel decide who
	// answers.
	Bots map[string]string
}

// GitHub configures the built-in GitHub connector (WithGitHub). Token and
// WebhookSecret together turn it on, and it needs WithTenant.
type GitHub struct {
	Token, WebhookSecret string
	Owners               []string
	ReadRepositories     []string
	Bots                 map[string]string
	Checks               bool
	// Login is the app's own login. A comment that mentions it and names no
	// agent is published with no mentions, and the members of the issue's
	// channel decide whether anyone answers.
	Login string
}

// Jira configures the built-in Jira connector (WithJira). Sites (short key
// to base URL; the key is what a jira:// address names) turns it on, and then
// Email, Token, WebhookSecret and WithTenant are required.
type Jira struct {
	Sites                       map[string]string
	Email, Token, WebhookSecret string
	Bots                        map[string]string
}
