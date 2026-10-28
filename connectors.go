package fiber

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"

	"github.com/urmzd/fiber/pkg/blob"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/connectors"
	"github.com/urmzd/fiber/pkg/connectors/cron"
	"github.com/urmzd/fiber/pkg/connectors/github"
	"github.com/urmzd/fiber/pkg/connectors/jira"
	"github.com/urmzd/fiber/pkg/connectors/mention"
	"github.com/urmzd/fiber/pkg/connectors/slack"
	"github.com/urmzd/fiber/pkg/connectors/webhook"
	"github.com/urmzd/fiber/pkg/definition"
	"github.com/urmzd/fiber/pkg/membership"
	"github.com/urmzd/fiber/pkg/signature"
	"github.com/urmzd/fiber/pkg/thread"
)

// listers collects the connectors that can list a thread's surface, keyed
// by the address scheme each owns, for runtime.WithReconciler.
func listers(reg *connector.Registry) thread.Listers {
	out := thread.Listers{}
	for _, name := range reg.Names() {
		c, err := reg.Get(name)
		if err != nil {
			continue
		}
		if l, ok := c.(thread.Lister); ok {
			out[name] = l
		}
	}
	return out
}

// buildConnectors registers every connector this deployment is configured for.
//
// Roles are DERIVED, never declared: a connector that implements Deliver is a
// sink, one that implements Ingest is a trigger, and the registry works that
// out from the types. So this function only has to construct; nothing here says
// what any connector is for.
func buildConnectors(cfg options, store definition.Source, blobs blob.Store, members membership.Store) (*connector.Registry, error) {
	reg := connector.NewRegistry()
	// Agent names come from the control plane, which is the only place that
	// knows them. Without a name set, "@urmzd" and "@docs-bot" are the same
	// syntax on every one of these surfaces, and guessing would start a run
	// each time one human tagged another.
	agents := agentNames{store: store, tenant: cfg.Tenant}
	// Files on a triggering message are brought in under the deployment's
	// tenant, into the app's blob store.
	var files connectors.Files
	if cfg.Tenant != "" && blobs != nil {
		files = connectors.Files{Store: blobs, Log: cfg.Log}
	}

	for _, build := range []func(options, mention.Set, connectors.Files) (connector.Connector, error){
		newSlack, newGitHub, newJira, newWebhook,
		func(o options, _ mention.Set, _ connectors.Files) (connector.Connector, error) {
			return newCron(o, members)
		},
	} {
		c, err := build(cfg, agents, files)
		if err != nil {
			return nil, err
		}
		if c == nil {
			continue // Not configured, which is a valid deployment.
		}
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("fiber: register %s: %w", c.Name(), err)
		}
	}
	// Connectors a binary built on fiber added, after the built-in ones: a
	// name both claim is refused by the registry rather than shadowed.
	for _, c := range cfg.Connectors {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("fiber: register %s: %w", c.Name(), err)
		}
	}
	return reg, nil
}

func newSlack(cfg options, agents mention.Set, files connectors.Files) (connector.Connector, error) {
	sc := cfg.Slack
	token, secret := sc.Token, sc.SigningSecret
	if token == "" && secret == "" {
		return nil, nil
	}
	if token == "" || secret == "" {
		return nil, configf("slack needs both a token and a signing secret")
	}
	if err := needTenant(cfg, "slack"); err != nil {
		return nil, err
	}
	c, err := slack.New(slack.Config{
		API:       slack.NewHTTPAPI(token, http.DefaultClient),
		Verifier:  signature.NewSlack(secret),
		Team:      sc.Team,
		Bots:      sc.Bots,
		Tenant:    cfg.Tenant,
		Files:     files,
		FileToken: token,
		AppID:     sc.AppID,
	})
	if err != nil {
		return nil, fmt.Errorf("fiber: slack: %w", err)
	}
	return c, nil
}

func newGitHub(cfg options, agents mention.Set, files connectors.Files) (connector.Connector, error) {
	gc := cfg.GitHub
	token, secret := gc.Token, gc.WebhookSecret
	if token == "" && secret == "" {
		return nil, nil
	}
	if token == "" || secret == "" {
		return nil, configf("github needs both a token and a webhook secret")
	}
	if err := needTenant(cfg, "github"); err != nil {
		return nil, err
	}
	c, err := github.New(github.Config{
		API:              github.NewHTTPAPI(token, http.DefaultClient),
		Verifier:         signature.NewGitHub(secret),
		Owners:           gc.Owners,
		Bots:             gc.Bots,
		Agents:           agents,
		Checks:           gc.Checks,
		ReadRepositories: gc.ReadRepositories,
		Login:            gc.Login,
		Tenant:           cfg.Tenant,
		Files:            files,
	})
	if err != nil {
		return nil, fmt.Errorf("fiber: github: %w", err)
	}
	return c, nil
}

func newJira(cfg options, agents mention.Set, files connectors.Files) (connector.Connector, error) {
	jc := cfg.Jira
	if len(jc.Sites) == 0 {
		return nil, nil
	}
	email, token := jc.Email, jc.Token
	if email == "" || token == "" {
		return nil, configf("jira needs an email and a token")
	}
	secret := jc.WebhookSecret
	if secret == "" {
		return nil, configf("jira needs a webhook secret: Jira signs nothing, so a shared secret is the only thing standing between its webhook and the internet")
	}
	if err := needTenant(cfg, "jira"); err != nil {
		return nil, err
	}
	sites := map[string]jira.Site{}
	for key, base := range jc.Sites {
		sites[key] = jira.Site{BaseURL: strings.TrimRight(base, "/"), Email: email, Token: token}
	}
	api, err := jira.NewHTTPAPI(sites, http.DefaultClient)
	if err != nil {
		return nil, fmt.Errorf("fiber: jira: %w", err)
	}
	c, err := jira.New(jira.Config{
		API:      api,
		Sites:    sites,
		Verifier: jiraSecret(secret),
		Bots:     jc.Bots,
		Agents:   agents,
		Tenant:   cfg.Tenant,
		Files:    files,
	})
	if err != nil {
		return nil, fmt.Errorf("fiber: jira: %w", err)
	}
	return c, nil
}

// needTenant refuses a trigger connector in a deployment with no tenant. A
// webhook from Slack, GitHub or Jira carries no credential to derive one
// from, so the connector resolves mentions and stamps events in the
// deployment's tenant (WithTenant); with none, no mention would name an agent
// and no event would be dispatched, and the connector would be configured and
// silently dead. A schedule carries its own tenant and does not need one.
func needTenant(cfg options, name string) error {
	if cfg.Tenant == "" {
		return configf("%s publishes events in the deployment's tenant, and none is set: pass WithTenant", name)
	}
	return nil
}

// newWebhook builds the outbound webhook sink. It publishes nothing, so it
// has no files to bring in.
func newWebhook(cfg options, _ mention.Set, _ connectors.Files) (connector.Connector, error) {
	if len(cfg.Webhooks) == 0 {
		return nil, nil
	}
	endpoints := map[string]webhook.Endpoint{}
	for name, ep := range cfg.Webhooks {
		if ep.Secret == "" {
			return nil, configf("webhook workspace %q has no secret, and a receiver that accepts unsigned events accepts them from anyone who learned the URL", name)
		}
		ep.BaseURL = strings.TrimRight(ep.BaseURL, "/")
		endpoints[name] = ep
	}
	c, err := webhook.New(webhook.Config{Endpoints: endpoints})
	if err != nil {
		return nil, fmt.Errorf("fiber: webhook: %w", err)
	}
	return c, nil
}

// newCron builds the schedule trigger. Each schedule's agent is made a member
// of the schedule's channel in members.
func newCron(cfg options, members membership.Store) (connector.Connector, error) {
	if len(cfg.Schedules) == 0 {
		return nil, nil
	}
	store, err := cron.NewMemory(cfg.Schedules...)
	if err != nil {
		return nil, fmt.Errorf("fiber: cron: %w", err)
	}
	c, err := cron.New(cron.Config{Store: store, Log: cfg.Log, Members: members})
	if err != nil {
		return nil, fmt.Errorf("fiber: cron: %w", err)
	}
	return c, nil
}

// HeaderJiraSecret carries the shared secret a Jira webhook is registered with.
// Jira signs nothing, so the secret is whatever the person who configured the
// URL put in a header; see jiraSecret.
const HeaderJiraSecret = "X-Fiber-Secret"

// jiraSecret authenticates a Jira webhook by a shared secret in a header.
//
// That is strictly weaker than a MAC: it does not bind the secret to the body,
// so anyone who captures one request can replay a different one, and it is all
// Jira offers. The mitigation is the same as everywhere else here: event id
// idempotency downstream, plus TLS. The comparison is constant time because
// this endpoint is public and can be probed as often as an attacker likes.
type jiraSecret string

func (s jiraSecret) Verify(h http.Header, _ []byte) error {
	got := h.Values(HeaderJiraSecret)
	if len(got) != 1 || subtle.ConstantTimeCompare([]byte(got[0]), []byte(s)) != 1 {
		return fmt.Errorf("%w: %s does not match the configured secret", signature.ErrSignatureMismatch, HeaderJiraSecret)
	}
	return nil
}

// agentNames answers "does this token name an agent" from the control plane.
//
// mention.Set has no context because the question is asked deep inside parsing
// a payload, where there is nothing to plumb one from and nothing to cancel: a
// lookup is a map read. The registry is the authority for what agents exist, so
// an agent that has not been created cannot be mentioned into existence, which
// is the property that keeps a mention of a colleague from starting a run.
type agentNames struct {
	store  definition.Source
	tenant string
}

func (a agentNames) Lookup(token string) (string, bool) {
	if !mention.Valid(token) {
		return "", false
	}
	rec, err := a.store.Latest(context.Background(), a.tenant, definition.NormalizeName(token))
	if err != nil {
		return "", false
	}
	return rec.Name, true
}
