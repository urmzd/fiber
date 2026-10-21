package fiberapp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/urmzd/playscape/pkg/metrics"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/connectors/cron"
	"github.com/urmzd/fiber/pkg/connectors/webhook"
)

// ErrConfig reports a configuration that cannot be what its author meant: a
// half-configured connector, a malformed URL, a providers file with a typo.
// New fails with it before anything runs.
var ErrConfig = errors.New("fiberapp: invalid configuration")

// ErrUnavailable reports a backend New could not reach or migrate. Retrying
// unchanged is the right response.
var ErrUnavailable = errors.New("fiberapp: backend unavailable")

func configf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrConfig, fmt.Sprintf(format, args...))
}

// DefaultTenant is the tenant ConfigFromEnv uses when $FIBER_TENANT is unset.
// It is a real scope rather than a blank one, so that turning multi-tenancy on
// later is a configuration change and not a data migration.
const DefaultTenant = "default"

// Config is everything an App is built from that is data rather than code.
// Every backend is chosen here by a URL or a path; the zero value is a
// complete single-process system in memory, with files under the user's
// cache directory. Code-shaped choices (a model provider, a connector, a
// replacement backend) are Options.
type Config struct {
	// Tenant is the authorization scope this deployment serves. It is
	// stamped onto every tag a connector raises (a trigger has no credential
	// to derive one from), the author agent is seeded for it, and declared
	// tool providers are recorded under it. Empty seeds nothing.
	Tenant string

	// Bus selects the broker by URL scheme (see bus.Open): empty or
	// "memory:" keeps it in this process, redis:// puts it on Redis
	// Streams, postgres:// on Postgres. A Bus equal to Postgres shares
	// that pool.
	Bus string

	// Postgres, when set, selects Postgres for every store family: the
	// agent registry, the run ledger and queue, knowledge, thread journals
	// and MCP tool providers. Each is migrated by New. Empty keeps them all
	// in memory, where they are complete and forget everything on exit.
	Postgres string

	// Workspace is the directory everything this process writes to disk
	// lives under: agent artifacts ("workspace/") and uploaded or ingested
	// files ("blobs/", content addressed). Empty takes a directory under
	// the user's cache.
	Workspace string

	// ProvidersFile is a JSON file of OpenAI-compatible model endpoints and
	// MCP tool providers (see README.md). Empty loads none.
	ProvidersFile string

	// Embeddings selects the model the knowledge layer embeds with. The
	// zero value is the offline hashing embedder.
	Embeddings Embeddings

	// AuthorProvider and AuthorModel are what the built-in author agent
	// runs on. Empty takes the offline provider.
	AuthorProvider, AuthorModel string

	// Compaction is the history compaction every agent runs with. The zero
	// value never compacts. OnRequest lets a host ask a running turn to
	// compact (App.Compact).
	Compaction agentrt.Compaction

	// Concurrency is how many runs the worker executes at once. Zero means
	// one.
	Concurrency int

	// Schedules are the cron connector's standing requests. Empty registers
	// no cron connector at all.
	Schedules []cron.Schedule

	// Built-in connectors. Each is registered only when configured, and a
	// half-configured one is ErrConfig: a token with no signing secret would
	// authenticate the internet.
	Slack    Slack
	GitHub   GitHub
	Jira     Jira
	Webhooks map[string]webhook.Endpoint

	// Log receives operational output. Nil discards it.
	Log *slog.Logger
	// Metrics receives counters. Nil records nothing.
	Metrics metrics.Recorder
}

// Embeddings selects an OpenAI-compatible embeddings API. An empty URL keeps
// the offline hashing embedder, which needs no key and matches on word
// overlap rather than meaning. Indexing and searching must agree on it: a
// corpus embedded with one model is not searchable with another.
type Embeddings struct {
	URL, Model, Key string
	Dimensions      int
}

// Slack configures the Slack connector. Token and SigningSecret together
// turn it on.
type Slack struct {
	Token, SigningSecret string
	// Team is the workspace id deliveries are confined to.
	Team string
	// AppID marks messages this deployment posted when a thread is
	// reconciled.
	AppID string
	// DefaultAgent answers a mention of the app that names no agent.
	DefaultAgent string
	// Bots maps a Slack user id to the agent it stands for.
	Bots map[string]string
}

// GitHub configures the GitHub connector. Token and WebhookSecret together
// turn it on.
type GitHub struct {
	Token, WebhookSecret string
	Owners               []string
	ReadRepositories     []string
	Bots                 map[string]string
	Checks               bool
}

// Jira configures the Jira connector. Sites (short key to base URL) turns it
// on, and then Email, Token and WebhookSecret are required.
type Jira struct {
	Sites                       map[string]string
	Email, Token, WebhookSecret string
	Bots                        map[string]string
}

// Environment variables ConfigFromEnv reads. Everything that names an
// external system is an environment variable rather than a flag, because it
// is deployment configuration, and because a credential in a flag ends up in
// shell history.
const (
	EnvTenant     = "FIBER_TENANT"
	EnvBus        = "FIBER_BUS_URL"
	EnvRedis      = "FIBER_REDIS_URL" // used as the bus when FIBER_BUS_URL is unset
	EnvPostgres   = "FIBER_POSTGRES_URL"
	EnvWorkspace  = "FIBER_WORKSPACE"
	EnvProviders  = "FIBER_PROVIDERS"
	EnvSchedules  = "FIBER_SCHEDULES"
	EnvConcurrent = "FIBER_CONCURRENCY"

	EnvAuthorProvider = "FIBER_AUTHOR_PROVIDER"
	EnvAuthorModel    = "FIBER_AUTHOR_MODEL"

	EnvCompactTokens    = "FIBER_COMPACT_TOKENS"
	EnvCompactKeepTurns = "FIBER_COMPACT_KEEP_TURNS"
	EnvCompactOnRequest = "FIBER_COMPACT_ON_REQUEST"

	EnvEmbeddingsURL        = "FIBER_EMBEDDINGS_URL"
	EnvEmbeddingsModel      = "FIBER_EMBEDDINGS_MODEL"
	EnvEmbeddingsKey        = "FIBER_EMBEDDINGS_KEY"
	EnvEmbeddingsDimensions = "FIBER_EMBEDDINGS_DIMENSIONS"

	EnvSlackToken   = "FIBER_SLACK_TOKEN"
	EnvSlackSecret  = "FIBER_SLACK_SIGNING_SECRET"
	EnvSlackTeam    = "FIBER_SLACK_TEAM"
	EnvSlackBots    = "FIBER_SLACK_BOTS"
	EnvSlackDefault = "FIBER_SLACK_DEFAULT_AGENT"
	EnvSlackAppID   = "FIBER_SLACK_APP_ID"

	EnvGitHubReadRepositories = "FIBER_GITHUB_READ_REPOSITORIES"
	EnvGitHubToken            = "FIBER_GITHUB_TOKEN"
	EnvGitHubSecret           = "FIBER_GITHUB_WEBHOOK_SECRET"
	EnvGitHubOwners           = "FIBER_GITHUB_OWNERS"
	EnvGitHubBots             = "FIBER_GITHUB_BOTS"
	EnvGitHubChecks           = "FIBER_GITHUB_CHECKS"

	// EnvJiraSites is a comma-separated list of key=base-URL pairs; the
	// key, not the hostname, is what appears in a jira:// address.
	EnvJiraSites  = "FIBER_JIRA_SITES"
	EnvJiraEmail  = "FIBER_JIRA_EMAIL"
	EnvJiraToken  = "FIBER_JIRA_TOKEN"
	EnvJiraSecret = "FIBER_JIRA_WEBHOOK_SECRET"
	EnvJiraBots   = "FIBER_JIRA_BOTS"

	// EnvWebhookEndpoints is a comma-separated list of
	// workspace=base-url=secret triples. The workspace is an indirection so
	// that a route in an agent spec cannot name an arbitrary URL.
	EnvWebhookEndpoints = "FIBER_WEBHOOK_ENDPOINTS"
)

// ConfigFromEnv reads a Config from the FIBER_* environment variables, for a
// program that wants twelve-factor configuration without writing the
// mapping itself. It validates what it parses and reads files it names
// ($FIBER_SCHEDULES); it opens nothing.
func ConfigFromEnv() (Config, error) {
	for _, name := range []string{"FIBER_GITHUB_AUTO_LABELS", "FIBER_GITHUB_GUIDES", "FIBER_GITHUB_MUTATIONS", "FIBER_SLACK_SESSIONS", "FIBER_SLACK_COMMANDS"} {
		if os.Getenv(name) != "" {
			return Config{}, configf("%s moved to warp; use WARP_* with warp", name)
		}
	}
	env := func(k string) string { return strings.TrimSpace(os.Getenv(k)) }
	cfg := Config{
		Tenant:         env(EnvTenant),
		Bus:            env(EnvBus),
		Postgres:       env(EnvPostgres),
		Workspace:      env(EnvWorkspace),
		ProvidersFile:  env(EnvProviders),
		AuthorProvider: env(EnvAuthorProvider),
		AuthorModel:    env(EnvAuthorModel),
		Embeddings: Embeddings{
			URL:   env(EnvEmbeddingsURL),
			Model: env(EnvEmbeddingsModel),
			Key:   os.Getenv(EnvEmbeddingsKey),
		},
		Slack: Slack{
			Token:         os.Getenv(EnvSlackToken),
			SigningSecret: os.Getenv(EnvSlackSecret),
			Team:          env(EnvSlackTeam),
			AppID:         env(EnvSlackAppID),
			DefaultAgent:  env(EnvSlackDefault),
			Bots:          pairs(os.Getenv(EnvSlackBots)),
		},
		GitHub: GitHub{
			Token:            os.Getenv(EnvGitHubToken),
			WebhookSecret:    os.Getenv(EnvGitHubSecret),
			Owners:           list(os.Getenv(EnvGitHubOwners)),
			ReadRepositories: list(os.Getenv(EnvGitHubReadRepositories)),
			Bots:             pairs(os.Getenv(EnvGitHubBots)),
		},
		Jira: Jira{
			Sites:         pairs(os.Getenv(EnvJiraSites)),
			Email:         os.Getenv(EnvJiraEmail),
			Token:         os.Getenv(EnvJiraToken),
			WebhookSecret: os.Getenv(EnvJiraSecret),
			Bots:          pairs(os.Getenv(EnvJiraBots)),
		},
	}
	if cfg.Tenant == "" {
		cfg.Tenant = DefaultTenant
	}
	if cfg.Bus == "" {
		cfg.Bus = env(EnvRedis)
	}
	if os.Getenv(EnvJiraSites) != "" && len(cfg.Jira.Sites) == 0 {
		return Config{}, configf("$%s named no sites; want acme=https://acme.atlassian.net", EnvJiraSites)
	}
	var err error
	if cfg.GitHub.Checks, err = boolEnv(EnvGitHubChecks); err != nil {
		return Config{}, err
	}
	if cfg.Embeddings.Dimensions, err = intEnv(EnvEmbeddingsDimensions); err != nil {
		return Config{}, err
	}
	if cfg.Compaction.MaxInputTokens, err = intEnv(EnvCompactTokens); err != nil {
		return Config{}, err
	}
	if cfg.Compaction.KeepTurns, err = intEnv(EnvCompactKeepTurns); err != nil {
		return Config{}, err
	}
	if cfg.Compaction.OnRequest, err = boolEnv(EnvCompactOnRequest); err != nil {
		return Config{}, err
	}
	if cfg.Concurrency, err = intEnv(EnvConcurrent); err != nil {
		return Config{}, err
	}
	if cfg.Webhooks, err = webhookEndpoints(os.Getenv(EnvWebhookEndpoints)); err != nil {
		return Config{}, err
	}
	if cfg.Schedules, err = LoadSchedules(env(EnvSchedules)); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// LoadSchedules reads the cron connector's standing requests from a JSON
// file: an array of schedules, each validated, so a typo fails at startup
// rather than at 2am. An empty path is no schedules.
func LoadSchedules(path string) ([]cron.Schedule, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, configf("schedules: %v", err)
	}
	var out []cron.Schedule
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, configf("schedules %q: %v", path, err)
	}
	for i, s := range out {
		if err := s.Validate(); err != nil {
			return nil, configf("schedule %d in %q: %v", i, path, err)
		}
	}
	return out, nil
}

func webhookEndpoints(spec string) (map[string]webhook.Endpoint, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	out := map[string]webhook.Endpoint{}
	for _, entry := range list(spec) {
		name, rest, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, configf("$%s entry %q is not workspace=url=secret", EnvWebhookEndpoints, entry)
		}
		base, secret, ok := strings.Cut(rest, "=")
		if !ok || secret == "" {
			return nil, configf("$%s entry %q has no secret, and a receiver that accepts unsigned events accepts them from anyone who learned the URL", EnvWebhookEndpoints, entry)
		}
		out[name] = webhook.Endpoint{BaseURL: strings.TrimRight(base, "/"), Secret: secret}
	}
	return out, nil
}

// intEnv reads a non-negative integer, treating unset as zero.
func intEnv(key string) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, configf("$%s must be a non-negative integer, got %q", key, raw)
	}
	return n, nil
}

// boolEnv reads a boolean, treating unset as false.
func boolEnv(key string) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return false, configf("$%s must be true or false, got %q", key, raw)
	}
	return b, nil
}

// pairs parses "k=v,k=v" into a map. Empty entries are skipped so that a
// trailing comma is not a configuration error.
func pairs(s string) map[string]string {
	out := map[string]string{}
	for _, entry := range list(s) {
		k, v, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// list splits a comma-separated environment value.
func list(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
