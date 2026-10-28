package author_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urmzd/fiber/pkg/address"
	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentrt/payload"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/connector"
	"github.com/urmzd/fiber/pkg/connectors/author"
	"github.com/urmzd/fiber/pkg/registry"
	"github.com/urmzd/fiber/pkg/turn/native/agenttest"
)

// runs is the test's run ledger: which principal each run acts as.
type runs map[string]author.Principal

func (r runs) Principal(_ context.Context, runID string) (author.Principal, error) {
	p, ok := r[runID]
	if !ok {
		return author.Principal{}, errors.New("no such run")
	}
	return p, nil
}

// The fixture: two tenants, and in the first one two humans in separate runs.
var fixture = runs{
	"run-ada":   {Tenant: "acme", Actor: "slack:ada"},
	"run-grace": {Tenant: "acme", Actor: "slack:grace"},
	"run-cron":  {Tenant: "acme"},
	"run-other": {Tenant: "globex", Actor: "slack:eve"},
}

// counting wraps a store and counts the calls that change it.
type counting struct {
	registry.Store
	writes atomic.Int64
}

func (c *counting) Create(ctx context.Context, tenant string, spec agentspec.AgentSpec, by string) (agentspec.Revision, error) {
	c.writes.Add(1)
	return c.Store.Create(ctx, tenant, spec, by)
}

func (c *counting) Revise(ctx context.Context, tenant string, spec agentspec.AgentSpec, rev int, by string) (agentspec.Revision, error) {
	c.writes.Add(1)
	return c.Store.Revise(ctx, tenant, spec, rev, by)
}

func (c *counting) Delete(ctx context.Context, tenant, name string, expectedRev int) error {
	c.writes.Add(1)
	return c.Store.Delete(ctx, tenant, name, expectedRev)
}

type harness struct {
	t     *testing.T
	store *counting
	conn  *author.Connector
	now   *time.Time
}

func newHarness(t *testing.T, edit ...func(*author.Config)) *harness {
	t.Helper()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	h := &harness{t: t, store: &counting{Store: registry.NewMemory()}, now: &now}
	cfg := author.Config{
		Store:      h.store,
		Principals: fixture,
		Clock:      func() time.Time { return *h.now },
	}
	for _, e := range edit {
		e(&cfg)
	}
	conn, err := author.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.conn = conn
	return h
}

// call invokes an action as a run, the way the runtime's catalog does: the run
// id arrives in the context and the arguments are whatever the model wrote.
func (h *harness) call(runID, action string, args any) (connector.Result, error) {
	h.t.Helper()
	raw, ok := args.(string)
	if !ok {
		b, err := json.Marshal(args)
		if err != nil {
			h.t.Fatalf("marshal args: %v", err)
		}
		raw = string(b)
	}
	ctx := context.Background()
	if runID != "" {
		ctx = connector.WithRunID(ctx, runID)
	}
	for _, a := range h.conn.Actions() {
		if a.Name == action {
			// A hostile target must make no difference: it names a surface,
			// not a tenant.
			return a.Invoke(ctx, address.Address{Connector: "slack", Workspace: "globex"}, json.RawMessage(raw))
		}
	}
	h.t.Fatalf("no action %q", action)
	return connector.Result{}, nil
}

func (h *harness) must(runID, action string, args any) connector.Result {
	h.t.Helper()
	res, err := h.call(runID, action, args)
	if err != nil {
		h.t.Fatalf("%s as %s: %v", action, runID, err)
	}
	return res
}

func (h *harness) seed(tenant string, spec agentspec.AgentSpec) agentspec.Revision {
	h.t.Helper()
	rev, err := h.store.Store.Create(context.Background(), tenant, spec, "setup")
	if err != nil {
		h.t.Fatalf("seed %s/%s: %v", tenant, spec.Name, err)
	}
	return rev
}

// token extracts the proposal token from a propose result.
func token(t *testing.T, res connector.Result) string {
	t.Helper()
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(res.Data, &out); err != nil || out.Token == "" {
		t.Fatalf("no token in %s (%v)", res.Data, err)
	}
	return out.Token
}

func spec(name, prompt string) agentspec.AgentSpec {
	return agentspec.AgentSpec{
		Name:         name,
		Description:  "test agent",
		Model:        "offline",
		Provider:     agentspec.ProviderOffline,
		SystemPrompt: prompt,
	}
}

const goodPrompt = "You are a docs agent. Your goal is to answer from the docs. Output format: concise plain text."

// validArgs returns arguments each action accepts, so a test can drive the
// whole catalog.
func validArgs(action string) map[string]any {
	draft := spec("docs", goodPrompt)
	switch action {
	case "author_list", "author_pending":
		return map[string]any{}
	case "author_get", "author_history":
		return map[string]any{"name": "docs"}
	case "author_diff":
		return map[string]any{"name": "docs", "from": 1}
	case "author_validate", "author_review":
		return map[string]any{"spec": draft}
	case "author_scaffold":
		return map[string]any{
			"router":      map[string]any{"name": "front", "role": "Routes requests."},
			"specialists": []any{map[string]any{"name": "support", "role": "Handles support."}},
			"model":       "offline", "provider": "offline",
		}
	case "author_propose_create":
		return map[string]any{"spec": spec("fresh", goodPrompt)}
	case "author_propose_revise":
		draft.Description = "edited"
		return map[string]any{"spec": draft, "expected_rev": 1}
	case "author_propose_delete":
		return map[string]any{"name": "docs", "expected_rev": 1}
	case "author_apply":
		return map[string]any{"token": "prop_unknown"}
	case author.KnowledgeToolName:
		return map[string]any{"query": "prompts"}
	}
	return nil
}

// The property the whole package exists to hold: a run reaches its own
// tenant's agents and no argument, target or token reaches anybody else's.
func TestTheTenantComesFromTheRunAndCannotBeNamed(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.seed("acme", spec("docs", goodPrompt))
	h.seed("globex", spec("secrets", goodPrompt))

	if got := h.must("run-ada", "author_list", map[string]any{}).Summary; !strings.Contains(got, "docs") || strings.Contains(got, "secrets") {
		t.Fatalf("acme's listing is %q; want its own agent and nobody else's", got)
	}
	if got := h.must("run-other", "author_list", map[string]any{}).Summary; !strings.Contains(got, "secrets") || strings.Contains(got, "docs") {
		t.Fatalf("globex's listing is %q; want its own agent and nobody else's", got)
	}

	// Another tenant's agent is not found, by any read.
	for _, action := range []string{"author_get", "author_history"} {
		if _, err := h.call("run-ada", action, map[string]any{"name": "secrets"}); !errors.Is(err, registry.ErrNotFound) {
			t.Errorf("%s of another tenant's agent = %v, want ErrNotFound", action, err)
		}
	}
	if _, err := h.call("run-ada", "author_propose_delete", map[string]any{"name": "secrets", "expected_rev": 1}); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("proposing to delete another tenant's agent = %v, want ErrNotFound", err)
	}

	// Naming a tenant in the arguments is an error on every action, at the
	// top level and inside the spec, and never a silent success.
	for _, a := range h.conn.Actions() {
		args := validArgs(a.Name)
		args["tenant"] = "globex"
		if _, err := h.call("run-ada", a.Name, args); !errors.Is(err, author.ErrInvalid) {
			t.Errorf("%s with a tenant argument = %v, want ErrInvalid", a.Name, err)
		}
		args = validArgs(a.Name)
		if _, ok := args["spec"]; ok {
			args["spec"] = json.RawMessage(`{"name":"docs","model":"m","provider":"offline","system_prompt":"p","tenant":"globex"}`)
			if _, err := h.call("run-ada", a.Name, args); !errors.Is(err, author.ErrInvalid) {
				t.Errorf("%s with a tenant inside the spec = %v, want ErrInvalid", a.Name, err)
			}
		}
	}
	if n := h.store.writes.Load(); n != 0 {
		t.Fatalf("%d writes reached the store", n)
	}
}

// The schema is what the model is shown. If it never offers a tenant, a
// well-behaved model never sends one.
func TestNoActionSchemaOffersATenant(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(c *author.Config) { c.Knowledge = &corpus{} })
	for _, a := range h.conn.Actions() {
		if strings.Contains(strings.ToLower(string(a.Schema)), "tenant") {
			t.Errorf("%s has a schema that mentions a tenant", a.Name)
		}
		var schema map[string]any
		if err := json.Unmarshal(a.Schema, &schema); err != nil {
			t.Errorf("%s schema is not JSON: %v", a.Name, err)
		}
	}
}

// Default deny: with no run, an unknown run, or a run with no tenant, every
// action refuses.
func TestACallThatCannotBeScopedIsRefused(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		run  string
		want error
	}{
		{"no run bound to the call", "", author.ErrNoRun},
		{"a run the ledger does not know", "run-forged", author.ErrNoPrincipal},
		{"a run with no tenant", "run-blank", author.ErrNoPrincipal},
	}
	ledger := runs{"run-blank": {Actor: "slack:ada"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(c *author.Config) { c.Principals = ledger })
			h.seed("acme", spec("docs", goodPrompt))
			for _, a := range h.conn.Actions() {
				if _, err := h.call(tc.run, a.Name, validArgs(a.Name)); !errors.Is(err, tc.want) {
					t.Errorf("%s = %v, want %v", a.Name, err, tc.want)
				}
			}
			if n := h.store.writes.Load(); n != 0 {
				t.Fatalf("%d writes reached the store", n)
			}
		})
	}
}

func TestNewRefusesAConnectorThatCouldNotScopeACall(t *testing.T) {
	t.Parallel()
	if _, err := author.New(author.Config{Store: registry.NewMemory()}); !errors.Is(err, author.ErrInvalid) {
		t.Errorf("New with no Principals = %v, want ErrInvalid", err)
	}
	if _, err := author.New(author.Config{Principals: fixture}); !errors.Is(err, author.ErrInvalid) {
		t.Errorf("New with no Store = %v, want ErrInvalid", err)
	}
}

// Reads and proposals are separable from the write by name and by metadata,
// and the classification is true: only author_apply ever reaches a store
// method that changes anything.
func TestOnlyTheWriteActionWrites(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(c *author.Config) { c.Knowledge = &corpus{} })
	h.seed("acme", spec("docs", goodPrompt))

	writes := 0
	for _, a := range h.conn.Actions() {
		effect, ok := author.EffectOf(a.Name)
		if !ok {
			t.Fatalf("%s declares no effect", a.Name)
		}
		var want author.Effect
		switch {
		case a.Name == "author_apply":
			want = author.EffectWrite
			writes++
		case strings.HasPrefix(a.Name, "author_propose_"):
			want = author.EffectPropose
		default:
			want = author.EffectRead
		}
		if effect != want {
			t.Errorf("%s is classified %s, want %s from its name", a.Name, effect, want)
		}
		if !strings.HasPrefix(a.Description, "["+string(effect)+"] ") {
			t.Errorf("%s description %q does not lead with its effect", a.Name, a.Description)
		}
		if effect == author.EffectWrite {
			continue
		}
		if _, err := h.call("run-ada", a.Name, validArgs(a.Name)); err != nil {
			t.Errorf("%s: %v", a.Name, err)
		}
	}
	if writes != 1 {
		t.Errorf("%d write actions, want exactly one", writes)
	}
	if n := h.store.writes.Load(); n != 0 {
		t.Fatalf("read and propose actions made %d store writes", n)
	}
	if got := h.must("run-ada", "author_get", map[string]any{"name": "docs"}).Summary; !strings.Contains(got, "rev 1") {
		t.Fatalf("docs moved after proposals alone: %s", got)
	}
}

// The pause for approval is enforced by run identity. The run that proposed
// cannot apply; a later run can, and the revision records that run's actor.
func TestAChangeNeedsASecondRunAndRecordsWhoApproved(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	res := h.must("run-ada", "author_propose_create", map[string]any{"spec": spec("docs", goodPrompt)})
	if !strings.Contains(res.Summary, "NOT SAVED") || !strings.Contains(res.Summary, "review:") || !strings.Contains(res.Summary, "+ name: docs") {
		t.Fatalf("a proposal must show the change and the findings: %s", res.Summary)
	}
	tok := token(t, res)

	if _, err := h.call("run-ada", "author_apply", map[string]any{"token": tok}); !errors.Is(err, author.ErrSameRun) {
		t.Fatalf("apply in the proposing run = %v, want ErrSameRun", err)
	}
	if n := h.store.writes.Load(); n != 0 {
		t.Fatalf("the refused apply wrote %d times", n)
	}
	if got := h.must("run-grace", "author_pending", map[string]any{}).Summary; !strings.Contains(got, tok) {
		t.Fatalf("a later run cannot find the proposal: %s", got)
	}

	h.must("run-grace", "author_apply", map[string]any{"token": tok})
	rev, err := h.store.Get(context.Background(), "acme", "docs", 0)
	if err != nil {
		t.Fatalf("the applied agent is not in the registry: %v", err)
	}
	if rev.CreatedBy != "slack:grace" {
		t.Errorf("revision is by %q, want the approving run's actor", rev.CreatedBy)
	}
	if _, err := h.store.Get(context.Background(), "globex", "docs", 0); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("the agent leaked into another tenant: %v", err)
	}
}

func TestATokenIsSingleUseExpiresAndStaysInItsTenant(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	propose := func(name string) string {
		return token(t, h.must("run-ada", "author_propose_create", map[string]any{"spec": spec(name, goodPrompt)}))
	}

	leaked := propose("one")
	if _, err := h.call("run-other", "author_apply", map[string]any{"token": leaked}); !errors.Is(err, author.ErrToken) {
		t.Errorf("another tenant applying a leaked token = %v, want ErrToken", err)
	}
	if got := h.must("run-other", "author_pending", map[string]any{}).Summary; strings.Contains(got, leaked) {
		t.Errorf("another tenant can see the proposal: %s", got)
	}

	h.must("run-grace", "author_apply", map[string]any{"token": leaked})
	if _, err := h.call("run-grace", "author_apply", map[string]any{"token": leaked}); !errors.Is(err, author.ErrToken) {
		t.Errorf("reusing a token = %v, want ErrToken", err)
	}

	stale := propose("two")
	*h.now = h.now.Add(author.DefaultProposalTTL)
	if _, err := h.call("run-grace", "author_apply", map[string]any{"token": stale}); !errors.Is(err, author.ErrToken) {
		t.Errorf("applying an expired token = %v, want ErrToken", err)
	}
	if n := h.store.writes.Load(); n != 1 {
		t.Fatalf("%d store writes, want only the one approved create", n)
	}
}

// Repeating a proposal must not be a way to move it into a new run and so
// around the approval pause.
func TestRepeatingAProposalDoesNotResetWhoProposedIt(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	args := map[string]any{"spec": spec("docs", goodPrompt)}
	first := token(t, h.must("run-ada", "author_propose_create", args))
	if again := token(t, h.must("run-ada", "author_propose_create", args)); again != first {
		t.Fatalf("the same change staged twice: %s and %s", first, again)
	}
	if _, err := h.call("run-ada", "author_apply", map[string]any{"token": first}); !errors.Is(err, author.ErrSameRun) {
		t.Fatalf("apply = %v, want ErrSameRun", err)
	}
}

func TestAConcurrentEditConflictsInsteadOfBeingOverwritten(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.seed("acme", spec("docs", goodPrompt))
	mine := spec("docs", goodPrompt+" Cite the page.")
	theirs := spec("docs", goodPrompt+" Be kind.")

	tok := token(t, h.must("run-ada", "author_propose_revise", map[string]any{"spec": mine, "expected_rev": 1}))
	if _, err := h.store.Store.Revise(context.Background(), "acme", theirs, 1, "someone-else"); err != nil {
		t.Fatalf("concurrent revise: %v", err)
	}

	if _, err := h.call("run-grace", "author_apply", map[string]any{"token": tok}); !errors.Is(err, registry.ErrConflict) {
		t.Fatalf("applying over a concurrent edit = %v, want ErrConflict", err)
	}
	cur, _ := h.store.Get(context.Background(), "acme", "docs", 0)
	if cur.Rev != 2 || !strings.Contains(cur.Spec.SystemPrompt, "Be kind.") {
		t.Fatalf("the concurrent edit was overwritten: rev %d %q", cur.Rev, cur.Spec.SystemPrompt)
	}
	// And a draft made against a revision that is already stale never stages.
	if _, err := h.call("run-ada", "author_propose_revise", map[string]any{"spec": mine, "expected_rev": 1}); !errors.Is(err, registry.ErrConflict) {
		t.Errorf("proposing against a stale revision = %v, want ErrConflict", err)
	}
	if _, err := h.call("run-ada", "author_propose_revise", map[string]any{"spec": mine}); !errors.Is(err, author.ErrInvalid) {
		t.Errorf("proposing a revise with no expected revision = %v, want ErrInvalid", err)
	}
}

// interleaved lets a test land a write in the instant between apply deciding to
// retire an agent and the store being asked to.
type interleaved struct {
	registry.Store
	beforeDelete func()
}

func (s *interleaved) Delete(ctx context.Context, tenant, name string, expectedRev int) error {
	if s.beforeDelete != nil {
		s.beforeDelete()
	}
	return s.Store.Delete(ctx, tenant, name, expectedRev)
}

// An approved retirement retires the revision that was approved and no other.
// A revise that lands after the approval, however late, makes the retirement a
// conflict: the newer revision stays live because nobody was shown it, and the
// proposal is spent rather than left to be retried against it.
func TestAnApprovedRetirementCannotRetireARevisionThatLandedAfterIt(t *testing.T) {
	t.Parallel()
	var racing *interleaved
	h := newHarness(t, func(cfg *author.Config) {
		racing = &interleaved{Store: cfg.Store}
		cfg.Store = racing
	})
	h.seed("acme", spec("docs", goodPrompt))
	tok := token(t, h.must("run-ada", "author_propose_delete", map[string]any{"name": "docs", "expected_rev": 1}))

	racing.beforeDelete = func() {
		if _, err := racing.Revise(context.Background(), "acme", spec("docs", goodPrompt+" Be kind."), 1, "someone-else"); err != nil {
			t.Errorf("concurrent revise: %v", err)
		}
	}
	if _, err := h.call("run-grace", "author_apply", map[string]any{"token": tok}); !errors.Is(err, registry.ErrConflict) {
		t.Fatalf("retiring past a concurrent revise = %v, want ErrConflict", err)
	}
	cur, err := racing.Get(context.Background(), "acme", "docs", 0)
	if err != nil || cur.Rev != 2 {
		t.Fatalf("the unapproved revision was retired: rev %d, %v; want live at 2", cur.Rev, err)
	}
}

func TestARevisionShowsItsDiffAndAnUnchangedDraftProposesNothing(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.seed("acme", spec("docs", goodPrompt))

	same := h.must("run-ada", "author_propose_revise", map[string]any{"spec": spec("docs", goodPrompt), "expected_rev": 1})
	if !strings.Contains(same.Summary, "no change") {
		t.Fatalf("an identical draft was staged: %s", same.Summary)
	}

	edited := spec("docs", goodPrompt)
	edited.Tools = []string{"search"}
	res := h.must("run-ada", "author_propose_revise", map[string]any{"spec": edited, "expected_rev": 1})
	for _, want := range []string{"+ tool: search", "unused-tool"} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("proposal does not show %q:\n%s", want, res.Summary)
		}
	}
	h.must("run-grace", "author_apply", map[string]any{"token": token(t, res)})
	if got := h.must("run-ada", "author_diff", map[string]any{"name": "docs", "from": 1}).Summary; !strings.Contains(got, "+ tool: search") {
		t.Errorf("diff of rev 1 to current = %s", got)
	}
	if got := h.must("run-ada", "author_history", map[string]any{"name": "docs"}).Summary; !strings.Contains(got, "rev 2 by slack:grace") {
		t.Errorf("history = %s", got)
	}
	if got := h.must("run-ada", "author_get", map[string]any{"name": "docs", "rev": 1}).Summary; strings.Contains(got, "search") {
		t.Errorf("rev 1 changed after rev 2 was written: %s", got)
	}
}

// A revision must say who made it, and a failed attempt must not cost the
// human who will approve it their proposal.
func TestAWriteWithNoIdentifiedActorIsRefusedAndTheProposalSurvives(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	tok := token(t, h.must("run-ada", "author_propose_create", map[string]any{"spec": spec("docs", goodPrompt)}))
	if _, err := h.call("run-cron", "author_apply", map[string]any{"token": tok}); !errors.Is(err, author.ErrNoActor) {
		t.Fatalf("apply by an unidentified run = %v, want ErrNoActor", err)
	}
	if n := h.store.writes.Load(); n != 0 {
		t.Fatalf("%d writes by an unidentified run", n)
	}
	h.must("run-grace", "author_apply", map[string]any{"token": tok})
}

func TestRetiringAnAgentShowsWhatDependsOnItAndKeepsItsHistory(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.seed("acme", spec("docs", goodPrompt))
	front := spec("front", goodPrompt)
	front.Access.Spawn = []string{"docs"}
	h.seed("acme", front)

	if _, err := h.call("run-ada", "author_propose_delete", map[string]any{"name": "docs", "expected_rev": 4}); !errors.Is(err, registry.ErrConflict) {
		t.Fatalf("retiring at the wrong revision = %v, want ErrConflict", err)
	}
	res := h.must("run-ada", "author_propose_delete", map[string]any{"name": "docs", "expected_rev": 1})
	if !strings.Contains(res.Summary, `"front" delegates to "docs"`) {
		t.Fatalf("the proposal hides a dependent: %s", res.Summary)
	}
	h.must("run-grace", "author_apply", map[string]any{"token": token(t, res)})
	if _, err := h.store.Get(context.Background(), "acme", "docs", 0); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("the agent is still live: %v", err)
	}
	if got := h.must("run-ada", "author_history", map[string]any{"name": "docs"}).Summary; !strings.Contains(got, "1 revision") {
		t.Errorf("history was erased: %s", got)
	}
}

func TestValidateReportsWhatTheRegistryWouldRefuseWithoutSaving(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	bad := spec("docs", goodPrompt)
	bad.Provider = "nonesuch"
	got := h.must("run-ada", "author_validate", map[string]any{"spec": bad}).Summary
	_, want := h.store.Store.Create(context.Background(), "scratch", bad, "x")
	if want == nil || !strings.HasPrefix(got, "invalid: ") || !errors.Is(want, agentspec.ErrInvalid) {
		t.Fatalf("validate said %q, the registry said %v", got, want)
	}
	if got := h.must("run-ada", "author_validate", map[string]any{"spec": spec("docs", goodPrompt)}).Summary; !strings.HasPrefix(got, "valid") {
		t.Errorf("a valid draft reported %q", got)
	}
	if _, err := h.call("run-ada", "author_propose_create", map[string]any{"spec": bad}); !errors.Is(err, author.ErrInvalid) {
		t.Errorf("proposing an invalid draft = %v, want ErrInvalid", err)
	}
}

// corpus is a Knowledge that records the tenant it was searched under.
type corpus struct{ tenant string }

func (c *corpus) Search(_ context.Context, tenant, _ string, _ int) ([]author.Passage, error) {
	c.tenant = tenant
	return []author.Passage{{Title: "Prompt guide", Text: "State the goal first."}}, nil
}

func TestTheKnowledgeSeamIsOptionalAndScopedByTheRun(t *testing.T) {
	t.Parallel()
	bare := newHarness(t)
	for _, a := range bare.conn.Actions() {
		if a.Name == author.KnowledgeToolName {
			t.Fatal("the knowledge action exists with no Knowledge configured")
		}
	}
	k := &corpus{}
	h := newHarness(t, func(c *author.Config) { c.Knowledge = k })
	got := h.must("run-other", author.KnowledgeToolName, map[string]any{"query": "goals"}).Summary
	if k.tenant != "globex" || !strings.Contains(got, "State the goal first.") {
		t.Fatalf("searched as %q and answered %q", k.tenant, got)
	}
}

// The built-in agent has to be buildable from what Seed stores, against the
// tools this connector really contributes, with no API key.
func TestTheBuiltInAgentRunsOfflineAgainstItsOwnTools(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	if _, err := author.Seed(ctx, h.store, "acme", author.Spec("offline", agentspec.ProviderOffline)); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	reg := connector.NewRegistry()
	if err := reg.Register(h.conn); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if roles := connector.RolesOf(h.conn); !roles.Actor || roles.Trigger || roles.Sink {
		t.Fatalf("roles = %+v, want an actor and nothing else", roles)
	}
	rev, err := registry.NewSpecs(h.store).Latest(ctx, "acme", author.AgentName)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}

	var actions []payload.Action
	tools := agentrt.ConnectorToolsForRun(reg, address.Address{}, "run-ada", func(a payload.Action) error {
		actions = append(actions, a)
		return nil
	})
	for _, name := range rev.Spec.Tools {
		if _, err := tools.Lookup(name); err != nil {
			t.Errorf("the built-in agent grants %s, which does not resolve: %v", name, err)
		}
	}
	runner, err := agentrt.New(rev,
		agentrt.WithTools(tools),
		agentrt.WithScript(
			agenttest.ToolCallResponse("call_1", "author_list", map[string]any{}),
			agenttest.TextResponse("You have one agent."),
		))
	if err != nil {
		t.Fatalf("agentrt.New: %v", err)
	}
	res, err := runner.Run(ctx, agentrt.Turn{Text: "what agents do I have?"}, func(context.Context, agentrt.Chunk) error { return nil })
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "You have one agent." {
		t.Errorf("answer = %q", res.Text)
	}
	if len(actions) != 1 || actions[0].Error != "" || !strings.Contains(actions[0].Summary, author.AgentName) {
		t.Fatalf("actions = %+v, want one successful listing that includes the agent itself", actions)
	}
}
