package author_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/urmzd/fiber/pkg/connectors/author"
	"github.com/urmzd/fiber/pkg/definition"
)

func TestSeedingAtEveryStartupWritesNoNewVersions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := definition.NewMemory()
	own := author.Spec("offline", definition.ProviderOffline)

	first, err := author.Seed(ctx, store, "acme", own)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if first.Version != "1.0.0" || first.CreatedBy != author.SeededBy {
		t.Fatalf("first seed = %s by %q", first.Version, first.CreatedBy)
	}
	a, err := definition.Parse(first.Content, first.Policy, definition.Checks{})
	if err != nil {
		t.Fatal(err)
	}
	if want := slices.Sorted(slices.Values(author.ToolNames())); !slices.Equal(a.Spec.Tools, want) {
		t.Errorf("the seeded agent's tools are %v, want every core action", a.Spec.Tools)
	}
	if len(a.Spec.Access.Spawn) != 0 || a.Policy.Hash() != (definition.Policy{}).Hash() {
		t.Errorf("the seeded agent was granted %+v and policy %+v, want nothing", a.Spec.Access, a.Policy)
	}
	for range 3 {
		again, err := author.Seed(ctx, store, "acme", own)
		if err != nil {
			t.Fatalf("Seed again: %v", err)
		}
		if again.Version != first.Version || again.Digest != first.Digest {
			t.Fatalf("re-seeding moved the agent to %s", again.Version)
		}
	}
	if _, err := store.Latest(ctx, "globex", author.AgentName); !errors.Is(err, definition.ErrNotFound) {
		t.Errorf("seeding one tenant created the agent in another: %v", err)
	}
}

func TestSeedUpgradesItsOwnVersionsAndLeavesAPersonsAlone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := definition.NewMemory()
	old := author.Spec("offline", definition.ProviderOffline)
	if _, err := author.Seed(ctx, store, "acme", old); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	// A new binary ships a new definition: the seed writes its next major.
	next := old
	next.Model = "newer-model"
	upgraded, err := author.Seed(ctx, store, "acme", next)
	if err != nil || upgraded.Version != "2.0.0" || !strings.Contains(upgraded.Content, "offline/newer-model") {
		t.Fatalf("upgrade = %s err %v\n%s", upgraded.Version, err, upgraded.Content)
	}

	// A person edits it: from then on the seed keeps its hands off.
	edited := next
	edited.SystemPrompt += "\nAlways answer in French."
	rec, err := definition.FromAgentSpec(edited, 3)
	if err != nil {
		t.Fatal(err)
	}
	rec.Tenant, rec.CreatedBy = "acme", "slack:ada"
	if _, err := store.Put(ctx, rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	kept, err := author.Seed(ctx, store, "acme", old)
	if err != nil || kept.Version != "3.0.0" || kept.CreatedBy != "slack:ada" {
		t.Fatalf("seed over a person's edit = %s by %q err %v", kept.Version, kept.CreatedBy, err)
	}
}

func TestSeedDoesNotResurrectAnAgentSomeoneRetired(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := definition.NewMemory()
	own := author.Spec("offline", definition.ProviderOffline)
	if _, err := author.Seed(ctx, store, "acme", own); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if err := store.Retire(ctx, "acme", author.AgentName, "slack:ada"); err != nil {
		t.Fatalf("Retire: %v", err)
	}
	rec, err := author.Seed(ctx, store, "acme", own)
	if err != nil || rec.Version != "" {
		t.Fatalf("seed after retirement = %q err %v, want nothing and no error", rec.Version, err)
	}
	if _, err := store.Latest(ctx, "acme", author.AgentName); !errors.Is(err, definition.ErrRetired) {
		t.Fatalf("the retired agent came back: %v", err)
	}
}

func TestSeedRefusesWhatTheSourceRefuses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if _, err := author.Seed(ctx, definition.NewMemory(), "", author.Spec("offline", definition.ProviderOffline)); !errors.Is(err, definition.ErrInvalid) {
		t.Errorf("Seed with no tenant = %v, want the source's ErrInvalid", err)
	}
	if _, err := author.Seed(ctx, definition.NewMemory(), "acme", author.Spec("", definition.ProviderOffline)); !errors.Is(err, definition.ErrInvalid) {
		t.Errorf("Seed with no model = %v, want the source's ErrInvalid", err)
	}
	if _, err := author.Seed(ctx, nil, "acme", author.Spec("m", definition.ProviderOffline)); !errors.Is(err, author.ErrInvalid) {
		t.Errorf("Seed with no source = %v, want ErrInvalid", err)
	}
}

func TestDiffShowsOnlyWhatChanged(t *testing.T) {
	t.Parallel()
	from := author.Draft{Definition: "---\nname: docs\nversion: 1.0.0\n---\nLine one.\nLine two.\nLine three.\nLine four.\nLine five.\nLine six.\n"}
	if d := author.Diff(from, from); d != "" {
		t.Errorf("an unchanged draft produced a diff:\n%s", d)
	}
	to := author.Draft{
		Definition: "---\nname: docs\nversion: 1.1.0\n---\nLine one.\nLine two.\nLine three.\nLine four.\nLine five.\nLine 6.\n",
		Policy:     definition.Policy{TaggedOnly: true},
	}
	const want = `  ---
  name: docs
- version: 1.0.0
+ version: 1.1.0
  ---
  Line one.
  ...
  Line four.
  Line five.
- Line six.
+ Line 6.
+ # policy
+ {
+   "tagged_only": true
+ }`
	if got := author.Diff(from, to); got != want {
		t.Errorf("diff =\n%s\nwant\n%s", got, want)
	}
}
