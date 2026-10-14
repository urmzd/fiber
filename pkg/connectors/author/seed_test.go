package author_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/urmzd/mandatum/pkg/agentspec"
	"github.com/urmzd/mandatum/pkg/connectors/author"
	"github.com/urmzd/mandatum/pkg/registry"
)

func TestSeedingAtEveryStartupBurnsNoRevisions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := registry.NewMemory()
	own := author.Spec("offline", agentspec.ProviderOffline)

	first, err := author.Seed(ctx, store, "acme", own)
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if first.Rev != agentspec.FirstRev || first.CreatedBy != author.SeededBy {
		t.Fatalf("first seed = rev %d by %q", first.Rev, first.CreatedBy)
	}
	if !reflect.DeepEqual(first.Spec.Tools, agentspec.AgentSpec{Tools: author.ToolNames()}.Normalize().Tools) {
		t.Errorf("the seeded agent's tools are %v, want every core action", first.Spec.Tools)
	}
	if !reflect.DeepEqual(first.Spec.Access, agentspec.Access{}) {
		t.Errorf("the seeded agent was granted access %+v, want none", first.Spec.Access)
	}
	for range 3 {
		again, err := author.Seed(ctx, store, "acme", own)
		if err != nil {
			t.Fatalf("Seed again: %v", err)
		}
		if again.Rev != first.Rev || again.Hash != first.Hash {
			t.Fatalf("re-seeding moved the agent to rev %d", again.Rev)
		}
	}
	if _, err := store.Get(ctx, "globex", author.AgentName, 0); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("seeding one tenant created the agent in another: %v", err)
	}
}

func TestSeedUpgradesItsOwnRevisionsAndLeavesAPersonsAlone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := registry.NewMemory()
	old := author.Spec("offline", agentspec.ProviderOffline)
	if _, err := author.Seed(ctx, store, "acme", old); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	// A new binary ships a new definition: the seed's own revision moves.
	next := old
	next.Model = "newer-model"
	upgraded, err := author.Seed(ctx, store, "acme", next)
	if err != nil || upgraded.Rev != 2 || upgraded.Spec.Model != "newer-model" {
		t.Fatalf("upgrade = rev %d model %q err %v", upgraded.Rev, upgraded.Spec.Model, err)
	}

	// A person edits it: from then on the seed keeps its hands off.
	edited := next
	edited.SystemPrompt += "\nAlways answer in French."
	if _, err := store.Revise(ctx, "acme", edited, 2, "slack:ada"); err != nil {
		t.Fatalf("Revise: %v", err)
	}
	kept, err := author.Seed(ctx, store, "acme", old)
	if err != nil || kept.Rev != 3 || kept.CreatedBy != "slack:ada" {
		t.Fatalf("seed over a person's edit = rev %d by %q err %v", kept.Rev, kept.CreatedBy, err)
	}
}

func TestSeedDoesNotResurrectAnAgentSomeoneRetired(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := registry.NewMemory()
	own := author.Spec("offline", agentspec.ProviderOffline)
	if _, err := author.Seed(ctx, store, "acme", own); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if err := store.Delete(ctx, "acme", author.AgentName, 0); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	rev, err := author.Seed(ctx, store, "acme", own)
	if err != nil || rev.Rev != 0 {
		t.Fatalf("seed after retirement = rev %d err %v, want nothing and no error", rev.Rev, err)
	}
	if _, err := store.Get(ctx, "acme", author.AgentName, 0); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("the retired agent came back: %v", err)
	}
}

func TestSeedRefusesWhatTheRegistryRefuses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if _, err := author.Seed(ctx, registry.NewMemory(), "", author.Spec("offline", agentspec.ProviderOffline)); !errors.Is(err, registry.ErrInvalid) {
		t.Errorf("Seed with no tenant = %v, want the registry's ErrInvalid", err)
	}
	if _, err := author.Seed(ctx, registry.NewMemory(), "acme", author.Spec("", agentspec.ProviderOffline)); !errors.Is(err, registry.ErrInvalid) {
		t.Errorf("Seed with no model = %v, want the registry's ErrInvalid", err)
	}
	if _, err := author.Seed(ctx, nil, "acme", author.Spec("m", agentspec.ProviderOffline)); !errors.Is(err, author.ErrInvalid) {
		t.Errorf("Seed with no store = %v, want ErrInvalid", err)
	}
}

func TestDiffShowsOnlyWhatChanged(t *testing.T) {
	t.Parallel()
	from := spec("docs", "You are a docs agent.\nLine two.\nLine three.\nLine four.\nLine five.\nLine six.")
	from.Tools = []string{"search", "fetch"}

	reordered := from
	reordered.Tools = []string{"fetch", "search"}
	reordered.Name = " Docs "
	if d := author.Diff(from, reordered); d != "" {
		t.Errorf("a change of presentation produced a diff:\n%s", d)
	}

	to := from
	to.Tools = []string{"search"}
	to.SystemPrompt = "You are a docs agent.\nLine two.\nLine three.\nLine four.\nLine five.\nLine 6."
	const want = `  model: offline
  provider: offline
- tool: fetch
  tool: search
  system_prompt:
  ...
    Line four.
    Line five.
-   Line six.
+   Line 6.`
	if got := author.Diff(from, to); got != want {
		t.Errorf("diff =\n%s\nwant\n%s", got, want)
	}
}
