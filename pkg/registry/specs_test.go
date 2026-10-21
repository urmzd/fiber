package registry_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/registry"
	"github.com/urmzd/fiber/pkg/runtime"
)

// The seam this file exists to fit, as a compile-time claim.
var _ runtime.Specs = registry.NewSpecs(registry.NewMemory())

// What a run needs: resolve "current" once, then resolve the number it pinned
// forever, including after the agent is revised past it and deleted.
func TestSpecsResolvesPinnedRevisionsForever(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := registry.NewMemory()
	specs := registry.NewSpecs(store)

	first := spec("docs-bot", "v1")
	first.Fallbacks = []agentspec.ModelRef{{Provider: agentspec.ProviderOllama, Model: "llama4"}}
	first.FallbackOn = []string{agentspec.FallbackOnUnavailable, agentspec.FallbackOnRateLimit}
	if _, err := store.Create(ctx, tenantA, first, "ada"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	pinned, err := specs.Latest(ctx, tenantA, "docs-bot")
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if pinned.Rev != agentspec.FirstRev {
		t.Fatalf("Latest = revision %d, want %d", pinned.Rev, agentspec.FirstRev)
	}
	if err := pinned.Validate(); err != nil {
		t.Fatalf("the runtime cannot build an agent from what it was handed: %v", err)
	}
	want := agentrt.Spec{
		Name:         "docs-bot",
		Description:  "answers questions",
		Model:        "claude-haiku-5-5",
		Provider:     agentspec.ProviderOffline,
		SystemPrompt: "v1",
		Tools:        []string{"search"},
		Sources:      []agentrt.Source{{Name: "handbook", URI: "s3://h", Options: map[string]string{"k": "v"}}},
		Access:       agentrt.Access{Spawn: []string{"linter"}, WorkspaceAreas: []string{"docs/"}},
		Fallbacks:    []agentrt.ModelRef{{Provider: agentspec.ProviderOllama, Model: "llama4"}},
		FallbackOn:   []string{agentspec.FallbackOnRateLimit, agentspec.FallbackOnUnavailable},
	}
	if !reflect.DeepEqual(pinned.Spec, want) {
		t.Fatalf("resolved spec =\n%+v\nwant\n%+v", pinned.Spec, want)
	}

	if _, err := store.Revise(ctx, tenantA, spec("docs-bot", "v2"), 1, "grace"); err != nil {
		t.Fatalf("Revise: %v", err)
	}
	if err := store.Delete(ctx, tenantA, "docs-bot", 0); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	replayed, err := specs.At(ctx, tenantA, "docs-bot", pinned.Rev)
	if err != nil {
		t.Fatalf("a run pinned to revision %d cannot replay: %v", pinned.Rev, err)
	}
	if !reflect.DeepEqual(replayed, pinned) {
		t.Fatalf("replay resolved %+v, want the pinned %+v", replayed, pinned)
	}
	if replayed.Hash != pinned.Hash {
		t.Fatalf("the pinned hash moved: %s != %s", replayed.Hash, pinned.Hash)
	}

	// Latest is a control-plane read and a retired agent has no current
	// definition, so a new tag against it does not start.
	if _, err := specs.Latest(ctx, tenantA, "docs-bot"); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("Latest on a retired agent = %v, want ErrNotFound", err)
	}
	// At never means "latest", whatever it is passed.
	if _, err := specs.At(ctx, tenantA, "docs-bot", 0); !errors.Is(err, registry.ErrInvalid) {
		t.Fatalf("At(0) = %v, want ErrInvalid; a pinned revision counts from one", err)
	}
	if _, err := specs.At(ctx, tenantB, "docs-bot", 1); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("At across a tenant boundary = %v, want ErrNotFound", err)
	}
}

// The runtime and the definition agree on what a fallback condition is, so a
// stored definition never names one the runtime refuses to build.
func TestFallbackConditionsAgreeAcrossTheSeam(t *testing.T) {
	t.Parallel()
	if got, want := agentrt.FallbackConditions(), agentspec.FallbackConditions(); !reflect.DeepEqual(got, want) {
		t.Fatalf("agentrt knows %v, agentspec %v", got, want)
	}
}
