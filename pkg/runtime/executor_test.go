package runtime_test

import (
	"context"
	"testing"

	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/turn"
	"github.com/urmzd/fiber/pkg/turn/native"
	"github.com/urmzd/fiber/pkg/turn/native/agenttest"
)

func bridgeRequest() runtime.Request {
	return runtime.Request{
		RunID:  "r1",
		Tenant: "t1",
		Text:   "update the docs",
		Meta:   map[string]string{"repo": "fiber"},
		Definition: agentrt.Revision{Name: "docs-bot", Version: "2.0.0", Digest: "h", Spec: agentrt.Spec{
			Name: "docs-bot", Provider: agentrt.ProviderOffline, Model: "scripted",
		}},
	}
}

// A turn.Runtime runs as an Executor: the request text it renders is the
// one an Executor was always sent, and its bodies arrive as chunks.
func TestRuntimeExecutorRunsATurnRuntime(t *testing.T) {
	t.Parallel()
	x := runtime.RuntimeExecutor(&native.Runtime{Options: []native.Option{native.WithScript(agenttest.TextResponse("done"))}})
	var kinds []envelope.Kind
	out, err := x.Execute(t.Context(), bridgeRequest(), func(_ context.Context, c agentrt.Chunk) error {
		kinds = append(kinds, c.Kind)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "done" {
		t.Fatalf("Text = %q", out.Text)
	}
	if want := agentrt.RequestText("update the docs", map[string]string{"repo": "fiber"}); out.Prompt != want {
		t.Fatalf("Prompt = %q, want %q", out.Prompt, want)
	}
	if out.Chunks != len(kinds) || len(kinds) == 0 {
		t.Fatalf("Chunks = %d, emitted %v", out.Chunks, kinds)
	}
	var text bool
	for _, k := range kinds {
		text = text || k == envelope.KindText
	}
	if !text {
		t.Fatalf("no text chunk in %v", kinds)
	}
}

// The runtime's stop signal reaches a turn.Runtime on its context.
func TestRuntimeExecutorCarriesTheStopSignal(t *testing.T) {
	t.Parallel()
	var seen bool
	x := runtime.RuntimeExecutor(turn.Func(func(ctx context.Context, _ turn.Pinned, _ turn.Input, _ func(context.Context, event.Body) error) (turn.Result, error) {
		s := <-runtime.StopSignal(ctx)
		seen = true
		return turn.Result{Stopped: &s}, nil
	}))
	stop := make(chan turn.Stop, 1)
	stop <- turn.Stop{Reason: "enough"}
	out, err := x.Execute(turn.WithStopSignal(t.Context(), stop), bridgeRequest(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !seen || out.Stopped == nil || out.Stopped.Reason != "enough" {
		t.Fatalf("Stopped = %+v", out.Stopped)
	}
}
