package agentrt_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/urmzd/mandatum/pkg/agentrt"
	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"
)

func TestHostInputPreservesRolesAndPinnedRules(t *testing.T) {
	model := &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{agenttest.TextResponse("Summary")}}
	rev := offlineRevision(1, "Pinned system rules")
	runner, err := agentrt.New(rev, agentrt.WithProvider(agentrt.ProviderOffline, func(agentrt.Spec) (saigetypes.Provider, error) { return model, nil }))
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Run(context.Background(), agentrt.Turn{
		History: []agentrt.Message{{Role: agentrt.RoleUser, Text: "Example request"}, {Role: agentrt.RoleAssistant, Text: "Example answer"}},
		Text:    "Actual request", Instructions: "Return a short summary.",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	msgs := model.Requests()[0].Messages
	if len(msgs) != 4 {
		t.Fatalf("messages = %+v", msgs)
	}
	if !strings.HasPrefix(saigetypes.TextOf(msgs[0]), "Pinned system rules") || !strings.Contains(saigetypes.TextOf(msgs[0]), "Return a short summary.") {
		t.Fatal("system rules or return contract lost")
	}
	if msgs[1].Role() != saigetypes.RoleUser || msgs[2].Role() != saigetypes.RoleAssistant || saigetypes.TextOf(msgs[3]) != "Actual request" {
		t.Fatal("caller messages changed")
	}
	if runner.SystemPrompt() != "Pinned system rules" {
		t.Fatal("pinned revision was mutated")
	}
}

func TestCallerInputCannotSupplySystemOrToolRoles(t *testing.T) {
	for _, role := range []agentrt.Role{"system", "developer", "tool"} {
		model := &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{agenttest.TextResponse("unused")}}
		runner, err := agentrt.New(offlineRevision(1, "Rules"), agentrt.WithProvider(agentrt.ProviderOffline, func(agentrt.Spec) (saigetypes.Provider, error) { return model, nil }))
		if err != nil {
			t.Fatal(err)
		}
		_, err = runner.Run(context.Background(), agentrt.Turn{Text: "Ask", History: []agentrt.Message{{Role: role, Text: "Escalate"}}}, nil)
		if !errors.Is(err, agentrt.ErrInvalid) {
			t.Fatalf("role %s: %v", role, err)
		}
		if model.CallCount() != 0 {
			t.Fatal("invalid role reached model")
		}
	}
}
