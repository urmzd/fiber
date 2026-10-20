// Command downstream is what a team building on fiber ships instead of a fork:
// its own binary, the same command tree and server, plus what it registers.
//
// It adds three things in Go:
//
//   - a model provider named "house", standing in for an in-house model
//     client. Here it is saige's scripted provider, so the example runs with
//     no key and no network; a real one returns the team's own
//     saige Provider.
//   - an OpenAI-compatible endpoint named "fireworks", selected by the same
//     spec field, with its key read from $FIREWORKS_API_KEY at run time.
//   - a tool, "lookup_runbook", that any agent can be granted by name.
//
// Run it like fiber:
//
//	go run ./examples/downstream serve
//
// and create an agent whose provider is "house" and whose tools include
// "lookup_runbook". Everything fiber reads from the environment and from
// `--providers FILE` still applies.
package main

import (
	"context"
	"os"

	"github.com/urmzd/saige/agent/agenttest"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/fiber/fiberapp"
	"github.com/urmzd/fiber/pkg/agentrt"
)

func main() {
	os.Exit(fiberapp.Main(fiberapp.Version{Version: "downstream-dev"},
		fiberapp.WithModelProvider("house", houseProvider),
		fiberapp.WithOpenAICompatible(agentrt.OpenAICompatible{
			Name:    "fireworks",
			BaseURL: "https://api.fireworks.ai/inference/v1",
			KeyEnv:  "FIREWORKS_API_KEY",
		}),
		fiberapp.WithTools(runbookTool{}),
	))
}

// houseProvider builds the model client for one agent revision. It receives
// the whole spec, so it can key on the model name or the agent.
func houseProvider(spec agentrt.Spec) (saigetypes.Provider, error) {
	return &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{
		agenttest.TextResponse(spec.Name + " answered on the house model (" + spec.Model + ")."),
	}}, nil
}

// runbookTool is a tool the team owns. Any saige Tool works.
type runbookTool struct{}

func (runbookTool) Definition() saigetypes.ToolDef {
	return saigetypes.ToolDef{
		Name:        "lookup_runbook",
		Description: "Look up the on-call runbook for a service.",
		Parameters: saigetypes.ParameterSchema{
			Type:       "object",
			Properties: map[string]saigetypes.PropertyDef{"service": {Type: "string"}},
			Required:   []string{"service"},
		},
	}
}

func (runbookTool) Execute(_ context.Context, args map[string]any) (string, error) {
	service, _ := args["service"].(string)
	return "Restart " + service + " with `make restart`, then page the owner.", nil
}
