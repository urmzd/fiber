// Command downstream is what a team building on fiber ships instead of a fork:
// its own program, composing fiber in Go, plus what it registers.
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
// It creates an agent on the house provider, raises one tag in process and
// prints the answer:
//
//	go run ./examples/downstream
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/urmzd/fiber"
	"github.com/urmzd/fiber/pkg/agentrt"
	"github.com/urmzd/fiber/pkg/agentspec"
	"github.com/urmzd/fiber/pkg/bus"
	"github.com/urmzd/fiber/pkg/envelope"
	"github.com/urmzd/fiber/pkg/event"
	"github.com/urmzd/fiber/pkg/registry"
	"github.com/urmzd/fiber/pkg/runtime"
	"github.com/urmzd/fiber/pkg/turn/native/agenttest"
	saigetypes "github.com/urmzd/fiber/pkg/turn/native/saigetypes"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "downstream:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Everything is in memory unless an option says otherwise: a program
	// on Postgres adds fiber.WithPostgres(pool) and fiber.WithBus(...).
	const tenant = "acme"
	app, err := fiber.New(ctx,
		fiber.WithTenant(tenant),
		fiber.WithModelProvider("house", houseProvider),
		fiber.WithOpenAICompatible(agentrt.OpenAICompatible{
			Name:    "fireworks",
			BaseURL: "https://api.fireworks.ai/inference/v1",
			KeyEnv:  "FIREWORKS_API_KEY",
		}),
		fiber.WithTools(runbookTool{}),
	)
	if err != nil {
		return err
	}
	defer func() { _ = app.Close() }()

	// Delivery and execution, in this process, until ctx ends.
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()

	if _, err := app.Registry().Create(ctx, tenant, agentspec.AgentSpec{
		Name: "oncall", Provider: "house", Model: "house-1",
		SystemPrompt: "Help the on-call engineer.", Tools: []string{"lookup_runbook"},
	}, "downstream"); err != nil && !errors.Is(err, registry.ErrExists) {
		return err
	}

	// Watch the channel the tag is raised on, then raise it the way any
	// trigger would.
	tag := envelope.Tag{
		ID: fmt.Sprintf("downstream-%d", time.Now().UnixNano()), Tenant: tenant,
		Agent: "oncall", Origin: "downstream", Text: "api is down", At: time.Now(),
	}
	stream, err := app.Bus().Subscribe(ctx, bus.Subscription{
		Tenant:   runtime.TenantOf(tag),
		Channels: []string{runtime.ChannelOf(tag)},
		Types:    []string{"run"},
	})
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()
	acc, err := app.Invoke(ctx, tag)
	if err != nil {
		return err
	}
	for {
		e, err := stream.Recv(ctx)
		if err != nil {
			return err
		}
		if e.Run != acc.RunID {
			continue
		}
		switch b := e.Data.(type) {
		case event.Text:
			fmt.Print(b.Text)
		case event.Completed, event.Failed, event.Stopped:
			fmt.Printf("\nrun %s: %s\n", acc.RunID, e.Type)
			cancel()
			return <-done
		}
	}
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
