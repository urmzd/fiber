package author

import (
	"fmt"
	"strings"

	"github.com/urmzd/mandatum/pkg/agentspec"
)

// Role is one agent in a scaffolded system: what it is called and what it is
// responsible for.
type Role struct {
	// Name is the agent name, such as "triage".
	Name string `json:"name"`
	// Role is one or two sentences on what the agent handles, such as
	// "Answers how-to questions from the product documentation."
	Role string `json:"role"`
	// Tools are the tool names to grant it. Optional.
	Tools []string `json:"tools,omitempty"`
}

// System describes a router and the specialists it hands work to.
type System struct {
	// Router receives every request and delegates it.
	Router Role `json:"router"`
	// Specialists do the work. At least one is required.
	Specialists []Role `json:"specialists"`
	// Model and Provider are applied to every agent in the system.
	Model    string `json:"model"`
	Provider string `json:"provider"`
}

// Scaffold lays a System out as definitions, validated against the built-in
// provider table.
func Scaffold(sys System) ([]agentspec.AgentSpec, error) {
	return ScaffoldWith(sys, agentspec.KnownProviders())
}

// ScaffoldWith is Scaffold for a deployment with its own provider table.
//
// The result is the specialists in the order given, then the router, which is
// the order to create them in: the router's spawn grants then name agents that
// already exist.
//
// The delegation graph is a star and nothing more. The router's Access.Spawn
// is exactly the specialists, and every specialist's Access is empty, so a
// specialist cannot delegate onward, cannot call the router back, and holds no
// workspace area. Access is default deny, and a scaffold that granted more
// than its description asked for would be widening authority by template.
//
// Every definition returned has passed the same canonicalization and
// validation the registry applies on write, so a scaffold that returns is a
// scaffold that can be stored.
func ScaffoldWith(sys System, providers []string) ([]agentspec.AgentSpec, error) {
	if len(sys.Specialists) == 0 {
		return nil, fmt.Errorf("%w: a system needs at least one specialist for the router to delegate to", ErrInvalid)
	}
	router := agentspec.NormalizeName(sys.Router.Name)
	seen := map[string]bool{router: true}
	names := make([]string, 0, len(sys.Specialists))
	for _, s := range sys.Specialists {
		name := agentspec.NormalizeName(s.Name)
		if seen[name] {
			return nil, fmt.Errorf("%w: the name %q is used by more than one agent in the system", ErrInvalid, name)
		}
		seen[name] = true
		names = append(names, name)
	}
	for _, r := range append([]Role{sys.Router}, sys.Specialists...) {
		if strings.TrimSpace(r.Role) == "" {
			return nil, fmt.Errorf("%w: agent %q has no role; the role is what its prompt is written from", ErrInvalid, agentspec.NormalizeName(r.Name))
		}
	}

	out := make([]agentspec.AgentSpec, 0, len(sys.Specialists)+1)
	for i, s := range sys.Specialists {
		out = append(out, agentspec.AgentSpec{
			Name:         names[i],
			Description:  oneLine(s.Role),
			Model:        sys.Model,
			Provider:     sys.Provider,
			SystemPrompt: specialistPrompt(names[i], s),
			Tools:        s.Tools,
		})
	}
	out = append(out, agentspec.AgentSpec{
		Name:         router,
		Description:  oneLine(sys.Router.Role),
		Model:        sys.Model,
		Provider:     sys.Provider,
		SystemPrompt: routerPrompt(router, sys.Router, names, sys.Specialists),
		Tools:        sys.Router.Tools,
		Access:       agentspec.Access{Spawn: names},
	})

	for i, spec := range out {
		canonical := spec.Normalize()
		if err := canonical.ValidateWith(providers); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
		}
		out[i] = canonical
	}
	return out, nil
}

func specialistPrompt(name string, r Role) string {
	var b strings.Builder
	fmt.Fprintf(&b, "You are %s, a specialist agent. Your role: %s\n\n", name, sentence(r.Role))
	b.WriteString("Your goal is to handle requests inside that role completely and accurately. " +
		"If a request falls outside your role, say so plainly instead of guessing.\n")
	if tools := cleanList(r.Tools); len(tools) > 0 {
		fmt.Fprintf(&b, "\nTools you may use when they help: %s.\n", strings.Join(tools, ", "))
	}
	b.WriteString("\nOutput format: answer in plain text, leading with the answer and keeping it concise.")
	return b.String()
}

func routerPrompt(name string, r Role, names []string, specialists []Role) string {
	var b strings.Builder
	fmt.Fprintf(&b, "You are %s, the router for a system of specialist agents. %s\n\n", name, sentence(r.Role))
	b.WriteString("Your goal is to send each request to the one specialist best placed to handle it, and to relay that specialist's answer.\n\n")
	b.WriteString("Specialists:\n")
	for i, s := range specialists {
		fmt.Fprintf(&b, "- %s: %s Delegate with the %s%s tool.\n", names[i], sentence(s.Role), delegatePrefix, names[i])
	}
	b.WriteString("\nRules:\n" +
		"- Delegate each request to exactly one specialist, and pass the request on in full.\n" +
		"- If no specialist fits, say so and ask one clarifying question instead of answering yourself.\n")
	if tools := cleanList(r.Tools); len(tools) > 0 {
		fmt.Fprintf(&b, "- Tools you may use when they help: %s.\n", strings.Join(tools, ", "))
	}
	b.WriteString("\nOutput format: reply with one line naming the specialist that handled the request, then that specialist's answer unchanged.")
	return b.String()
}

// sentence trims a role and makes sure it ends like a sentence, so it reads
// correctly wherever a prompt template places it.
func sentence(s string) string {
	s = oneLine(s)
	if s != "" && !strings.ContainsAny(s[len(s)-1:], ".!?") {
		s += "."
	}
	return s
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func cleanList(values []string) []string {
	var out []string
	for _, v := range values {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}
