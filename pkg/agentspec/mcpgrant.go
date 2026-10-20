package agentspec

import (
	"fmt"
	"regexp"
	"strings"
)

// MCPGrantPrefix marks a tool grant that stands for a whole MCP tool provider
// rather than one tool: "mcp:<provider>" grants the provider's accepted read
// tools, and "mcp:<provider>:write" its accepted read and write tools. The
// expanded tools are named "<provider>_<tool>".
const MCPGrantPrefix = "mcp:"

// mcpProviderName mirrors pkg/toolprovider's provider-name rule. It is
// repeated rather than imported because this package is a leaf; a grant that
// passed here and failed there would name a provider no one can register.
var mcpProviderName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

// MCPGrant is a parsed "mcp:" tool grant.
type MCPGrant struct {
	// Provider is the tool provider's name.
	Provider string
	// Write admits the provider's write tools as well as its read tools.
	// Without it the grant is read only: the default fails closed, as a
	// tool provider's own grant does.
	Write bool
}

// ParseMCPGrant reports whether a tool grant is an MCP provider grant and, if
// it is, parses it. ok is false for an ordinary tool name; err is set for a
// name that starts with MCPGrantPrefix but is not a well-formed grant.
func ParseMCPGrant(name string) (g MCPGrant, ok bool, err error) {
	rest, ok := strings.CutPrefix(name, MCPGrantPrefix)
	if !ok {
		return MCPGrant{}, false, nil
	}
	provider, class, hasClass := strings.Cut(rest, ":")
	if !mcpProviderName.MatchString(provider) {
		return MCPGrant{}, true, fmt.Errorf("%w: tool grant %q: provider name %q must match %s", ErrInvalid, name, provider, mcpProviderName)
	}
	g = MCPGrant{Provider: provider}
	switch {
	case !hasClass, class == "read":
	case class == "write":
		g.Write = true
	default:
		return MCPGrant{}, true, fmt.Errorf("%w: tool grant %q: class %q, want read or write", ErrInvalid, name, class)
	}
	return g, true, nil
}

// Covers reports whether a tool name is one this grant can expand to.
func (g MCPGrant) Covers(tool string) bool {
	return strings.HasPrefix(tool, g.Provider+"_")
}

// mcpGrants parses every MCP grant in tools, refusing malformed ones and two
// grants of one provider, which would leave its class ambiguous.
func mcpGrants(agent string, tools []string) ([]MCPGrant, error) {
	var out []MCPGrant
	seen := map[string]bool{}
	for _, name := range tools {
		g, ok, err := ParseMCPGrant(name)
		if !ok {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("agent %q: %w", agent, err)
		}
		if seen[g.Provider] {
			return nil, fmt.Errorf("%w: agent %q grants MCP provider %q twice; grant it once, with the class it needs", ErrInvalid, agent, g.Provider)
		}
		seen[g.Provider] = true
		out = append(out, g)
	}
	return out, nil
}
