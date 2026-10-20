package agentspec

import (
	"fmt"
	"strings"
)

// Classes an MCP group grant may end in: "mcp:<provider>:write" admits the
// provider's write tools as well as its read tools, and "mcp:<provider>:read"
// spells out the default. A grant without a class is read only, so leaving the
// class off can only ever grant less.
const (
	MCPClassRead  = "read"
	MCPClassWrite = "write"
)

// MCPGrant is a parsed tool group grant (see MCPGroupPrefix).
type MCPGrant struct {
	// Provider names the group: the MCP tool provider whose accepted tools
	// it stands for.
	Provider string
	// Write admits the provider's write tools as well as its read tools.
	// Without it the grant is read only, as a tool provider's own grant is.
	Write bool
}

// ParseMCPGrant reports whether a Tools entry is a tool group grant and, if
// it is, parses it. ok is false for an ordinary tool name. err (ErrInvalid) is
// set for an entry that starts with MCPGroupPrefix but names no group.
//
// The class is the last colon-separated segment, and only when it is "read"
// or "write"; anything else is part of the group's name, as it always was. A
// name that is not a provider this deployment knows fails when the agent is
// built, not here: which groups exist is a property of a deployment.
func ParseMCPGrant(name string) (g MCPGrant, ok bool, err error) {
	rest, ok := strings.CutPrefix(name, MCPGroupPrefix)
	if !ok {
		return MCPGrant{}, false, nil
	}
	g.Provider = rest
	if i := strings.LastIndexByte(rest, ':'); i >= 0 {
		switch rest[i+1:] {
		case MCPClassWrite:
			g = MCPGrant{Provider: rest[:i], Write: true}
		case MCPClassRead:
			g = MCPGrant{Provider: rest[:i]}
		}
	}
	if g.Provider == "" || strings.TrimSpace(g.Provider) != g.Provider {
		return MCPGrant{}, true, fmt.Errorf("%w: tool group %q names no group", ErrInvalid, name)
	}
	return g, true, nil
}

// String is the grant in its canonical form: "mcp:<provider>" for a read
// grant, "mcp:<provider>:write" for a write grant.
func (g MCPGrant) String() string {
	if g.Write {
		return MCPGroupPrefix + g.Provider + ":" + MCPClassWrite
	}
	return MCPGroupPrefix + g.Provider
}
