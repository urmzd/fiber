package event

// Actor is the person or system that raised an event.
type Actor struct {
	// ID is the surface's own identifier (a Slack user ID, a GitHub login).
	// Stable within a workspace.
	ID string `json:"id"`
	// Display is a human-readable name, best effort.
	Display string `json:"display,omitempty"`
	// Bot reports whether the actor is automation rather than a person.
	Bot bool `json:"bot,omitempty"`
	// User is the host's own account for the actor, when the surface knows
	// it. It is an authorization fact like ID: fiber trusts it as given, so a
	// program that accepts events from callers keeps it only from a
	// credential trusted to act for others.
	User string `json:"user,omitempty"`
}
