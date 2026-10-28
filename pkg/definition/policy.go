package definition

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// PolicyDomain is the hash domain of a Policy. Changing the encoding is a
// new domain, never a redefinition of this one.
const PolicyDomain = "fiber/policy/v1"

// Conditions FallbackOn may name.
const (
	FallbackOnAny           = "any"
	FallbackOnUnavailable   = "unavailable"
	FallbackOnRateLimit     = "rate_limit"
	FallbackOnTransient     = "transient"
	FallbackOnAuth          = "auth"
	FallbackOnContextLength = "context_length"
	FallbackOnContentFilter = "content_filter"
)

// FallbackConditions lists the conditions FallbackOn may name, sorted.
func FallbackConditions() []string {
	return []string{
		FallbackOnAny, FallbackOnAuth, FallbackOnContentFilter, FallbackOnContextLength,
		FallbackOnRateLimit, FallbackOnTransient, FallbackOnUnavailable,
	}
}

// Policy is what fiber decides about an agent that saige does not. Every
// field is an allowlist or a narrowing: the zero Policy grants nothing.
type Policy struct {
	// Subscriptions are the channels and type prefixes the agent listens
	// on. None means it is woken by nothing.
	Subscriptions []Subscription `json:"subscriptions,omitempty"`
	// WorkspaceAreas are the shared-workspace key prefixes it may use.
	WorkspaceAreas []string `json:"workspace_areas,omitempty"`
	// MCPWrite names the MCP servers (tools.mcp) whose write tools it may
	// call. Every other server it is granted is read only.
	MCPWrite []string `json:"mcp_write,omitempty"`
	// Knowledge are the corpora it may retrieve from.
	Knowledge []Corpus `json:"knowledge,omitempty"`
	// FallbackOn is when a call moves on to the next fallback model. Empty
	// means any error another model could fix.
	FallbackOn []string `json:"fallback_on,omitempty"`
	// Approvers may approve its gated calls. Empty admits anyone but the
	// requester. "role:<name>" admits a role.
	Approvers []string `json:"approvers,omitempty"`
	// SelfApproval lets the person who asked approve their own request.
	SelfApproval bool `json:"self_approval,omitempty"`
	// Answerers may answer its questions. Empty admits anyone.
	Answerers []string `json:"answerers,omitempty"`
	// TaggedOnly keeps it out of messages that do not name it.
	TaggedOnly bool `json:"tagged_only,omitempty"`
}

// Subscription is one channel prefix and the type prefixes taken from it.
type Subscription struct {
	Channel string   `json:"channel"`
	Types   []string `json:"types"`
}

// Corpus is one retrieval corpus.
type Corpus struct {
	Name    string            `json:"name"`
	URI     string            `json:"uri"`
	Options map[string]string `json:"options,omitempty"`
}

// Normalize returns p with every set trimmed and sorted, so two policies that
// differ only in order hash the same.
func (p Policy) Normalize() Policy {
	out := Policy{
		WorkspaceAreas: set(p.WorkspaceAreas, false),
		MCPWrite:       set(p.MCPWrite, false),
		FallbackOn:     set(p.FallbackOn, true),
		Approvers:      set(p.Approvers, false),
		SelfApproval:   p.SelfApproval,
		Answerers:      set(p.Answerers, false),
		TaggedOnly:     p.TaggedOnly,
	}
	for _, s := range p.Subscriptions {
		out.Subscriptions = append(out.Subscriptions, Subscription{
			Channel: strings.TrimSpace(s.Channel), Types: set(s.Types, false),
		})
	}
	slices.SortStableFunc(out.Subscriptions, func(a, b Subscription) int { return strings.Compare(a.Channel, b.Channel) })
	for _, c := range p.Knowledge {
		c := Corpus{Name: NormalizeName(c.Name), URI: strings.TrimSpace(c.URI), Options: maps.Clone(c.Options)}
		if len(c.Options) == 0 {
			c.Options = nil
		}
		out.Knowledge = append(out.Knowledge, c)
	}
	slices.SortStableFunc(out.Knowledge, func(a, b Corpus) int {
		if c := strings.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		return strings.Compare(a.URI, b.URI)
	})
	return out
}

// Hash is the content hash of the normalized policy under PolicyDomain, as
// "sha256:<hex>".
func (p Policy) Hash() string {
	raw, err := json.Marshal(p.Normalize())
	if err != nil {
		// A Policy is strings, bools and maps of strings: it always encodes.
		panic(fmt.Sprintf("definition: encode policy: %v", err))
	}
	h := sha256.New()
	h.Write([]byte(PolicyDomain))
	h.Write([]byte{0})
	h.Write(raw)
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// Validate checks what a policy can prove on its own.
func (p Policy) Validate() error {
	n := p.Normalize()
	for what, list := range map[string][]string{
		"workspace area": n.WorkspaceAreas, "mcp write grant": n.MCPWrite, "fallback condition": n.FallbackOn,
		"approver": n.Approvers, "answerer": n.Answerers,
	} {
		if err := uniqueNonEmpty(what, list); err != nil {
			return err
		}
	}
	for _, cond := range n.FallbackOn {
		if !slices.Contains(FallbackConditions(), cond) {
			return fmt.Errorf("%w: policy falls back on %q; the conditions are %s", ErrInvalid, cond, strings.Join(FallbackConditions(), ", "))
		}
		if cond == FallbackOnAny && len(n.FallbackOn) > 1 {
			return fmt.Errorf("%w: policy falls back on %q together with other conditions; %q already covers them", ErrInvalid, FallbackOnAny, FallbackOnAny)
		}
	}
	for i, s := range n.Subscriptions {
		switch {
		case s.Channel == "":
			return fmt.Errorf("%w: subscription %d names no channel", ErrInvalid, i)
		case len(s.Types) == 0:
			return fmt.Errorf("%w: subscription to %q takes no type; name the type prefixes it takes", ErrInvalid, s.Channel)
		case i > 0 && n.Subscriptions[i-1].Channel == s.Channel:
			return fmt.Errorf("%w: channel %q is subscribed twice", ErrInvalid, s.Channel)
		}
		if err := uniqueNonEmpty("subscribed type", s.Types); err != nil {
			return err
		}
	}
	for i, c := range n.Knowledge {
		switch {
		case c.Name == "":
			return fmt.Errorf("%w: a knowledge corpus has no name", ErrInvalid)
		case c.URI == "":
			return fmt.Errorf("%w: knowledge corpus %q has no uri", ErrInvalid, c.Name)
		case i > 0 && n.Knowledge[i-1].Name == c.Name:
			return fmt.Errorf("%w: knowledge corpus %q is declared twice", ErrInvalid, c.Name)
		}
	}
	return nil
}

// MayWrite reports whether the policy grants the write tools of an MCP
// server.
func (p Policy) MayWrite(server string) bool { return slices.Contains(p.MCPWrite, server) }

// MayUseArea reports whether key falls in a granted workspace area.
func (p Policy) MayUseArea(key string) bool {
	for _, area := range p.WorkspaceAreas {
		if area != "" && strings.HasPrefix(key, area) {
			return true
		}
	}
	return false
}

func uniqueNonEmpty(what string, sorted []string) error {
	for i, v := range sorted {
		if v == "" {
			return fmt.Errorf("%w: an empty %s", ErrInvalid, what)
		}
		if i > 0 && v == sorted[i-1] {
			return fmt.Errorf("%w: %s %q is listed twice", ErrInvalid, what, v)
		}
	}
	return nil
}

func set(values []string, lower bool) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if lower {
			v = strings.ToLower(v)
		}
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}
