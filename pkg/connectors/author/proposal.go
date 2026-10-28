package author

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/urmzd/fiber/pkg/definition"
)

// Operations a proposal can stage.
const (
	opCreate = "create"
	opRevise = "revise"
	opDelete = "delete"
)

// proposal is one staged change, waiting for a different run to apply it.
//
// It records everything apply needs, so apply takes a token and nothing else:
// the model cannot change what was approved between showing the diff and
// writing it.
type proposal struct {
	Token string `json:"token"`
	Op    string `json:"op"`
	Agent string `json:"agent"`
	// Version is the version a create or revise writes.
	Version string `json:"version,omitempty"`
	// ExpectedVersion is the latest version the change was drafted against.
	// Empty for a create.
	ExpectedVersion string `json:"expected_version,omitempty"`
	// Digest and PolicyHash identify the proposed content. Empty for a
	// delete.
	Digest     string    `json:"digest,omitempty"`
	PolicyHash string    `json:"policy_hash,omitempty"`
	ProposedBy string    `json:"proposed_by,omitempty"`
	ExpiresAt  time.Time `json:"expires_at"`

	record definition.Record
	runID  string
}

// key identifies a change by its content, so proposing the same change twice
// (a replayed attempt, a model repeating itself) yields one proposal.
func (p proposal) key() string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s", p.Op, p.Agent, p.Version, p.ExpectedVersion, p.Digest, p.PolicyHash)
}

// pending is the in-memory set of staged proposals, partitioned by tenant.
//
// The partition is the security property: a token is only ever looked up
// inside the tenant the applying run resolved to, so a token that leaked
// across tenants names nothing there.
type pending struct {
	clock func() time.Time
	ttl   time.Duration

	mu       sync.Mutex
	byTenant map[string]map[string]proposal // tenant -> token -> proposal
}

func newPending(clock func() time.Time, ttl time.Duration) *pending {
	return &pending{clock: clock, ttl: ttl, byTenant: make(map[string]map[string]proposal)}
}

// pruneLocked drops expired proposals for one tenant.
func (s *pending) pruneLocked(tenant string) {
	now := s.clock()
	for token, p := range s.byTenant[tenant] {
		if !now.Before(p.ExpiresAt) {
			delete(s.byTenant[tenant], token)
		}
	}
}

// stage records p for tenant and returns it with its token and expiry set. A
// change already staged is returned as it was, original run included, so
// repeating a proposal cannot be used to move it into a fresh run.
func (s *pending) stage(tenant string, p proposal) (proposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(tenant)
	for _, existing := range s.byTenant[tenant] {
		if existing.key() == p.key() {
			return existing, nil
		}
	}
	if len(s.byTenant[tenant]) >= MaxPending {
		return proposal{}, fmt.Errorf("%w: tenant has %d staged; apply some or let them expire", ErrTooManyPending, MaxPending)
	}
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return proposal{}, fmt.Errorf("author: mint proposal token: %w", err)
	}
	p.Token = "prop_" + hex.EncodeToString(buf[:])
	p.ExpiresAt = s.clock().Add(s.ttl).UTC()
	if s.byTenant[tenant] == nil {
		s.byTenant[tenant] = make(map[string]proposal)
	}
	s.byTenant[tenant][p.Token] = p
	return p, nil
}

// take removes and returns the proposal a run may apply. Removing it under the
// lock is what makes a token single-use when two runs race to apply it.
func (s *pending) take(tenant, token, runID string) (proposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(tenant)
	p, ok := s.byTenant[tenant][token]
	if !ok {
		return proposal{}, ErrToken
	}
	if p.runID == runID {
		return proposal{}, ErrSameRun
	}
	delete(s.byTenant[tenant], token)
	return p, nil
}

// restore puts back a proposal whose apply failed for a reason that says
// nothing about the proposal itself, so a transient store error does not cost
// the human their approval.
func (s *pending) restore(tenant string, p proposal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.clock().Before(p.ExpiresAt) {
		return
	}
	if s.byTenant[tenant] == nil {
		s.byTenant[tenant] = make(map[string]proposal)
	}
	s.byTenant[tenant][p.Token] = p
}

// list returns a tenant's live proposals, soonest to expire first.
func (s *pending) list(tenant string) []proposal {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(tenant)
	out := make([]proposal, 0, len(s.byTenant[tenant]))
	for _, p := range s.byTenant[tenant] {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ExpiresAt.Equal(out[j].ExpiresAt) {
			return out[i].ExpiresAt.Before(out[j].ExpiresAt)
		}
		return out[i].Token < out[j].Token
	})
	return out
}
