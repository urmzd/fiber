package toolprovider

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// Memory is a Store held in process. It is the reference implementation:
// every rule in Store is enforced here exactly as Postgres enforces it.
type Memory struct {
	settings
	mu        sync.Mutex
	providers map[key]Provider
	snapshots map[key][]Snapshot // index i holds version i+1
}

type key struct{ tenant, name string }

// NewMemory returns an empty store.
func NewMemory(opts ...Option) *Memory {
	return &Memory{settings: newSettings(opts), providers: map[key]Provider{}, snapshots: map[key][]Snapshot{}}
}

var _ Store = (*Memory)(nil)

// PutProvider implements Store.
func (m *Memory) PutProvider(ctx context.Context, p Provider) (Provider, error) {
	if err := ctx.Err(); err != nil {
		return Provider{}, err
	}
	if err := p.Validate(); err != nil {
		return Provider{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key{p.Tenant, p.Name}
	cur, ok := m.providers[k]
	switch {
	case p.Version == 0 && ok:
		return Provider{}, fmt.Errorf("%w: provider %q", ErrExists, p.Name)
	case p.Version != 0 && !ok:
		return Provider{}, fmt.Errorf("%w: provider %q", ErrNotFound, p.Name)
	case p.Version != 0 && cur.Version != p.Version:
		return Provider{}, fmt.Errorf("%w: provider %q is at version %d, not %d", ErrConflict, p.Name, cur.Version, p.Version)
	}
	p = p.stored()
	p.Version++
	p.UpdatedAt = m.stamp()
	m.providers[k] = p
	return p.clone(), nil
}

// GetProvider implements Store.
func (m *Memory) GetProvider(ctx context.Context, tenant, name string) (Provider, error) {
	if err := checkName(ctx, tenant, name); err != nil {
		return Provider{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.providers[key{tenant, name}]
	if !ok {
		return Provider{}, fmt.Errorf("%w: provider %q", ErrNotFound, name)
	}
	return p.clone(), nil
}

// ListProviders implements Store.
func (m *Memory) ListProviders(ctx context.Context, tenant string) ([]Provider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tenant == "" {
		return nil, fmt.Errorf("%w: no tenant", ErrInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Provider{}
	for k, p := range m.providers {
		if k.tenant == tenant {
			out = append(out, p.clone())
		}
	}
	slices.SortFunc(out, func(a, b Provider) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// Accept implements Store.
func (m *Memory) Accept(ctx context.Context, tenant, provider string, base uint64, tools []Tool, by string) (Snapshot, error) {
	if err := checkName(ctx, tenant, provider); err != nil {
		return Snapshot{}, err
	}
	prepared, err := prepareTools(provider, tools)
	if err != nil {
		return Snapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key{tenant, provider}
	if _, ok := m.providers[k]; !ok {
		return Snapshot{}, fmt.Errorf("%w: provider %q", ErrNotFound, provider)
	}
	held := m.snapshots[k]
	if cur := uint64(len(held)); cur != base {
		return Snapshot{}, fmt.Errorf("%w: provider %q is at snapshot %d, not %d", ErrConflict, provider, cur, base)
	}
	s := Snapshot{
		Tenant: tenant, Provider: provider, Version: base + 1,
		Tools: prepared, AcceptedBy: by, AcceptedAt: m.stamp(),
	}
	m.snapshots[k] = append(held, s.clone())
	return s.clone(), nil
}

// Snapshot implements Store.
func (m *Memory) Snapshot(ctx context.Context, tenant, provider string, version uint64) (Snapshot, error) {
	if err := checkName(ctx, tenant, provider); err != nil {
		return Snapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key{tenant, provider}
	if _, ok := m.providers[k]; !ok {
		return Snapshot{}, fmt.Errorf("%w: provider %q", ErrNotFound, provider)
	}
	held := m.snapshots[k]
	if version == 0 {
		if len(held) == 0 {
			return emptySnapshot(tenant, provider), nil
		}
		return held[len(held)-1].clone(), nil
	}
	if version > uint64(len(held)) {
		return Snapshot{}, fmt.Errorf("%w: provider %q has no snapshot %d", ErrNotFound, provider, version)
	}
	return held[version-1].clone(), nil
}

func emptySnapshot(tenant, provider string) Snapshot {
	return Snapshot{Tenant: tenant, Provider: provider, Tools: []Tool{}}
}
