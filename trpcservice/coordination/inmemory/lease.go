// Package inmemory implements monotonic local fencing for contract tests.
package inmemory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Manager struct {
	mu     sync.Mutex
	leases map[coordination.SessionKey]coordination.Lease
	fences map[coordination.SessionKey]uint64
	nextID uint64
}

func New() *Manager {
	return &Manager{leases: make(map[coordination.SessionKey]coordination.Lease), fences: make(map[coordination.SessionKey]uint64)}
}
func (m *Manager) Acquire(ctx context.Context, key coordination.SessionKey, workerID string, ttl time.Duration) (coordination.Lease, error) {
	if err := ctx.Err(); err != nil {
		return coordination.Lease{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if current, ok := m.leases[key]; ok && current.ExpiresAt.After(now) {
		return coordination.Lease{}, runtime.ErrVersionConflict
	}
	m.fences[key]++
	m.nextID++
	lease := coordination.Lease{Session: key, WorkerID: workerID, LeaseID: fmt.Sprintf("lease-%d", m.nextID), Fence: m.fences[key], ExpiresAt: now.Add(ttl)}
	m.leases[key] = lease
	return lease, nil
}
func (m *Manager) Renew(ctx context.Context, lease coordination.Lease, ttl time.Duration) (coordination.Lease, error) {
	if err := ctx.Err(); err != nil {
		return coordination.Lease{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.leases[lease.Session]
	if !ok || current.WorkerID != lease.WorkerID || current.LeaseID != lease.LeaseID || current.Fence != lease.Fence || current.ExpiresAt.Before(time.Now()) {
		return coordination.Lease{}, runtime.ErrLeaseLost
	}
	current.ExpiresAt = time.Now().Add(ttl)
	m.leases[lease.Session] = current
	return current, nil
}
func (m *Manager) Release(ctx context.Context, lease coordination.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.leases[lease.Session]
	if !ok {
		return nil
	}
	if current.LeaseID != lease.LeaseID || current.Fence != lease.Fence {
		return runtime.ErrLeaseLost
	}
	delete(m.leases, lease.Session)
	return nil
}
func (m *Manager) EnsureFenceAtLeast(ctx context.Context, key coordination.SessionKey, min uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fences[key] < min {
		m.fences[key] = min
	}
	return nil
}
