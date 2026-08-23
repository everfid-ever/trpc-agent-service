package inmemory

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type BundleFactory func(context.Context, profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error)
type bundleEntry struct {
	bundle   profile.RuntimeBundle
	close    func(context.Context) error
	refs     int
	retired  bool
	building bool
	ready    chan struct{}
}
type BundleManager struct {
	mu      sync.Mutex
	factory BundleFactory
	entries map[profile.ExecutionProfileKey]*bundleEntry
	closed  bool
}

func NewBundleManager(factory BundleFactory) *BundleManager {
	return &BundleManager{factory: factory, entries: make(map[profile.ExecutionProfileKey]*bundleEntry)}
}

func (m *BundleManager) Acquire(ctx context.Context, key profile.ExecutionProfileKey) (profile.BundleLease, error) {
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, runtime.ErrBackendUnavailable
		}
		if entry, ok := m.entries[key]; ok {
			if entry.retired {
				m.mu.Unlock()
				return nil, runtime.ErrVersionMismatch
			}
			if entry.building {
				ready := entry.ready
				m.mu.Unlock()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-ready:
				}
				continue
			}
			entry.refs++
			lease := &lease{manager: m, key: key, bundle: entry.bundle}
			m.mu.Unlock()
			return lease, nil
		}
		entry := &bundleEntry{building: true, ready: make(chan struct{})}
		m.entries[key] = entry
		m.mu.Unlock()
		bundle, closeFn, err := m.factory(ctx, key)
		m.mu.Lock()
		entry.building = false
		if err != nil {
			delete(m.entries, key)
			close(entry.ready)
			m.mu.Unlock()
			return nil, err
		}
		entry.bundle = bundle
		entry.close = closeFn
		entry.refs = 1
		close(entry.ready)
		result := &lease{manager: m, key: key, bundle: bundle}
		m.mu.Unlock()
		return result, nil
	}
}
func (m *BundleManager) Retire(key profile.ExecutionProfileKey) {
	m.mu.Lock()
	entry := m.entries[key]
	if entry == nil {
		m.mu.Unlock()
		return
	}
	entry.retired = true
	if entry.refs > 0 || entry.building {
		m.mu.Unlock()
		return
	}
	delete(m.entries, key)
	closeFn := entry.close
	m.mu.Unlock()
	if closeFn != nil {
		_ = closeFn(context.Background())
	}
}
func (m *BundleManager) Close(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	var closeFns []func(context.Context) error
	for key, entry := range m.entries {
		entry.retired = true
		if entry.refs == 0 && !entry.building {
			delete(m.entries, key)
			if entry.close != nil {
				closeFns = append(closeFns, entry.close)
			}
		}
	}
	m.mu.Unlock()
	for _, fn := range closeFns {
		if err := fn(ctx); err != nil {
			return err
		}
	}
	return nil
}

type lease struct {
	once    sync.Once
	manager *BundleManager
	key     profile.ExecutionProfileKey
	bundle  profile.RuntimeBundle
}

func (l *lease) Bundle() profile.RuntimeBundle { return l.bundle }
func (l *lease) Release()                      { l.once.Do(func() { l.manager.release(l.key) }) }
func (m *BundleManager) release(key profile.ExecutionProfileKey) {
	m.mu.Lock()
	entry := m.entries[key]
	if entry == nil {
		m.mu.Unlock()
		return
	}
	entry.refs--
	if entry.refs > 0 || !entry.retired {
		m.mu.Unlock()
		return
	}
	delete(m.entries, key)
	closeFn := entry.close
	m.mu.Unlock()
	if closeFn != nil {
		_ = closeFn(context.Background())
	}
}
