package memory

import (
	"context"
	"sort"
	"sync"
	"time"
)

// ReadView keeps only writes made by one request. It is never shared across
// requests or nodes; durability remains owned by Store.
type ReadView struct {
	Store   Store
	mu      sync.RWMutex
	overlay map[Key]Entry
}

func NewReadView(store Store) *ReadView { return &ReadView{Store: store, overlay: make(map[Key]Entry)} }

func (v *ReadView) Put(ctx context.Context, request PutRequest) (Entry, error) {
	if v == nil || v.Store == nil {
		return Entry{}, ErrInvalid
	}
	entry, err := v.Store.Put(ctx, request)
	if err != nil {
		return Entry{}, err
	}
	v.mu.Lock()
	v.overlay[entry.Key] = clone(entry)
	v.mu.Unlock()
	return entry, nil
}

func (v *ReadView) Get(ctx context.Context, key Key) (Entry, error) {
	if v == nil || v.Store == nil {
		return Entry{}, ErrInvalid
	}
	v.mu.RLock()
	entry, ok := v.overlay[key]
	v.mu.RUnlock()
	if ok {
		return clone(entry), nil
	}
	return v.Store.Get(ctx, key)
}

func (v *ReadView) List(ctx context.Context, query Query) ([]Entry, error) {
	if v == nil || v.Store == nil {
		return nil, ErrInvalid
	}
	entries, err := v.Store.List(ctx, query)
	if err != nil {
		return nil, err
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	byKey := make(map[Key]Entry, len(entries)+len(v.overlay))
	for _, entry := range entries {
		byKey[entry.Key] = entry
	}
	for key, entry := range v.overlay {
		if key.TenantID == query.TenantID && key.Scope == query.Scope && key.SubjectID == query.SubjectID {
			byKey[key] = clone(entry)
		}
	}
	result := make([]Entry, 0, len(byKey))
	for _, entry := range byKey {
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].MemoryID < result[j].MemoryID })
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}
	return result, nil
}

// Cache is a process-local acceleration layer. Every invalidation removes a
// tenant's entries; missed invalidations converge through the bounded TTL.
type Cache struct {
	Store      Store
	TTL        time.Duration
	Now        func() time.Time
	mu         sync.RWMutex
	entries    map[Key]cachedEntry
	watermarks map[string]int64
}
type cachedEntry struct {
	entry     Entry
	expiresAt time.Time
}

func NewCache(store Store, ttl time.Duration) *Cache {
	return &Cache{Store: store, TTL: ttl, Now: time.Now, entries: make(map[Key]cachedEntry), watermarks: make(map[string]int64)}
}
func (c *Cache) Get(ctx context.Context, key Key) (Entry, error) {
	if c == nil || c.Store == nil {
		return Entry{}, ErrInvalid
	}
	now := c.now()
	c.mu.RLock()
	cached, ok := c.entries[key]
	c.mu.RUnlock()
	if ok && cached.expiresAt.After(now) {
		return clone(cached.entry), nil
	}
	entry, err := c.Store.Get(ctx, key)
	if err != nil {
		return Entry{}, err
	}
	c.mu.Lock()
	c.entries[key] = cachedEntry{entry: clone(entry), expiresAt: now.Add(c.ttl())}
	c.mu.Unlock()
	return entry, nil
}
func (c *Cache) ApplyInvalidation(event Invalidation) {
	if c == nil || event.TenantID == "" || event.Version < 1 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if event.Version <= c.watermarks[event.TenantID] {
		return
	}
	c.watermarks[event.TenantID] = event.Version
	for key := range c.entries {
		if key.TenantID == event.TenantID {
			delete(c.entries, key)
		}
	}
}
func (c *Cache) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}
func (c *Cache) ttl() time.Duration {
	if c.TTL > 0 {
		return c.TTL
	}
	return time.Second
}

func clone(in Entry) Entry {
	out := in
	out.Attributes = make(map[string]string, len(in.Attributes))
	for k, v := range in.Attributes {
		out.Attributes[k] = v
	}
	return out
}
