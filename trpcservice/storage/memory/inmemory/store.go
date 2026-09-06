package inmemory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/memory"
)

type Store struct {
	mu         sync.RWMutex
	entries    map[memory.Key]memory.Entry
	mutations  map[string]memory.Entry
	watermarks map[string]int64
}

func New() *Store {
	return &Store{entries: make(map[memory.Key]memory.Entry), mutations: make(map[string]memory.Entry), watermarks: make(map[string]int64)}
}
func (s *Store) Put(ctx context.Context, request memory.PutRequest) (memory.Entry, error) {
	if err := ctx.Err(); err != nil {
		return memory.Entry{}, err
	}
	if err := request.Key.Validate(); err != nil || request.MutationID == "" || request.ExpectedVersion < 0 || request.ContentRef == "" || len(request.ContentDigest) != 64 {
		return memory.Entry{}, memory.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mutationKey := request.TenantID + "\x00" + request.MutationID
	if existing, ok := s.mutations[mutationKey]; ok {
		if !sameMutation(existing, request) {
			return memory.Entry{}, memory.ErrVersionConflict
		}
		return clone(existing), nil
	}
	current, exists := s.entries[request.Key]
	if (!exists && request.ExpectedVersion != 0) || (exists && current.Version != request.ExpectedVersion) {
		return memory.Entry{}, memory.ErrVersionConflict
	}
	now := time.Now().UTC()
	request.Version = current.Version + 1
	if !exists {
		request.CreatedAt = now
	}
	request.UpdatedAt = now
	s.watermarks[request.TenantID]++
	request.TenantWatermark = s.watermarks[request.TenantID]
	entry := clone(request.Entry)
	s.entries[entry.Key], s.mutations[mutationKey] = entry, entry
	return clone(entry), nil
}
func (s *Store) Get(ctx context.Context, key memory.Key) (memory.Entry, error) {
	if err := ctx.Err(); err != nil {
		return memory.Entry{}, err
	}
	if err := key.Validate(); err != nil {
		return memory.Entry{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.entries[key]
	if !ok {
		return memory.Entry{}, memory.ErrNotFound
	}
	return clone(value), nil
}
func (s *Store) List(ctx context.Context, query memory.Query) ([]memory.Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query.TenantID == "" || query.Scope == "" || (query.Scope == memory.ScopeUser && query.SubjectID == "") || (query.Scope == memory.ScopeTenant && query.SubjectID != "") {
		return nil, memory.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := []memory.Entry{}
	for _, v := range s.entries {
		if v.TenantID == query.TenantID && v.Scope == query.Scope && v.SubjectID == query.SubjectID {
			result = append(result, clone(v))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].MemoryID < result[j].MemoryID })
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}
	return result, nil
}
func (s *Store) TenantWatermark(ctx context.Context, tenantID string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if tenantID == "" {
		return 0, memory.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.watermarks[tenantID], nil
}
func clone(in memory.Entry) memory.Entry {
	out := in
	out.Attributes = map[string]string{}
	for k, v := range in.Attributes {
		out.Attributes[k] = v
	}
	return out
}

func sameMutation(entry memory.Entry, request memory.PutRequest) bool {
	if entry.Key != request.Key || entry.ContentRef != request.ContentRef || entry.ContentDigest != request.ContentDigest || len(entry.Attributes) != len(request.Attributes) {
		return false
	}
	for key, value := range entry.Attributes {
		if request.Attributes[key] != value {
			return false
		}
	}
	return true
}

var _ memory.Store = (*Store)(nil)
