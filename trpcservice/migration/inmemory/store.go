package inmemory

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Store struct {
	mu           sync.Mutex
	migrations   map[string]migration.Migration
	activeDomain map[string]string
	batches      map[string]migration.Batch
}

func New() *Store {
	return &Store{migrations: make(map[string]migration.Migration), activeDomain: make(map[string]string),
		batches: make(map[string]migration.Batch)}
}

func (s *Store) Create(ctx context.Context, in migration.CreateRequest) (migration.Migration, error) {
	if err := ctx.Err(); err != nil {
		return migration.Migration{}, err
	}
	created, err := migration.NewMigration(in)
	if err != nil {
		return migration.Migration{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := migrationKey(in.TenantID, in.MigrationID)
	if existing, ok := s.migrations[key]; ok {
		if sameCreation(existing, created) {
			return cloneMigration(existing), nil
		}
		return migration.Migration{}, runtime.ErrIdempotencyCollision
	}
	domainKey := in.TenantID + "\x00" + in.Domain
	if _, exists := s.activeDomain[domainKey]; exists {
		return migration.Migration{}, runtime.ErrVersionConflict
	}
	s.migrations[key] = created
	s.activeDomain[domainKey] = in.MigrationID
	return cloneMigration(created), nil
}

func (s *Store) Get(ctx context.Context, tenantID, migrationID string) (migration.Migration, error) {
	if err := ctx.Err(); err != nil {
		return migration.Migration{}, err
	}
	if tenantID == "" || migrationID == "" {
		return migration.Migration{}, runtime.ErrTenantScope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.migrations[migrationKey(tenantID, migrationID)]
	if !ok {
		return migration.Migration{}, runtime.ErrNotFound
	}
	return cloneMigration(value), nil
}

func (s *Store) Transition(ctx context.Context, in migration.TransitionRequest) (migration.Migration, error) {
	if err := ctx.Err(); err != nil {
		return migration.Migration{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := migrationKey(in.TenantID, in.MigrationID)
	current, ok := s.migrations[key]
	if !ok {
		return migration.Migration{}, runtime.ErrNotFound
	}
	next, err := migration.ApplyTransition(current, in)
	if err != nil {
		return migration.Migration{}, err
	}
	s.migrations[key] = next
	if next.State == migration.StateCleanup {
		delete(s.activeDomain, next.TenantID+"\x00"+next.Domain)
	}
	return cloneMigration(next), nil
}

func (s *Store) CommitBatch(ctx context.Context, in migration.BatchRequest) (migration.BatchResult, error) {
	if err := ctx.Err(); err != nil {
		return migration.BatchResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	batchKey := migrationKey(in.TenantID, in.MigrationID) + "\x00" + in.BatchID
	if existing, ok := s.batches[batchKey]; ok {
		if !migration.SameBatch(existing, in) {
			return migration.BatchResult{}, runtime.ErrIdempotencyCollision
		}
		current, ok := s.migrations[migrationKey(in.TenantID, in.MigrationID)]
		if !ok {
			return migration.BatchResult{}, runtime.ErrNotFound
		}
		return cloneBatchResult(migration.BatchResult{Migration: current, Batch: existing}), nil
	}
	key := migrationKey(in.TenantID, in.MigrationID)
	current, ok := s.migrations[key]
	if !ok {
		return migration.BatchResult{}, runtime.ErrNotFound
	}
	next, batch, err := migration.ApplyBatch(current, in)
	if err != nil {
		return migration.BatchResult{}, err
	}
	result := migration.BatchResult{Migration: next, Batch: batch}
	s.migrations[key] = next
	s.batches[batchKey] = batch
	return cloneBatchResult(result), nil
}

func migrationKey(tenantID, migrationID string) string { return tenantID + "\x00" + migrationID }

func sameCreation(left, right migration.Migration) bool {
	return left.TenantID == right.TenantID && left.MigrationID == right.MigrationID && left.Domain == right.Domain &&
		left.Epoch == right.Epoch && left.Source == right.Source && left.Target == right.Target && left.CreatedAt.Equal(right.CreatedAt)
}

func cloneMigration(value migration.Migration) migration.Migration       { return value }
func cloneBatchResult(value migration.BatchResult) migration.BatchResult { return value }

var _ migration.Repository = (*Store)(nil)
