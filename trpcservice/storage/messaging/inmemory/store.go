// Package inmemory provides durable-semantics reference stores for tests.
package inmemory

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type Store struct {
	mu         sync.Mutex
	inbox      map[messaging.InboxKey]messaging.InboxRecord
	deliveries map[messaging.DeliveryKey]messaging.DeliveryRecord
}

func New() *Store {
	return &Store{inbox: make(map[messaging.InboxKey]messaging.InboxRecord), deliveries: make(map[messaging.DeliveryKey]messaging.DeliveryRecord)}
}

func (s *Store) ClaimInbox(ctx context.Context, in messaging.ClaimInboxRequest) (messaging.InboxRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.InboxRecord{}, err
	}
	if in.TenantID == "" || in.Channel == "" || in.ExternalAccountID == "" || in.ExternalMessageID == "" {
		return messaging.InboxRecord{}, runtime.ErrTenantScope
	}
	if in.RequestID == "" || in.AgentAppID == "" || in.PayloadRef == "" || in.PayloadDigest == "" || in.KeyVersion < 1 ||
		(in.InitialState != messaging.InboxPreprocessPending && in.InitialState != messaging.InboxDispatchPending) {
		return messaging.InboxRecord{}, runtime.ErrCommitConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.inbox[in.InboxKey]; ok {
		if old.PayloadDigest != in.PayloadDigest || old.PayloadRef != in.PayloadRef || old.AgentAppID != in.AgentAppID || old.SessionID != in.SessionID {
			return messaging.InboxRecord{}, runtime.ErrIdempotencyCollision
		}
		return old, nil
	}
	now := time.Now().UTC()
	record := messaging.InboxRecord{InboxKey: in.InboxKey, RequestID: in.RequestID, AgentAppID: in.AgentAppID, SessionID: in.SessionID, State: in.InitialState, PayloadRef: in.PayloadRef, PayloadDigest: in.PayloadDigest, KeyVersion: in.KeyVersion, CreatedAt: now, UpdatedAt: now}
	s.inbox[in.InboxKey] = record
	return record, nil
}

func (s *Store) GetInbox(ctx context.Context, key messaging.InboxKey) (messaging.InboxRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.InboxRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.inbox[key]
	if !ok {
		return messaging.InboxRecord{}, runtime.ErrNotFound
	}
	return record, nil
}

func (s *Store) GetDelivery(ctx context.Context, key messaging.DeliveryKey) (messaging.DeliveryRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.DeliveryRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.deliveries[key]
	if !ok {
		return messaging.DeliveryRecord{}, runtime.ErrNotFound
	}
	return record, nil
}

func (s *Store) RecordDelivery(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.DeliveryRecord{}, err
	}
	if in.TenantID == "" || in.DeliveryKey.DeliveryKey == "" || in.SegmentNo < 0 || in.State == "" {
		return messaging.DeliveryRecord{}, runtime.ErrTenantScope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.deliveries[in.DeliveryKey]
	if exists && old.Version != expectedVersion {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	if !exists && expectedVersion != 0 {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	in.Version = expectedVersion + 1
	in.UpdatedAt = time.Now().UTC()
	s.deliveries[in.DeliveryKey] = in
	return in, nil
}

var _ messaging.InboxClaimer = (*Store)(nil)
var _ messaging.DeliveryLedger = (*Store)(nil)
