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
	payloads   map[string]messaging.PayloadRecord
	results    map[string]messaging.ResultRecord
	routes     map[string]messaging.ReplyRoute
}

func New() *Store {
	return &Store{inbox: make(map[messaging.InboxKey]messaging.InboxRecord), deliveries: make(map[messaging.DeliveryKey]messaging.DeliveryRecord), payloads: make(map[string]messaging.PayloadRecord), results: make(map[string]messaging.ResultRecord), routes: make(map[string]messaging.ReplyRoute)}
}

func (s *Store) PutReplyRoute(route messaging.ReplyRoute) error {
	if route.TenantID == "" || route.RequestID == "" || route.Channel == "" || route.ChannelBindingID == "" || route.ExternalAccountID == "" || route.ConfigVersion < 1 {
		return runtime.ErrTenantScope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := route.TenantID + "\x00" + route.RequestID
	if old, ok := s.routes[key]; ok && old != route {
		return runtime.ErrIdempotencyCollision
	}
	s.routes[key] = route
	return nil
}

func (s *Store) ResolveReplyRoute(ctx context.Context, tenantID, requestID string) (messaging.ReplyRoute, error) {
	if err := ctx.Err(); err != nil {
		return messaging.ReplyRoute{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	route, ok := s.routes[tenantID+"\x00"+requestID]
	if !ok {
		return messaging.ReplyRoute{}, runtime.ErrNotFound
	}
	return route, nil
}

func (s *Store) PutResult(ctx context.Context, in messaging.ResultRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.TenantID == "" || in.RequestID == "" || in.ResultRef == "" || in.ContentDigest == "" || len(in.Content) == 0 {
		return runtime.ErrCommitConflict
	}
	if in.KeyVersion < 1 {
		in.KeyVersion = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := in.TenantID + "\x00" + in.RequestID
	if old, ok := s.results[key]; ok {
		if old.ResultRef != in.ResultRef || old.ContentDigest != in.ContentDigest || old.KeyVersion != in.KeyVersion || string(old.Content) != string(in.Content) {
			return runtime.ErrIdempotencyCollision
		}
		return nil
	}
	in.Content = append([]byte(nil), in.Content...)
	in.CreatedAt = time.Now().UTC()
	s.results[key] = in
	return nil
}

func (s *Store) GetResult(ctx context.Context, tenantID, requestID string) (messaging.ResultRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.ResultRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.results[tenantID+"\x00"+requestID]
	if !ok {
		return messaging.ResultRecord{}, runtime.ErrNotFound
	}
	record.Content = append([]byte(nil), record.Content...)
	return record, nil
}

func (s *Store) PutPayload(ctx context.Context, in messaging.PayloadRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.TenantID == "" || in.RequestID == "" || in.PayloadRef == "" || in.ContentDigest == "" || len(in.Content) == 0 {
		return runtime.ErrCommitConflict
	}
	if in.KeyVersion < 1 {
		in.KeyVersion = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := in.TenantID + "\x00" + in.RequestID
	if old, ok := s.payloads[key]; ok {
		if old.PayloadRef != in.PayloadRef || old.ContentDigest != in.ContentDigest || old.KeyVersion != in.KeyVersion || string(old.Content) != string(in.Content) {
			return runtime.ErrIdempotencyCollision
		}
		return nil
	}
	in.Content = append([]byte(nil), in.Content...)
	in.CreatedAt = time.Now().UTC()
	s.payloads[key] = in
	return nil
}

func (s *Store) GetPayload(ctx context.Context, tenantID, requestID string) (messaging.PayloadRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.PayloadRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.payloads[tenantID+"\x00"+requestID]
	if !ok {
		return messaging.PayloadRecord{}, runtime.ErrNotFound
	}
	record.Content = append([]byte(nil), record.Content...)
	return record, nil
}

func (s *Store) ClaimInbox(ctx context.Context, in messaging.ClaimInboxRequest) (messaging.InboxRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.InboxRecord{}, err
	}
	if in.TenantID == "" || in.Channel == "" || in.ExternalAccountID == "" || in.ExternalMessageID == "" {
		return messaging.InboxRecord{}, runtime.ErrTenantScope
	}
	if in.AgentAppID == "" || in.PayloadDigest == "" || in.KeyVersion < 1 ||
		(in.InitialState != messaging.InboxPreprocessPending && in.InitialState != messaging.InboxDispatchPending) {
		return messaging.InboxRecord{}, runtime.ErrCommitConflict
	}
	requestID, payloadRef := messaging.StableInboxIdentity(in.InboxKey)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.inbox[in.InboxKey]; ok {
		if old.PayloadDigest != in.PayloadDigest || old.PayloadRef != payloadRef || old.AgentAppID != in.AgentAppID || old.SessionID != in.SessionID {
			return messaging.InboxRecord{}, runtime.ErrIdempotencyCollision
		}
		return old, nil
	}
	now := time.Now().UTC()
	record := messaging.InboxRecord{InboxKey: in.InboxKey, RequestID: requestID, AgentAppID: in.AgentAppID, SessionID: in.SessionID, State: in.InitialState, PayloadRef: payloadRef, PayloadDigest: in.PayloadDigest, KeyVersion: in.KeyVersion, CreatedAt: now, UpdatedAt: now}
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

func (s *Store) ClaimDelivery(ctx context.Context, key messaging.DeliveryKey, plan messaging.DeliveryPlan, claim messaging.DeliveryClaim) (messaging.DeliveryRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	if err := validateDeliveryPlan(key, plan); err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	if claim.Owner == "" || claim.TTL <= 0 {
		return messaging.DeliveryRecord{}, false, runtime.ErrCommitConflict
	}
	clientRequestID, err := messaging.StableDeliveryRequestID(key)
	if err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	record, exists := s.deliveries[key]
	if !exists {
		record = messaging.DeliveryRecord{DeliveryKey: key, State: messaging.DeliverySending, Plan: plan, Attempt: 1, Version: 1,
			ClientRequestID: clientRequestID, ClaimOwner: claim.Owner, ClaimUntil: now.Add(claim.TTL), UpdatedAt: now}
		s.deliveries[key] = record
		return record, true, nil
	}
	if record.Plan != plan || record.ClientRequestID != clientRequestID {
		return messaging.DeliveryRecord{}, false, runtime.ErrIdempotencyCollision
	}
	if record.State == messaging.DeliverySending && !record.ClaimUntil.After(now) {
		record.State, record.LastErrorClass = messaging.DeliveryAmbiguous, "owner_lost"
		record.ClaimOwner, record.ClaimUntil = "", time.Time{}
		record.Version, record.UpdatedAt = record.Version+1, now
		s.deliveries[key] = record
		return record, false, nil
	}
	if record.State != messaging.DeliveryPending && (record.State != messaging.DeliveryRetryWait || record.NotBefore.After(now)) {
		return record, false, nil
	}
	record.State, record.Attempt, record.Version, record.UpdatedAt = messaging.DeliverySending, record.Attempt+1, record.Version+1, now
	record.ClaimOwner, record.ClaimUntil = claim.Owner, now.Add(claim.TTL)
	s.deliveries[key] = record
	return record, true, nil
}

func (s *Store) FinishDelivery(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.DeliveryRecord{}, err
	}
	if in.State != messaging.DeliverySent && in.State != messaging.DeliveryRetryWait && in.State != messaging.DeliveryAmbiguous && in.State != messaging.DeliveryFailed {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.deliveries[in.DeliveryKey]
	if !exists || old.Version != expectedVersion || old.State != messaging.DeliverySending || old.Plan != in.Plan ||
		old.ClaimOwner == "" || old.ClaimOwner != in.ClaimOwner || old.ClientRequestID != in.ClientRequestID {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	in.ClaimOwner, in.ClaimUntil = "", time.Time{}
	in.Version = expectedVersion + 1
	in.Attempt = old.Attempt
	in.UpdatedAt = time.Now().UTC()
	s.deliveries[in.DeliveryKey] = in
	return in, nil
}

func (s *Store) ReconcileDelivery(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.DeliveryRecord{}, err
	}
	if in.State != messaging.DeliverySent && in.State != messaging.DeliveryRetryWait && in.State != messaging.DeliveryFailed {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.deliveries[in.DeliveryKey]
	if !exists || old.Version != expectedVersion || old.State != messaging.DeliveryAmbiguous || old.Plan != in.Plan || old.ClientRequestID != in.ClientRequestID {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	in.ClaimOwner, in.ClaimUntil = "", time.Time{}
	in.Version, in.Attempt, in.UpdatedAt = expectedVersion+1, old.Attempt, time.Now().UTC()
	s.deliveries[in.DeliveryKey] = in
	return in, nil
}

func validateDeliveryPlan(key messaging.DeliveryKey, plan messaging.DeliveryPlan) error {
	if key.TenantID == "" || key.DeliveryKey == "" || key.SegmentNo < 0 {
		return runtime.ErrTenantScope
	}
	if plan.RendererVersion == "" || plan.FormatVersion == "" || plan.ContentDigest == "" || plan.SegmentCount < 1 || key.SegmentNo >= plan.SegmentCount {
		return runtime.ErrCommitConflict
	}
	return nil
}

var _ messaging.InboxClaimer = (*Store)(nil)
var _ messaging.PayloadStore = (*Store)(nil)
var _ messaging.ResultStore = (*Store)(nil)
var _ messaging.ReplyRouteStore = (*Store)(nil)
var _ messaging.DeliveryLedger = (*Store)(nil)
