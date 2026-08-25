// Package messaging defines durable Inbox, Outbox and delivery contracts.
package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"time"
)

type InboxKey struct {
	TenantID, Channel, ExternalAccountID, ExternalMessageID string
}

type InboxState string

const (
	InboxPreprocessPending InboxState = "preprocess_pending"
	InboxDispatchPending   InboxState = "dispatch_pending"
	InboxDispatchReady     InboxState = "dispatch_ready"
	InboxTerminal          InboxState = "terminal"
)

type ClaimInboxRequest struct {
	InboxKey
	AgentAppID, SessionID string
	PayloadDigest         string
	KeyVersion            int64
	InitialState          InboxState
}

// StableInboxIdentity is the single service-owned identity derivation used by
// every Inbox repository implementation.
func StableInboxIdentity(key InboxKey) (requestID, payloadRef string) {
	h := sha256.New()
	for _, part := range []string{key.TenantID, key.Channel, key.ExternalAccountID, key.ExternalMessageID} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(part)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(part))
	}
	requestID = "req_" + hex.EncodeToString(h.Sum(nil)[:16])
	return requestID, "inbound://" + key.TenantID + "/" + requestID
}

type InboxRecord struct {
	InboxKey
	RequestID, AgentAppID, SessionID string
	InputSeq                         uint64
	State                            InboxState
	PayloadRef, PayloadDigest        string
	KeyVersion, Version              int64
	TerminalReason, ResultRef        string
	CreatedAt, UpdatedAt             time.Time
}

type InboxClaimer interface {
	ClaimInbox(context.Context, ClaimInboxRequest) (InboxRecord, error)
	GetInbox(context.Context, InboxKey) (InboxRecord, error)
}

type PayloadRecord struct {
	TenantID, RequestID, PayloadRef, ContentDigest string
	Content                                        []byte
	KeyVersion                                     int64
	CreatedAt                                      time.Time
}

// PayloadStore persists normalized inbound payloads before durable ACK so a
// dispatcher can recover them after process restart.
type PayloadStore interface {
	PutPayload(context.Context, PayloadRecord) error
	GetPayload(context.Context, string, string) (PayloadRecord, error)
}

type ResultRecord struct {
	TenantID, RequestID, ResultRef, ContentDigest string
	Content                                       []byte
	KeyVersion                                    int64
	CreatedAt                                     time.Time
}

// ResultStore owns immutable terminal response payloads referenced by reply
// Outbox records.
type ResultStore interface {
	PutResult(context.Context, ResultRecord) error
	GetResult(context.Context, string, string) (ResultRecord, error)
}

type OutboxState string

const (
	OutboxPending    OutboxState = "pending"
	OutboxClaimed    OutboxState = "claimed"
	OutboxRetryWait  OutboxState = "retry_wait"
	OutboxPublished  OutboxState = "published"
	OutboxDeadLetter OutboxState = "dead_letter"
)

type OutboxRecord struct {
	TenantID, OutboxID, Kind, AggregateID, IdempotencyKey, PayloadRef string
	EventSeq, Version                                                 uint64
	State                                                             OutboxState
	Attempt                                                           int
	NextAttemptAt, ClaimUntil, CreatedAt                              time.Time
	ClaimOwner, TraceParent                                           string
}

type OutboxStore interface {
	ClaimOutbox(context.Context, string, int, string, time.Time) ([]OutboxRecord, error)
	RenewOutboxClaim(context.Context, string, string, uint64, string, time.Time) (uint64, error)
	MarkPublished(context.Context, string, string, uint64) error
	MarkRetry(context.Context, string, string, uint64, time.Time) error
}

type DeliveryKey struct {
	TenantID, DeliveryKey string
	SegmentNo             int
}

type DeliveryRecord struct {
	DeliveryKey
	ProviderMessageID, State string
	Version                  int64
	UpdatedAt                time.Time
}

type DeliveryLedger interface {
	GetDelivery(context.Context, DeliveryKey) (DeliveryRecord, error)
	RecordDelivery(context.Context, DeliveryRecord, int64) (DeliveryRecord, error)
}
