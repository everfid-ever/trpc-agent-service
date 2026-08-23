// Package messaging defines durable Inbox, Outbox and delivery contracts.
package messaging

import (
	"context"
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
	RequestID, AgentAppID, SessionID string
	PayloadRef, PayloadDigest        string
	KeyVersion                       int64
	InitialState                     InboxState
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
