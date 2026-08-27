// Package contract defines provider-neutral Channel events.
package contract

import (
	"context"
	"time"
)

type MediaRef struct {
	ID, Kind, ContentType string
	Size                  int64
}
type CallbackRequest struct {
	Headers    map[string]string
	Body       []byte
	ReceivedAt time.Time
}
type Capabilities struct{ Text, StreamEdit, Markdown, Card, Image, File, Recall bool }
type ProviderEvent struct {
	SchemaVersion                                 uint16
	Channel, ExternalAccountID, ExternalMessageID string
	ConversationType                              string
	ExternalUserID, ExternalChatID                string
	MessageType, Text                             string
	MediaRefs                                     []MediaRef
	OccurredAt                                    time.Time
	TraceParent                                   string
}
type ChannelEvent struct {
	SchemaVersion                                 uint16
	TenantID, AgentAppID, ChannelBindingID        string
	RequestID, UserID, SessionID                  string
	Channel, ExternalMessageID, MessageType, Text string
	MediaRefs                                     []MediaRef
	PayloadRef, TraceParent                       string
}
type ReplyEvent struct {
	SchemaVersion                                      uint16
	TenantID, RequestID, ChannelBindingID, DeliveryKey string
	EventSeq                                           uint64
	Kind, ContentRef                                   string
	Final                                              bool
	TraceParent                                        string
}
type ReplyDestination struct {
	TenantID, Channel, ChannelBindingID, ExternalAccountID string
}
type ReplyPublisher interface {
	PublishReply(context.Context, ReplyDestination, ReplyEvent) error
}

type ReplyDelivery struct {
	ID          string
	Destination ReplyDestination
	Event       ReplyEvent
}

type ReplyConsumerOptions struct {
	ConsumerID string
	Limit      int
}

// ReplyQueue is the durable account-level handoff between Reply Relay and a
// Channel Adapter owner. A delivery is ACKed only after Delivery Ledger has
// reached a durable state that makes replay safe.
type ReplyQueue interface {
	ConsumeReplies(context.Context, ReplyDestination, ReplyConsumerOptions, func(context.Context, ReplyDelivery) error) error
	AckReply(context.Context, ReplyDestination, ReplyDelivery) error
	ReclaimReplies(context.Context, ReplyDestination, ReplyConsumerOptions) ([]ReplyDelivery, error)
}
type DeliveryRequest struct {
	Event           ReplyEvent
	ClientRequestID string
}
type DeliveryResult struct {
	ProviderMessageID string
	Delivered         bool
}

type ReconciliationStatus string

const (
	ReconciliationDelivered    ReconciliationStatus = "delivered"
	ReconciliationNotDelivered ReconciliationStatus = "not_delivered"
	ReconciliationUnknown      ReconciliationStatus = "unknown"
)

type ReconciliationRequest struct {
	Event           ReplyEvent
	ClientRequestID string
}

type ReconciliationResult struct {
	Status            ReconciliationStatus
	ProviderMessageID string
}

// DeliveryReconciler is optional. Adapters without a provider query or stable
// idempotency facility leave ambiguous deliveries pending for manual repair.
type DeliveryReconciler interface {
	ReconcileDelivery(context.Context, ReconciliationRequest) (ReconciliationResult, error)
}

type Adapter interface {
	ID() string
	Run(context.Context) error
	Verify(context.Context, CallbackRequest) error
	Decode(context.Context, CallbackRequest) ([]ProviderEvent, error)
	Deliver(context.Context, DeliveryRequest) (DeliveryResult, error)
	Capabilities() Capabilities
}
