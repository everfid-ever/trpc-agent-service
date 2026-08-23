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
type DeliveryRequest struct{ Event ReplyEvent }
type DeliveryResult struct {
	ProviderMessageID string
	Delivered         bool
}

type Adapter interface {
	ID() string
	Run(context.Context) error
	Verify(context.Context, CallbackRequest) error
	Decode(context.Context, CallbackRequest) ([]ProviderEvent, error)
	Deliver(context.Context, DeliveryRequest) (DeliveryResult, error)
	Capabilities() Capabilities
}
