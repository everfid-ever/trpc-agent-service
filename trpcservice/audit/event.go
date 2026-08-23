// Package audit defines durable, versioned audit facts.
package audit

import (
	"context"
	"time"
)

type Event struct {
	SchemaVersion                                            uint16
	AuditID, TenantID, Channel, UserID, SessionID, RequestID string
	AgentAppID                                               string
	AgentAppRevision                                         int64
	AgentName, ToolName, Action, Decision, ReasonCode        string
	LatencyMS                                                int64
	ErrorType                                                string
	CostMicros                                               int64
	Currency                                                 string
	InputTokens, OutputTokens                                int64
	ConfigVersion, PolicyVersion                             int64
	ContentDigest, TraceID                                   string
	ResourceRefs                                             []string
	OccurredAt                                               time.Time
}
type AuditEvent = Event
type Sink interface {
	Emit(context.Context, Event) error
}
