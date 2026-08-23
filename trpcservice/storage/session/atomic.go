// Package session defines the atomic session authority used by distributed
// Workers.
package session

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type SessionKey struct{ TenantID, AgentAppID, SessionID string }
type TerminalKey struct {
	SessionKey
	InputSeq uint64
}
type OpenForRunRequest struct {
	SessionKey
	RequestID string
	InputSeq  uint64
	Fence     uint64
}
type SessionHead struct {
	SessionKey
	Version                                 int64
	LastFence, LastSessionSeq, NextInputSeq uint64
	State                                   map[string]any
}
type BufferedEvent struct{ EventID, EventType, PayloadRef string }
type StateDelta map[string]any
type SummaryCandidate struct {
	BaseSessionSeq uint64
	ContentRef     string
}
type CommitTurnRequest struct {
	SessionKey
	RequestID, CommitID, Stage string
	InputSeq, Fence            uint64
	ExpectedVersion            int64
	Outcome                    runtime.Outcome
	Events                     []BufferedEvent
	StateDelta                 StateDelta
	SummaryCandidate           *SummaryCandidate
	ResultRef, ReplyCursor     string
}
type CommitTurnResult struct {
	CommitID               string
	Outcome                runtime.Outcome
	SessionVersion         int64
	ResultRef, ReplyCursor string
}

type AtomicSessionStore interface {
	OpenForRun(context.Context, OpenForRunRequest) (SessionHead, error)
	CommitTurn(context.Context, CommitTurnRequest) (CommitTurnResult, error)
	GetTerminalByInputSeq(context.Context, TerminalKey) (CommitTurnResult, error)
	ReadLastFence(context.Context, SessionKey) (uint64, error)
}
