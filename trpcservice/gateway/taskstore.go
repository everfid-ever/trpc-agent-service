package gateway

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type PrepareDispatchRequest struct {
	Tenant      tenant.Context
	Binding     tenant.ExecutionBinding
	RequestID   string
	SessionID   string
	UserID      string
	PayloadRef  string
	TraceParent string
}

type PreparedDispatch struct {
	Envelope       runtime.ExecutionEnvelope
	Accepted       bool
	TerminalReason string
}
type ExecutionKey struct{ TenantID, RequestID string }
type ExecutionStatus struct {
	Envelope  runtime.ExecutionEnvelope
	Outcome   runtime.Outcome
	ResultRef string
}
type CancelRequest struct {
	TenantID, RequestID string
	ExpectedVersion     int64
}
type CancelResult struct {
	Accepted bool
	Version  int64
}
type ParkRequest struct {
	TenantID, RequestID string
	InputSeq            uint64
	Attempt             int
}

type TaskStore interface {
	PrepareDispatch(context.Context, PrepareDispatchRequest) (PreparedDispatch, error)
	GetExecution(context.Context, ExecutionKey) (ExecutionStatus, error)
	RequestCancel(context.Context, CancelRequest) (CancelResult, error)
	ParkInput(context.Context, ParkRequest) error
}
