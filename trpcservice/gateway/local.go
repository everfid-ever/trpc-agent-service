package gateway

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type BindingResolver interface {
	ResolveExecutionBinding(context.Context, tenant.Context) (tenant.ExecutionBinding, error)
}

// LocalDispatcher preserves production PrepareDispatch and Executor semantics
// while replacing the broker with an in-process call.
type LocalDispatcher struct {
	Tasks    TaskStore
	Bindings BindingResolver
	Executor Executor
}

func (d LocalDispatcher) Dispatch(ctx context.Context, in DispatchRequest) (ExecutionHandle, error) {
	if d.Tasks == nil || d.Bindings == nil || d.Executor == nil {
		return ExecutionHandle{}, runtime.ErrInvariantViolation
	}
	if err := in.Tenant.Validate(); err != nil {
		return ExecutionHandle{}, err
	}
	binding, err := d.Bindings.ResolveExecutionBinding(ctx, in.Tenant)
	if err != nil {
		return ExecutionHandle{}, err
	}
	prepared, err := d.Tasks.PrepareDispatch(ctx, PrepareDispatchRequest{Tenant: in.Tenant, Binding: binding, RequestID: in.RequestID, SessionID: in.SessionID, UserID: in.UserID, PayloadRef: in.PayloadRef, TraceParent: in.TraceParent})
	if err != nil {
		return ExecutionHandle{}, err
	}
	if !prepared.Accepted {
		return ExecutionHandle{RequestID: in.RequestID, Status: string(runtime.OutcomeDenied)}, nil
	}
	if err := d.Executor.Execute(ctx, prepared.Envelope); err != nil {
		return ExecutionHandle{}, err
	}
	return ExecutionHandle{RequestID: in.RequestID, Status: string(runtime.OutcomeSucceeded)}, nil
}
