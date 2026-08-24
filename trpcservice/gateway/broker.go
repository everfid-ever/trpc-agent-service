package gateway

import (
	"context"
	"hash/fnv"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type BrokerDispatcher struct {
	Tasks      TaskStore
	Bindings   BindingResolver
	Broker     broker.Broker
	ShardCount uint32
}

func (d BrokerDispatcher) Dispatch(ctx context.Context, in DispatchRequest) (ExecutionHandle, error) {
	if d.Tasks == nil || d.Bindings == nil || d.Broker == nil || d.ShardCount == 0 {
		return ExecutionHandle{}, runtime.ErrInvariantViolation
	}
	if err := in.Tenant.Validate(); err != nil {
		return ExecutionHandle{}, err
	}
	binding, err := d.Bindings.ResolveExecutionBinding(ctx, in.Tenant)
	if err != nil {
		return ExecutionHandle{}, err
	}
	prepared, err := d.Tasks.PrepareDispatch(ctx, PrepareDispatchRequest{
		Tenant: in.Tenant, Binding: binding, RequestID: in.RequestID, SessionID: in.SessionID,
		UserID: in.UserID, PayloadRef: in.PayloadRef, TraceParent: in.TraceParent,
	})
	if err != nil {
		return ExecutionHandle{}, err
	}
	if !prepared.Accepted {
		return ExecutionHandle{RequestID: in.RequestID, Status: string(runtime.OutcomeDenied)}, nil
	}
	if err := d.Broker.Publish(ctx, d.shard(prepared.Envelope), prepared.Envelope); err != nil {
		return ExecutionHandle{}, err
	}
	return ExecutionHandle{RequestID: in.RequestID, Status: "accepted"}, nil
}

func (d BrokerDispatcher) shard(envelope runtime.ExecutionEnvelope) broker.Shard {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(envelope.TenantID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(envelope.AgentAppID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(envelope.SessionID))
	return broker.Shard(hash.Sum32() % d.ShardCount)
}

var _ Dispatcher = BrokerDispatcher{}
