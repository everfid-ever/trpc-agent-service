package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
)

type leaseStub struct {
	mu        sync.Mutex
	renewals  int
	failRenew bool
}

func (s *leaseStub) Acquire(_ context.Context, key coordination.SessionKey, worker string, ttl time.Duration) (coordination.Lease, error) {
	return coordination.Lease{Session: key, WorkerID: worker, LeaseID: "lease", Fence: 1, ExpiresAt: time.Now().Add(ttl)}, nil
}
func (s *leaseStub) Renew(context.Context, coordination.Lease, time.Duration) (coordination.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewals++
	if s.failRenew {
		return coordination.Lease{}, runtime.ErrLeaseLost
	}
	return coordination.Lease{Fence: 1}, nil
}
func (*leaseStub) Release(context.Context, coordination.Lease) error { return nil }
func (*leaseStub) EnsureFenceAtLeast(context.Context, coordination.SessionKey, uint64) error {
	return nil
}

type brokerStub struct{ acked bool }

func (*brokerStub) Publish(context.Context, broker.Shard, runtime.ExecutionEnvelope) error {
	return nil
}
func (*brokerStub) Consume(context.Context, broker.ConsumerOptions, func(context.Context, broker.Delivery) error) error {
	return nil
}
func (s *brokerStub) Ack(context.Context, broker.Delivery) error { s.acked = true; return nil }
func (*brokerStub) Reclaim(context.Context, broker.ReclaimOptions) ([]broker.Delivery, error) {
	return nil, nil
}

type leaseExecutorFunc func(context.Context, runtime.ExecutionEnvelope, uint64, func(context.Context) error) error

func (f leaseExecutorFunc) ExecuteWithLease(ctx context.Context, envelope runtime.ExecutionEnvelope, fence uint64, guard func(context.Context) error) error {
	return f(ctx, envelope, fence, guard)
}

func TestConsumerRenewsSlowExecutionAndValidatesBeforeCommit(t *testing.T) {
	leases := &leaseStub{}
	messages := &brokerStub{}
	consumer := Consumer{WorkerID: "worker", Broker: messages, Leases: leases, Sessions: sessionmemory.New(), LeaseTTL: 30 * time.Millisecond, RenewInterval: 5 * time.Millisecond}
	consumer.Executor = leaseExecutorFunc(func(ctx context.Context, _ runtime.ExecutionEnvelope, _ uint64, guard func(context.Context) error) error {
		timer := time.NewTimer(35 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		return guard(ctx)
	})
	delivery := broker.Delivery{ID: "message", Envelope: runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}}
	if err := consumer.handle(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if leases.renewals < 2 || !messages.acked {
		t.Fatalf("renewals=%d acked=%t", leases.renewals, messages.acked)
	}
}

func TestConsumerDoesNotAckAfterLeaseLoss(t *testing.T) {
	leases := &leaseStub{failRenew: true}
	messages := &brokerStub{}
	consumer := Consumer{WorkerID: "worker", Broker: messages, Leases: leases, Sessions: sessionmemory.New(), LeaseTTL: 30 * time.Millisecond, RenewInterval: time.Millisecond}
	consumer.Executor = leaseExecutorFunc(func(ctx context.Context, _ runtime.ExecutionEnvelope, _ uint64, guard func(context.Context) error) error {
		<-ctx.Done()
		return guard(context.Background())
	})
	delivery := broker.Delivery{ID: "message", Envelope: runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}}
	err := consumer.handle(context.Background(), delivery)
	if !errors.Is(err, runtime.ErrLeaseLost) || messages.acked {
		t.Fatalf("err=%v acked=%t", err, messages.acked)
	}
}
