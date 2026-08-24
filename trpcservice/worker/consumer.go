package worker

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

type FencedExecutor interface {
	ExecuteWithFence(context.Context, runtime.ExecutionEnvelope, uint64) error
}

type Consumer struct {
	WorkerID  string
	Shards    []broker.Shard
	Broker    broker.Broker
	Leases    coordination.LeaseManager
	Sessions  sessionstore.AtomicSessionStore
	Executor  FencedExecutor
	LeaseTTL  time.Duration
	RetryWait time.Duration
}

func (w Consumer) Run(ctx context.Context) error {
	if w.WorkerID == "" || len(w.Shards) == 0 || w.Broker == nil || w.Leases == nil || w.Sessions == nil || w.Executor == nil {
		return runtime.ErrInvariantViolation
	}
	if w.LeaseTTL <= 0 {
		w.LeaseTTL = 5 * time.Second
	}
	if w.RetryWait <= 0 {
		w.RetryWait = 10 * time.Millisecond
	}
	return w.Broker.Consume(ctx, broker.ConsumerOptions{ConsumerID: w.WorkerID, Shards: w.Shards}, func(ctx context.Context, delivery broker.Delivery) error {
		for {
			err := w.handle(ctx, delivery)
			if !errors.Is(err, runtime.ErrCommitConflict) && !errors.Is(err, runtime.ErrVersionConflict) {
				return err
			}
			if err := wait(ctx, w.RetryWait); err != nil {
				return err
			}
		}
	})
}

func (w Consumer) handle(ctx context.Context, delivery broker.Delivery) error {
	key := coordination.SessionKey{
		TenantID: delivery.Envelope.TenantID, AgentAppID: delivery.Envelope.AgentAppID, SessionID: delivery.Envelope.SessionID,
	}
	persistedFence, err := w.Sessions.ReadLastFence(ctx, sessionstore.SessionKey(key))
	if err != nil {
		return err
	}
	if err := w.Leases.EnsureFenceAtLeast(ctx, key, persistedFence); err != nil {
		return err
	}
	lease, err := w.acquire(ctx, key)
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = w.Leases.Release(releaseCtx, lease)
	}()
	if err := w.Executor.ExecuteWithFence(ctx, delivery.Envelope, lease.Fence); err != nil {
		return err
	}
	return w.Broker.Ack(ctx, delivery)
}

func (w Consumer) acquire(ctx context.Context, key coordination.SessionKey) (coordination.Lease, error) {
	for {
		lease, err := w.Leases.Acquire(ctx, key, w.WorkerID, w.LeaseTTL)
		if err == nil {
			return lease, nil
		}
		if !errors.Is(err, runtime.ErrVersionConflict) {
			return coordination.Lease{}, err
		}
		if err := wait(ctx, w.RetryWait); err != nil {
			return coordination.Lease{}, err
		}
	}
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
