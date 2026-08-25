package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

type LeaseExecutor interface {
	ExecuteWithLease(context.Context, runtime.ExecutionEnvelope, uint64, func(context.Context) error) error
}

type Consumer struct {
	WorkerID        string
	Shards          []broker.Shard
	Broker          broker.Broker
	Leases          coordination.LeaseManager
	Sessions        sessionstore.AtomicSessionStore
	Executor        LeaseExecutor
	LeaseTTL        time.Duration
	RenewInterval   time.Duration
	RetryWait       time.Duration
	ReclaimInterval time.Duration
	ReclaimLimit    int
	OnDeliveryError func(context.Context, broker.Delivery, error)
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
	if w.RenewInterval <= 0 {
		w.RenewInterval = w.LeaseTTL / 3
	}
	if w.ReclaimInterval <= 0 {
		w.ReclaimInterval = time.Second
	}
	if w.ReclaimLimit <= 0 {
		w.ReclaimLimit = 100
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	process := func(deliveryCtx context.Context, delivery broker.Delivery) error {
		for {
			err := w.handle(deliveryCtx, delivery)
			if !errors.Is(err, runtime.ErrCommitConflict) && !errors.Is(err, runtime.ErrVersionConflict) {
				return err
			}
			if err := wait(deliveryCtx, w.RetryWait); err != nil {
				return err
			}
		}
	}
	reclaimDone := make(chan struct{})
	go func() {
		defer close(reclaimDone)
		ticker := time.NewTicker(w.ReclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				deliveries, err := w.Broker.Reclaim(runCtx, broker.ReclaimOptions{ConsumerID: w.WorkerID, Limit: w.ReclaimLimit})
				if err != nil {
					w.report(runCtx, broker.Delivery{}, err)
					continue
				}
				for _, delivery := range deliveries {
					if err := process(runCtx, delivery); err != nil && runCtx.Err() == nil {
						w.report(runCtx, delivery, err)
					}
				}
			}
		}
	}()
	err := w.Broker.Consume(runCtx, broker.ConsumerOptions{ConsumerID: w.WorkerID, Shards: w.Shards}, func(deliveryCtx context.Context, delivery broker.Delivery) error {
		if err := process(deliveryCtx, delivery); err != nil {
			if deliveryCtx.Err() != nil {
				return deliveryCtx.Err()
			}
			w.report(deliveryCtx, delivery, err)
		}
		// Execution failures deliberately leave the delivery pending. Returning
		// nil keeps this Worker alive so an idle delivery can be reclaimed.
		return nil
	})
	cancel()
	<-reclaimDone
	return err
}

func (w Consumer) report(ctx context.Context, delivery broker.Delivery, err error) {
	if err != nil && w.OnDeliveryError != nil {
		w.OnDeliveryError(ctx, delivery, err)
	}
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
	executionCtx, cancelExecution := context.WithCancel(ctx)
	defer cancelExecution()
	stopRenewal := make(chan struct{})
	renewalDone := make(chan struct{})
	leaseLost := make(chan struct{})
	var leaseLostOnce sync.Once
	markLeaseLost := func() {
		leaseLostOnce.Do(func() {
			close(leaseLost)
			cancelExecution()
		})
	}
	go func() {
		defer close(renewalDone)
		ticker := time.NewTicker(w.RenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopRenewal:
				return
			case <-executionCtx.Done():
				return
			case <-ticker.C:
				if _, renewErr := w.Leases.Renew(executionCtx, lease, w.LeaseTTL); renewErr != nil {
					markLeaseLost()
					return
				}
			}
		}
	}()
	beforeCommit := func(commitCtx context.Context) error {
		select {
		case <-leaseLost:
			return runtime.ErrLeaseLost
		default:
		}
		if _, renewErr := w.Leases.Renew(commitCtx, lease, w.LeaseTTL); renewErr != nil {
			markLeaseLost()
			return runtime.ErrLeaseLost
		}
		return nil
	}
	executeErr := w.Executor.ExecuteWithLease(executionCtx, delivery.Envelope, lease.Fence, beforeCommit)
	close(stopRenewal)
	<-renewalDone
	select {
	case <-leaseLost:
		return runtime.ErrLeaseLost
	default:
	}
	if executeErr != nil {
		return executeErr
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
