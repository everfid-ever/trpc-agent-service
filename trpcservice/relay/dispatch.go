package relay

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

// DispatchRelay is the only component that publishes dispatch outbox rows.
// The authoritative envelope is reloaded from the task store; outbox payload
// references are never trusted as execution input.
type DispatchRelay struct {
	Outbox             messaging.OutboxStore
	Tasks              gateway.TaskStore
	Broker             broker.Broker
	Owner              string
	ShardCount         uint32
	BatchSize          int
	ClaimTTL           time.Duration
	ClaimRenewInterval time.Duration
	RetryDelay         time.Duration
	PollInterval       time.Duration
}

func (r DispatchRelay) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	interval := r.PollInterval
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		// Individual publish/mark failures are persisted as retry or reclaimed
		// after claim expiry. A transient backend failure must not stop relay.
		_, _ = r.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r DispatchRelay) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	batchSize := r.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	claimTTL := r.ClaimTTL
	if claimTTL <= 0 {
		claimTTL = 30 * time.Second
	}
	retryDelay := r.RetryDelay
	if retryDelay <= 0 {
		retryDelay = time.Second
	}
	records, err := r.Outbox.ClaimOutbox(ctx, "dispatch", batchSize, r.Owner, time.Now().Add(claimTTL))
	if err != nil {
		return 0, err
	}
	published := 0
	var relayErr error
	for _, record := range records {
		processCtx, cancelProcess := context.WithCancel(ctx)
		stopRenewal := make(chan struct{})
		renewalDone := make(chan struct{})
		claimLost := make(chan struct{})
		version := record.Version
		renewInterval := r.ClaimRenewInterval
		if renewInterval <= 0 {
			renewInterval = claimTTL / 3
		}
		go func() {
			defer close(renewalDone)
			ticker := time.NewTicker(renewInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stopRenewal:
					return
				case <-processCtx.Done():
					return
				case <-ticker.C:
					nextVersion, renewErr := r.Outbox.RenewOutboxClaim(processCtx, record.TenantID, record.OutboxID, version, r.Owner, time.Now().Add(claimTTL))
					if renewErr != nil {
						close(claimLost)
						cancelProcess()
						return
					}
					version = nextVersion
				}
			}
		}()
		status, loadErr := r.Tasks.GetExecution(processCtx, gateway.ExecutionKey{TenantID: record.TenantID, RequestID: record.AggregateID})
		if loadErr == nil {
			if status.Envelope.TenantID != record.TenantID || status.Envelope.RequestID != record.AggregateID {
				loadErr = runtime.ErrTenantScope
			} else {
				loadErr = status.Envelope.Validate()
			}
		}
		if loadErr == nil {
			var shard broker.Shard
			shard, loadErr = broker.ShardForSession(status.Envelope.TenantID, status.Envelope.AgentAppID, status.Envelope.SessionID, r.ShardCount)
			if loadErr == nil {
				loadErr = r.Broker.Publish(processCtx, shard, status.Envelope)
			}
		}
		close(stopRenewal)
		<-renewalDone
		cancelProcess()
		select {
		case <-claimLost:
			relayErr = errors.Join(relayErr, runtime.ErrLeaseLost)
			continue
		default:
		}
		if loadErr != nil {
			markErr := r.Outbox.MarkRetry(ctx, record.TenantID, record.OutboxID, version, time.Now().Add(retryDelay))
			relayErr = errors.Join(relayErr, loadErr, markErr)
			continue
		}
		if markErr := r.Outbox.MarkPublished(ctx, record.TenantID, record.OutboxID, version); markErr != nil {
			relayErr = errors.Join(relayErr, markErr)
			continue
		}
		published++
	}
	return published, relayErr
}

func (r DispatchRelay) validate() error {
	if r.Outbox == nil || r.Tasks == nil || r.Broker == nil || r.Owner == "" || r.ShardCount == 0 {
		return runtime.ErrInvariantViolation
	}
	return nil
}
