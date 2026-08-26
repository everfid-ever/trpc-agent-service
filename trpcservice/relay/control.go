package relay

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type TenantControlEvent struct {
	TenantID, Kind, AggregateID, IdempotencyKey, PayloadRef, TraceParent string
	Version                                                              uint64
}

// TenantControlPublisher is broadcast semantics: every runtime node must see
// invalidation events. A single consumer-group delivery is not sufficient.
type TenantControlPublisher interface {
	PublishTenantControl(context.Context, TenantControlEvent) error
}

type TenantControlRelay struct {
	Outbox             messaging.OutboxStore
	Controls           TenantControlPublisher
	Kind, Owner        string
	BatchSize          int
	ClaimTTL           time.Duration
	ClaimRenewInterval time.Duration
	RetryDelay         time.Duration
	PollInterval       time.Duration
}

func (r TenantControlRelay) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	return r.base().Run(ctx, RecordProcessorFunc(r.publish))
}

func (r TenantControlRelay) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	return r.base().RunOnce(ctx, RecordProcessorFunc(r.publish))
}

func (r TenantControlRelay) publish(ctx context.Context, record messaging.OutboxRecord) error {
	if record.TenantID == "" || record.AggregateID == "" || record.EventSeq < 1 || record.PayloadRef == "" {
		return runtime.ErrInvariantViolation
	}
	event := TenantControlEvent{TenantID: record.TenantID, Kind: r.Kind,
		AggregateID: record.AggregateID, IdempotencyKey: record.IdempotencyKey,
		PayloadRef: record.PayloadRef, TraceParent: record.TraceParent, Version: record.EventSeq}
	return r.Controls.PublishTenantControl(ctx, event)
}

func (r TenantControlRelay) base() Base {
	return Base{Outbox: r.Outbox, Kind: r.Kind, Owner: r.Owner, BatchSize: r.BatchSize,
		ClaimTTL: r.ClaimTTL, ClaimRenewInterval: r.ClaimRenewInterval,
		RetryDelay: r.RetryDelay, PollInterval: r.PollInterval}
}

func (r TenantControlRelay) validate() error {
	if r.Outbox == nil || r.Controls == nil || r.Owner == "" || (r.Kind != "tenant-control" && r.Kind != "config-invalidation") {
		return runtime.ErrInvariantViolation
	}
	return nil
}
