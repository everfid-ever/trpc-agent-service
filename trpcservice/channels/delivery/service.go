// Package delivery owns provider-neutral reliable reply delivery after the
// account queue. Provider adapters are called only after a Delivery Ledger
// segment claim succeeds.
package delivery

import (
	"context"
	"errors"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type AdapterResolver interface {
	ResolveAdapter(context.Context, string, string) (channel.Adapter, error)
}

type AmbiguousError struct{ Err error }

func (e AmbiguousError) Error() string { return e.Err.Error() }
func (e AmbiguousError) Unwrap() error { return e.Err }

type RetryAfterError struct {
	Err        error
	RetryAfter time.Duration
}

func (e RetryAfterError) Error() string { return e.Err.Error() }
func (e RetryAfterError) Unwrap() error { return e.Err }

// DeferredError keeps the account-queue entry pending while another delivery
// attempt is active or its durable retry deadline has not arrived.
type DeferredError struct{ NotBefore time.Time }

func (e DeferredError) Error() string { return "reply delivery is deferred" }

type Service struct {
	Results           messaging.ResultStore
	Ledger            messaging.DeliveryLedger
	Adapters          AdapterResolver
	Owner             string
	ClaimTTL          time.Duration
	RendererVersion   string
	FormatVersion     string
	DefaultRetryDelay time.Duration
}

func (s Service) Deliver(ctx context.Context, event channel.ReplyEvent) error {
	if s.Results == nil || s.Ledger == nil || s.Adapters == nil || event.SchemaVersion != 1 ||
		s.Owner == "" || event.TenantID == "" || event.RequestID == "" || event.ChannelBindingID == "" || event.DeliveryKey == "" || event.ContentRef == "" {
		return runtime.ErrInvariantViolation
	}
	result, err := s.Results.GetResult(ctx, event.TenantID, event.RequestID)
	if err != nil {
		return err
	}
	if result.ResultRef != event.ContentRef {
		return runtime.ErrTenantScope
	}
	rendererVersion := s.RendererVersion
	if rendererVersion == "" {
		rendererVersion = "terminal-text-v1"
	}
	formatVersion := s.FormatVersion
	if formatVersion == "" {
		formatVersion = "text-v1"
	}
	key := messaging.DeliveryKey{TenantID: event.TenantID, DeliveryKey: event.DeliveryKey, SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: rendererVersion, FormatVersion: formatVersion, ContentDigest: result.ContentDigest, SegmentCount: 1}
	claimTTL := s.ClaimTTL
	if claimTTL <= 0 {
		claimTTL = 30 * time.Second
	}
	record, acquired, err := s.Ledger.ClaimDelivery(ctx, key, plan, messaging.DeliveryClaim{Owner: s.Owner, TTL: claimTTL})
	if err != nil {
		return err
	}
	if !acquired {
		switch record.State {
		case messaging.DeliverySent, messaging.DeliveryFailed:
			return nil
		case messaging.DeliveryAmbiguous:
			return s.reconcile(ctx, event, record)
		case messaging.DeliveryPending, messaging.DeliverySending, messaging.DeliveryRetryWait:
			return DeferredError{NotBefore: record.NotBefore}
		default:
			return runtime.ErrInvariantViolation
		}
	}
	adapter, err := s.Adapters.ResolveAdapter(ctx, event.TenantID, event.ChannelBindingID)
	if err != nil {
		return s.finishRetry(ctx, record, err, 0)
	}
	resultDelivery, deliverErr := adapter.Deliver(ctx, channel.DeliveryRequest{Event: event, ClientRequestID: record.ClientRequestID})
	if deliverErr != nil {
		var ambiguous AmbiguousError
		if errors.As(deliverErr, &ambiguous) {
			record.State, record.LastErrorClass = messaging.DeliveryAmbiguous, "response_lost"
			_, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
			return errors.Join(deliverErr, finishErr)
		}
		var retryAfter RetryAfterError
		if errors.As(deliverErr, &retryAfter) {
			return s.finishRetry(ctx, record, deliverErr, retryAfter.RetryAfter)
		}
		return s.finishRetry(ctx, record, deliverErr, 0)
	}
	if !resultDelivery.Delivered {
		return s.finishRetry(ctx, record, runtime.ErrBackendUnavailable, 0)
	}
	record.State = messaging.DeliverySent
	record.ProviderMessageID = resultDelivery.ProviderMessageID
	record.LastErrorClass = ""
	_, err = s.Ledger.FinishDelivery(ctx, record, record.Version)
	return err
}

func (s Service) reconcile(ctx context.Context, event channel.ReplyEvent, record messaging.DeliveryRecord) error {
	adapter, err := s.Adapters.ResolveAdapter(ctx, event.TenantID, event.ChannelBindingID)
	if err != nil {
		return err
	}
	reconciler, ok := adapter.(channel.DeliveryReconciler)
	if !ok {
		return runtime.ErrCommitConflict
	}
	result, err := reconciler.ReconcileDelivery(ctx, channel.ReconciliationRequest{Event: event, ClientRequestID: record.ClientRequestID})
	if err != nil {
		return err
	}
	switch result.Status {
	case channel.ReconciliationDelivered:
		if result.ProviderMessageID == "" {
			return runtime.ErrInvariantViolation
		}
		record.State, record.ProviderMessageID, record.LastErrorClass = messaging.DeliverySent, result.ProviderMessageID, ""
		_, err = s.Ledger.ReconcileDelivery(ctx, record, record.Version)
		return err
	case channel.ReconciliationNotDelivered:
		record.State, record.NotBefore, record.LastErrorClass = messaging.DeliveryRetryWait, time.Now().UTC(), "reconciled_not_delivered"
		if _, err = s.Ledger.ReconcileDelivery(ctx, record, record.Version); err != nil {
			return err
		}
		return DeferredError{NotBefore: record.NotBefore}
	case channel.ReconciliationUnknown:
		return runtime.ErrCommitConflict
	default:
		return runtime.ErrInvariantViolation
	}
}

func (s Service) finishRetry(ctx context.Context, record messaging.DeliveryRecord, cause error, delay time.Duration) error {
	if delay <= 0 {
		delay = s.DefaultRetryDelay
	}
	if delay <= 0 {
		delay = time.Second
	}
	record.State = messaging.DeliveryRetryWait
	record.NotBefore = time.Now().Add(delay)
	record.LastErrorClass = "retryable"
	_, finishErr := s.Ledger.FinishDelivery(ctx, record, record.Version)
	return errors.Join(cause, finishErr)
}
