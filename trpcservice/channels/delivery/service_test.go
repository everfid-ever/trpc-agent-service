package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	memory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

type adapterStub struct {
	calls            int
	err              error
	clientRequestIDs []string
	delay            time.Duration
}

func (*adapterStub) ID() string                                            { return "fake" }
func (*adapterStub) Run(context.Context) error                             { return nil }
func (*adapterStub) Verify(context.Context, channel.CallbackRequest) error { return nil }
func (*adapterStub) Decode(context.Context, channel.CallbackRequest) ([]channel.ProviderEvent, error) {
	return nil, nil
}
func (s *adapterStub) Deliver(_ context.Context, request channel.DeliveryRequest) (channel.DeliveryResult, error) {
	s.calls++
	s.clientRequestIDs = append(s.clientRequestIDs, request.ClientRequestID)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if s.err != nil {
		return channel.DeliveryResult{}, s.err
	}
	return channel.DeliveryResult{ProviderMessageID: "provider-message", Delivered: true}, nil
}
func (*adapterStub) Capabilities() channel.Capabilities { return channel.Capabilities{Text: true} }

type reconcilingAdapter struct {
	adapterStub
	result         channel.ReconciliationResult
	reconcileCalls int
}

func (a *reconcilingAdapter) ReconcileDelivery(_ context.Context, request channel.ReconciliationRequest) (channel.ReconciliationResult, error) {
	a.reconcileCalls++
	if request.ClientRequestID == "" {
		return channel.ReconciliationResult{}, errors.New("missing client request id")
	}
	return a.result, nil
}

type resolverStub struct{ adapter channel.Adapter }

func (s resolverStub) ResolveAdapter(context.Context, string, string) (channel.Adapter, error) {
	return s.adapter, nil
}

func TestDeliveryLedgerPreventsDuplicateProviderEffect(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	if err := store.PutResult(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	adapter := &adapterStub{}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1"}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: result.ResultRef, Final: true}
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if adapter.calls != 1 {
		t.Fatalf("provider calls=%d", adapter.calls)
	}
	if len(adapter.clientRequestIDs) != 1 || adapter.clientRequestIDs[0] == "" {
		t.Fatalf("client request IDs=%#v", adapter.clientRequestIDs)
	}
}

func TestExpiredOwnerReconcilesDeliveredWithoutResend(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	key := messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "terminal-text-v1", FormatVersion: "text-v1", ContentDigest: result.ContentDigest, SegmentCount: 1}
	if _, acquired, err := store.ClaimDelivery(context.Background(), key, plan, messaging.DeliveryClaim{Owner: "dead-owner", TTL: time.Nanosecond}); err != nil || !acquired {
		t.Fatalf("preclaim acquired=%t err=%v", acquired, err)
	}
	time.Sleep(time.Millisecond)
	adapter := &reconcilingAdapter{result: channel.ReconciliationResult{Status: channel.ReconciliationDelivered, ProviderMessageID: "provider-existing"}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "new-owner"}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: result.ResultRef, Final: true}
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	record, err := store.GetDelivery(context.Background(), key)
	if err != nil || record.State != messaging.DeliverySent || record.ProviderMessageID != "provider-existing" || adapter.calls != 0 || adapter.reconcileCalls != 1 {
		t.Fatalf("record=%#v calls=%d reconcile=%d err=%v", record, adapter.calls, adapter.reconcileCalls, err)
	}
}

func TestExpiredOwnerReconcilesNotDeliveredThenRetriesSameRequestID(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	key := messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "terminal-text-v1", FormatVersion: "text-v1", ContentDigest: result.ContentDigest, SegmentCount: 1}
	claimed, acquired, err := store.ClaimDelivery(context.Background(), key, plan, messaging.DeliveryClaim{Owner: "dead-owner", TTL: time.Nanosecond})
	if err != nil || !acquired {
		t.Fatalf("preclaim=%#v acquired=%t err=%v", claimed, acquired, err)
	}
	time.Sleep(time.Millisecond)
	adapter := &reconcilingAdapter{result: channel.ReconciliationResult{Status: channel.ReconciliationNotDelivered}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "new-owner"}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: result.ResultRef, Final: true}
	var deferred DeferredError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &deferred) {
		t.Fatalf("reconcile err=%v", err)
	}
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if adapter.calls != 1 || len(adapter.clientRequestIDs) != 1 || adapter.clientRequestIDs[0] != claimed.ClientRequestID {
		t.Fatalf("calls=%d IDs=%#v claimed=%s", adapter.calls, adapter.clientRequestIDs, claimed.ClientRequestID)
	}
}

func TestUnknownReconciliationStopsAtMaxAttempts(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	key := messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "terminal-text-v1", FormatVersion: "text-v1", ContentDigest: result.ContentDigest, SegmentCount: 1}
	_, _, _ = store.ClaimDelivery(context.Background(), key, plan, messaging.DeliveryClaim{Owner: "dead-owner", TTL: time.Nanosecond})
	time.Sleep(time.Millisecond)
	adapter := &reconcilingAdapter{result: channel.ReconciliationResult{Status: channel.ReconciliationUnknown}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "new-owner", MaxReconcileAttempts: 2, DefaultRetryDelay: time.Nanosecond}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: result.ResultRef, Final: true}
	var deferred DeferredError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &deferred) {
		t.Fatalf("first reconcile err=%v", err)
	}
	time.Sleep(time.Millisecond)
	var terminal TerminalError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &terminal) {
		t.Fatalf("second reconcile err=%v", err)
	}
	record, _ := store.GetDelivery(context.Background(), key)
	if record.State != messaging.DeliveryFailed || record.LastErrorClass != "reconcile_exhausted" || adapter.reconcileCalls != 2 {
		t.Fatalf("record=%#v reconcile calls=%d", record, adapter.reconcileCalls)
	}
}

func TestNonReconcilableAmbiguousDeliveryReachesFailedTerminal(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	key := messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "terminal-text-v1", FormatVersion: "text-v1", ContentDigest: result.ContentDigest, SegmentCount: 1}
	_, _, _ = store.ClaimDelivery(context.Background(), key, plan, messaging.DeliveryClaim{Owner: "dead-owner", TTL: time.Nanosecond})
	time.Sleep(time.Millisecond)
	// adapterStub does not implement DeliveryReconciler.
	adapter := &adapterStub{}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "new-owner", MaxReconcileAttempts: 2, DefaultRetryDelay: time.Nanosecond}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: result.ResultRef, Final: true}
	var deferred DeferredError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &deferred) {
		t.Fatalf("first reconcile err=%v", err)
	}
	time.Sleep(time.Millisecond)
	var terminal TerminalError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &terminal) {
		t.Fatalf("expected exhausted terminal error, got %v", err)
	}
	record, _ := store.GetDelivery(context.Background(), key)
	if record.State != messaging.DeliveryFailed || record.LastErrorClass != "reconcile_exhausted" || adapter.calls != 0 {
		t.Fatalf("record=%#v provider calls=%d", record, adapter.calls)
	}
}

func TestAmbiguousDeliveryIsNotBlindlyRetried(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	adapter := &adapterStub{err: AmbiguousError{Err: errors.New("response lost")}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1"}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: result.ResultRef, Final: true}
	if err := service.Deliver(context.Background(), event); err == nil {
		t.Fatal("expected ambiguous delivery error")
	}
	if err := service.Deliver(context.Background(), event); err == nil {
		t.Fatal("expected unresolved ambiguous state")
	}
	if adapter.calls != 1 {
		t.Fatalf("provider calls=%d", adapter.calls)
	}
}

func TestRetryWaitIsDeferredInsteadOfReportedAsDelivered(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	adapter := &adapterStub{err: RetryAfterError{Err: errors.New("rate limited")}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1", DefaultRetryDelay: time.Hour}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: result.ResultRef, Final: true}
	if err := service.Deliver(context.Background(), event); err == nil {
		t.Fatal("expected first provider error")
	}
	var deferred DeferredError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &deferred) {
		t.Fatalf("expected deferred retry, got %v", err)
	}
	if adapter.calls != 1 {
		t.Fatalf("provider calls=%d", adapter.calls)
	}
}

func TestPermanentFailureReachesFailedTerminal(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	adapter := &adapterStub{err: PermanentError{Err: errors.New("recipient blocked"), Class: "recipient_blocked"}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1"}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: result.ResultRef, Final: true}
	var terminal TerminalError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &terminal) {
		t.Fatalf("expected terminal error, got %v", err)
	}
	record, err := store.GetDelivery(context.Background(), messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0})
	if err != nil || record.State != messaging.DeliveryFailed || record.LastErrorClass != "recipient_blocked" {
		t.Fatalf("record=%#v err=%v", record, err)
	}
	if err := service.Deliver(context.Background(), event); err != nil || adapter.calls != 1 {
		t.Fatalf("terminal replay calls=%d err=%v", adapter.calls, err)
	}
}

func TestRetryableFailureStopsAtMaxAttempts(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	adapter := &adapterStub{err: errors.New("temporary")}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1", MaxAttempts: 2, DefaultRetryDelay: time.Nanosecond}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: result.ResultRef, Final: true}
	if err := service.Deliver(context.Background(), event); err == nil {
		t.Fatal("expected first retryable error")
	}
	time.Sleep(time.Millisecond)
	var terminal TerminalError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &terminal) {
		t.Fatalf("expected exhausted terminal error, got %v", err)
	}
	record, _ := store.GetDelivery(context.Background(), messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0})
	if record.State != messaging.DeliveryFailed || record.LastErrorClass != "retry_exhausted" || adapter.calls != 2 {
		t.Fatalf("record=%#v calls=%d", record, adapter.calls)
	}
}

func TestLongProviderCallRenewsSendingClaim(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	adapter := &adapterStub{delay: 60 * time.Millisecond}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1", ClaimTTL: 20 * time.Millisecond, ClaimRenewInterval: 5 * time.Millisecond}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: result.ResultRef, Final: true}
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	record, _ := store.GetDelivery(context.Background(), messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0})
	if record.State != messaging.DeliverySent || record.Version < 4 {
		t.Fatalf("record=%#v", record)
	}
}

func TestResultReferenceMismatchUsesVersionError(t *testing.T) {
	store := memory.New()
	_ = store.PutResult(context.Background(), messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://right", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1})
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: &adapterStub{}}, Owner: "adapter-1"}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: "result://stale", Final: true}
	if err := service.Deliver(context.Background(), event); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("err=%v", err)
	}
}
