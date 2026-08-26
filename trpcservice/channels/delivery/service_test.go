package delivery

import (
	"context"
	"errors"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	memory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

type adapterStub struct {
	calls int
	err   error
}

func (*adapterStub) ID() string                                            { return "fake" }
func (*adapterStub) Run(context.Context) error                             { return nil }
func (*adapterStub) Verify(context.Context, channel.CallbackRequest) error { return nil }
func (*adapterStub) Decode(context.Context, channel.CallbackRequest) ([]channel.ProviderEvent, error) {
	return nil, nil
}
func (s *adapterStub) Deliver(context.Context, channel.DeliveryRequest) (channel.DeliveryResult, error) {
	s.calls++
	if s.err != nil {
		return channel.DeliveryResult{}, s.err
	}
	return channel.DeliveryResult{ProviderMessageID: "provider-message", Delivered: true}, nil
}
func (*adapterStub) Capabilities() channel.Capabilities { return channel.Capabilities{Text: true} }

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
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}}
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
}

func TestAmbiguousDeliveryIsNotBlindlyRetried(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	adapter := &adapterStub{err: AmbiguousError{Err: errors.New("response lost")}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}}
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
