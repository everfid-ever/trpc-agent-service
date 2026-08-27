package relay

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type wakeupQueueStub struct{ acked bool }

func (*wakeupQueueStub) Consume(context.Context, WakeupConsumerOptions, func(context.Context, WakeupDelivery) error) error {
	panic("unexpected call")
}
func (s *wakeupQueueStub) AckWakeup(context.Context, WakeupDelivery) error {
	s.acked = true
	return nil
}
func (*wakeupQueueStub) ReclaimWakeups(context.Context, WakeupReclaimOptions) ([]WakeupDelivery, error) {
	return nil, nil
}

type wakeupStoreStub struct {
	candidate gateway.WakeupCandidate
	marked    bool
}

func (s *wakeupStoreStub) InspectWakeup(context.Context, gateway.ExecutionKey) (gateway.WakeupCandidate, error) {
	return s.candidate, nil
}
func (s *wakeupStoreStub) MarkWoken(context.Context, gateway.ExecutionKey, int64) error {
	s.marked = true
	s.candidate.Execution.Outcome = runtime.OutcomeQueued
	return nil
}

type wakeupBrokerStub struct{ published runtime.ExecutionEnvelope }

func (s *wakeupBrokerStub) Publish(_ context.Context, _ broker.Shard, envelope runtime.ExecutionEnvelope) error {
	s.published = envelope
	return nil
}
func (*wakeupBrokerStub) Consume(context.Context, broker.ConsumerOptions, func(context.Context, broker.Delivery) error) error {
	panic("unexpected call")
}
func (*wakeupBrokerStub) Ack(context.Context, broker.Delivery) error { panic("unexpected call") }
func (*wakeupBrokerStub) Reclaim(context.Context, broker.ReclaimOptions) ([]broker.Delivery, error) {
	panic("unexpected call")
}

func TestWakeupDispatcherPublishesMarksThenAcknowledges(t *testing.T) {
	envelope := runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session", RequestID: "request"}
	queue := &wakeupQueueStub{}
	store := &wakeupStoreStub{candidate: gateway.WakeupCandidate{Execution: gateway.ExecutionStatus{Envelope: envelope, Outcome: runtime.OutcomePending}, Ready: true, Version: 4}}
	dispatch := &wakeupBrokerStub{}
	d := WakeupDispatcher{ConsumerID: "consumer", Wakeups: queue, Store: store, Dispatch: dispatch, ShardCount: 4}
	if err := d.handle(context.Background(), WakeupDelivery{ID: "1-0", Event: WakeupEvent{TenantID: "tenant", AggregateID: "request"}}); err != nil {
		t.Fatal(err)
	}
	if dispatch.published.RequestID != "request" || !store.marked || !queue.acked {
		t.Fatalf("published=%#v marked=%t acked=%t", dispatch.published, store.marked, queue.acked)
	}
}
