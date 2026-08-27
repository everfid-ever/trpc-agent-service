package delivery

import (
	"context"
	"errors"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type replyQueueStub struct {
	reclaimed []channel.ReplyDelivery
	acked     []channel.ReplyDelivery
}

func (*replyQueueStub) ConsumeReplies(ctx context.Context, _ channel.ReplyDestination, _ channel.ReplyConsumerOptions, _ func(context.Context, channel.ReplyDelivery) error) error {
	<-ctx.Done()
	return ctx.Err()
}
func (q *replyQueueStub) AckReply(_ context.Context, _ channel.ReplyDestination, delivery channel.ReplyDelivery) error {
	q.acked = append(q.acked, delivery)
	return nil
}
func (q *replyQueueStub) ReclaimReplies(context.Context, channel.ReplyDestination, channel.ReplyConsumerOptions) ([]channel.ReplyDelivery, error) {
	return append([]channel.ReplyDelivery(nil), q.reclaimed...), nil
}

type eventDelivererStub struct {
	calls int
	err   error
}

func (s *eventDelivererStub) Deliver(context.Context, channel.ReplyEvent) error {
	s.calls++
	return s.err
}

func TestConsumerACKsOnlyAfterDurableDeliverySuccess(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	delivery := channel.ReplyDelivery{ID: "1-0", Destination: destination, Event: channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "reply", ContentRef: "result://request"}}
	queue := &replyQueueStub{reclaimed: []channel.ReplyDelivery{delivery}}
	deliverer := &eventDelivererStub{}
	consumer := Consumer{Queue: queue, Deliverer: deliverer, Destination: destination, ConsumerID: "adapter-1"}
	count, err := consumer.ReclaimOnce(context.Background())
	if err != nil || count != 1 || deliverer.calls != 1 || len(queue.acked) != 1 {
		t.Fatalf("count=%d calls=%d acked=%d err=%v", count, deliverer.calls, len(queue.acked), err)
	}
}

func TestConsumerLeavesRetryableDeliveryPending(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	delivery := channel.ReplyDelivery{ID: "1-0", Destination: destination, Event: channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "reply", ContentRef: "result://request"}}
	queue := &replyQueueStub{reclaimed: []channel.ReplyDelivery{delivery}}
	deliverer := &eventDelivererStub{err: runtime.ErrBackendUnavailable}
	var reported error
	consumer := Consumer{Queue: queue, Deliverer: deliverer, Destination: destination, ConsumerID: "adapter-1", OnDeliveryError: func(_ channel.ReplyDelivery, err error) { reported = err }}
	count, err := consumer.ReclaimOnce(context.Background())
	if err != nil || count != 1 || len(queue.acked) != 0 || !errors.Is(reported, runtime.ErrBackendUnavailable) {
		t.Fatalf("count=%d acked=%d reported=%v err=%v", count, len(queue.acked), reported, err)
	}
}

func TestConsumerRejectsCrossBindingDelivery(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	wrong := destination
	wrong.ChannelBindingID = "other"
	queue := &replyQueueStub{reclaimed: []channel.ReplyDelivery{{ID: "1-0", Destination: wrong}}}
	consumer := Consumer{Queue: queue, Deliverer: &eventDelivererStub{}, Destination: destination, ConsumerID: "adapter-1"}
	count, err := consumer.ReclaimOnce(context.Background())
	if count != 0 || !errors.Is(err, runtime.ErrTenantScope) || len(queue.acked) != 0 {
		t.Fatalf("count=%d acked=%d err=%v", count, len(queue.acked), err)
	}
}
