package redis

import (
	"context"
	"os"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	redisclient "github.com/redis/go-redis/v9"
)

func TestPublisherRedis7Contract(t *testing.T) {
	address := os.Getenv("TRPC_REDIS_TEST_ADDR")
	if address == "" {
		t.Skip("TRPC_REDIS_TEST_ADDR is not set")
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	publisher, err := NewPublisher(client, Config{Environment: "relay-contract"})
	if err != nil {
		t.Fatal(err)
	}
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	if err := publisher.PublishReply(context.Background(), destination, channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply", ContentRef: "result://request", Final: true}); err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishWakeup(context.Background(), relay.WakeupEvent{TenantID: "tenant", AggregateID: "request", IdempotencyKey: "wakeup:request", PayloadRef: "execution://tenant/request"}); err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishTenantControl(context.Background(), relay.TenantControlEvent{TenantID: "tenant", Kind: "tenant-control", AggregateID: "tenant", IdempotencyKey: "tenant:2", PayloadRef: "tenant://tenant/2", Version: 2}); err != nil {
		t.Fatal(err)
	}
	for _, stream := range []string{publisher.ReplyStream(destination), publisher.WakeupStream(), publisher.TenantControlStream()} {
		if length, err := client.XLen(context.Background(), stream).Result(); err != nil || length < 1 {
			t.Fatalf("stream=%s length=%d err=%v", stream, length, err)
		}
		t.Cleanup(func() { _ = client.Del(context.Background(), stream).Err() })
	}
}
