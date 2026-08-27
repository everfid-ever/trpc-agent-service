package redis

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

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

func TestWakeupQueueDeadLettersPoisonWithoutStopping(t *testing.T) {
	address := os.Getenv("TRPC_REDIS_TEST_ADDR")
	if address == "" {
		t.Skip("TRPC_REDIS_TEST_ADDR is not set")
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	publisher, err := NewPublisher(client, Config{Environment: fmt.Sprintf("wakeup_poison_%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewWakeupQueue(client, publisher, WakeupQueueConfig{Group: "workers", ReadBlock: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.XAdd(context.Background(), &redisclient.XAddArgs{Stream: publisher.WakeupStream(), Values: map[string]any{
		"schema_version": "1", "event": "{broken", "tenant_id": "tenant", "idempotency_key": "bad",
	}}).Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- queue.Consume(ctx, relay.WakeupConsumerOptions{ConsumerID: "worker"}, func(context.Context, relay.WakeupDelivery) error {
			t.Error("poison event reached handler")
			return nil
		})
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if length, _ := client.XLen(context.Background(), publisher.WakeupDeadLetterStream()).Result(); length == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if length, err := client.XLen(context.Background(), publisher.WakeupDeadLetterStream()).Result(); err != nil || length != 1 {
		t.Fatalf("dead-letter length=%d err=%v", length, err)
	}
	pending, err := client.XPending(context.Background(), publisher.WakeupStream(), "workers").Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wakeup consumer did not stop")
	}
	t.Cleanup(func() {
		_ = client.Del(context.Background(), publisher.WakeupStream(), publisher.WakeupDeadLetterStream()).Err()
	})
}
