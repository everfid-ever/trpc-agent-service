package redis

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	brokercontract "github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
)

func TestRedisStreamsPublishConsumeAck(t *testing.T) {
	client := redisTestClient(t)
	environment := fmt.Sprintf("broker_test_%d", time.Now().UnixNano())
	instance, err := New(client, Config{Environment: environment, Group: "workers", ShardCount: 2, ReadBlock: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Del(context.Background(), instance.stream(0), instance.stream(1)).Err()
	})
	envelope := testEnvelope()
	if err := instance.Publish(context.Background(), 1, envelope); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	deliveries := make(chan brokercontract.Delivery, 1)
	done := make(chan error, 1)
	go func() {
		done <- instance.Consume(ctx, brokercontract.ConsumerOptions{ConsumerID: "worker-1", Shards: []brokercontract.Shard{0, 1}}, func(ctx context.Context, delivery brokercontract.Delivery) error {
			if err := instance.Ack(ctx, delivery); err != nil {
				return err
			}
			deliveries <- delivery
			cancel()
			return nil
		})
	}()
	select {
	case delivery := <-deliveries:
		if delivery.Shard != 1 || delivery.Envelope != envelope {
			t.Fatalf("delivery=%#v", delivery)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	<-done
	pending, err := client.XPending(context.Background(), instance.stream(1), "workers").Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
}

func redisTestClient(t *testing.T) *redisclient.Client {
	t.Helper()
	address := os.Getenv("TRPC_REDIS_TEST_ADDR")
	if address == "" {
		t.Skip("TRPC_REDIS_TEST_ADDR is not set")
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}

func testEnvelope() runtime.ExecutionEnvelope {
	return runtime.ExecutionEnvelope{
		SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1,
		AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1,
		RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1,
		PayloadRef: "payload://request", CreatedAt: time.Now().UTC(),
	}
}
