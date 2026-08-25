package inmemory

import (
	"context"
	"testing"
	"time"

	brokercontract "github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestReclaimWaitsForPendingIdleThreshold(t *testing.T) {
	instance := NewWithReclaimIdle(20 * time.Millisecond)
	envelope := runtime.ExecutionEnvelope{
		SchemaVersion: 1, TenantID: "tenant", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1,
		AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1,
		RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1,
		PayloadRef: "payload://request", CreatedAt: time.Now().UTC(),
	}
	if err := instance.Publish(context.Background(), 0, envelope); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	delivered := make(chan struct{})
	go func() {
		_ = instance.Consume(ctx, brokercontract.ConsumerOptions{ConsumerID: "worker-1", Shards: []brokercontract.Shard{0}}, func(context.Context, brokercontract.Delivery) error {
			close(delivered)
			return nil
		})
	}()
	<-delivered
	if result, err := instance.Reclaim(context.Background(), brokercontract.ReclaimOptions{ConsumerID: "worker-2"}); err != nil || len(result) != 0 {
		t.Fatalf("early reclaim=%#v err=%v", result, err)
	}
	time.Sleep(25 * time.Millisecond)
	result, err := instance.Reclaim(context.Background(), brokercontract.ReclaimOptions{ConsumerID: "worker-2"})
	if err != nil || len(result) != 1 || result[0].Envelope != envelope {
		t.Fatalf("reclaim=%#v err=%v", result, err)
	}
	cancel()
}
