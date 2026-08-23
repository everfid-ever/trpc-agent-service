package inmemory

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

func TestConcurrentClaimReturnsOneStableRequest(t *testing.T) {
	store := New()
	key := messaging.InboxKey{TenantID: "t", Channel: "fake", ExternalAccountID: "account", ExternalMessageID: "message"}
	const callers = 50
	results := make(chan string, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			record, err := store.ClaimInbox(context.Background(), messaging.ClaimInboxRequest{InboxKey: key, RequestID: "request", AgentAppID: "app", SessionID: "session", PayloadRef: "payload", PayloadDigest: "digest", KeyVersion: 1, InitialState: messaging.InboxDispatchPending})
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			results <- record.RequestID
		}()
	}
	wg.Wait()
	close(results)
	for requestID := range results {
		if requestID != "request" {
			t.Fatalf("request=%q", requestID)
		}
	}
}

func TestClaimRejectsDigestCollision(t *testing.T) {
	store := New()
	key := messaging.InboxKey{TenantID: "t", Channel: "fake", ExternalAccountID: "account", ExternalMessageID: "message"}
	base := messaging.ClaimInboxRequest{InboxKey: key, RequestID: "r1", AgentAppID: "app", PayloadRef: "payload", PayloadDigest: "digest-1", KeyVersion: 1, InitialState: messaging.InboxPreprocessPending}
	if _, err := store.ClaimInbox(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	base.RequestID, base.PayloadDigest = "r2", "digest-2"
	if _, err := store.ClaimInbox(context.Background(), base); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("got %v", err)
	}
}
