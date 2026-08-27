package preprocess_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	preprocessmemory "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

func TestWorkerTransitionsReadyAndRecoversDispatch(t *testing.T) {
	clock := time.Now().UTC().Truncate(time.Microsecond)
	store := preprocessmemory.New()
	payloads := messagingmemory.New()
	inbox, _, err := store.ClaimInboxAndSchedule(context.Background(), claim("message-1"))
	if err != nil {
		t.Fatal(err)
	}
	content, _ := json.Marshal(preprocess.NormalizedText{ExternalMessageID: "message-1", ExternalUserID: "external-user", Text: "hello"})
	sum := sha256.Sum256(content)
	if err := payloads.PutPayload(context.Background(), messaging.PayloadRecord{TenantID: inbox.TenantID, RequestID: inbox.RequestID,
		PayloadRef: inbox.PayloadRef, ContentDigest: hex.EncodeToString(sum[:]), Content: content, KeyVersion: 1}); err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingDispatcher{fail: true}
	worker := preprocess.Worker{Store: store, Payloads: payloads, Dispatcher: dispatcher, Owner: "worker", Now: func() time.Time { return clock }}
	if _, err := worker.RunOnce(context.Background(), 10); err == nil {
		t.Fatal("expected first dispatch failure")
	}
	ready, err := store.ListReadyForDispatch(context.Background(), 10)
	if err != nil || len(ready) != 1 {
		t.Fatalf("ready=%#v err=%v", ready, err)
	}
	dispatcher.fail = false
	clock = clock.Add(31 * time.Second)
	if processed, err := worker.RunOnce(context.Background(), 10); err != nil || processed != 0 || len(dispatcher.requests) != 2 {
		t.Fatalf("processed=%d requests=%d err=%v", processed, len(dispatcher.requests), err)
	}
	ready, _ = store.ListReadyForDispatch(context.Background(), 10)
	if len(ready) != 0 {
		t.Fatalf("dispatched job remained ready=%#v", ready)
	}
}

func TestExpiredLeaseIsReclaimedAndCASProtected(t *testing.T) {
	store := preprocessmemory.New()
	if _, _, err := store.ClaimInboxAndSchedule(context.Background(), claim("message-2")); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	first, _ := store.ClaimJobs(context.Background(), preprocess.ClaimOptions{Owner: "one", Now: now, TTL: time.Second, Limit: 1})
	second, _ := store.ClaimJobs(context.Background(), preprocess.ClaimOptions{Owner: "two", Now: now.Add(2 * time.Second), TTL: time.Second, Limit: 1})
	if len(first) != 1 || len(second) != 1 || second[0].Attempt != 2 {
		t.Fatalf("first=%#v second=%#v", first, second)
	}
	if _, err := store.FinishReady(context.Background(), first[0]); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("stale lease finish=%v", err)
	}
	if _, err := store.FinishReady(context.Background(), second[0]); err != nil {
		t.Fatal(err)
	}
}

func TestDuplicateClaimIgnoresTraceContextChanges(t *testing.T) {
	store := preprocessmemory.New()
	original := claim("message-trace-retry")
	original.TraceParent = "first-trace"
	inbox, job, err := store.ClaimInboxAndSchedule(context.Background(), original)
	if err != nil {
		t.Fatal(err)
	}
	retry := original
	retry.TraceParent = "retry-trace"
	retriedInbox, retriedJob, err := store.ClaimInboxAndSchedule(context.Background(), retry)
	if err != nil || retriedInbox.RequestID != inbox.RequestID || retriedJob.JobID != job.JobID || retriedJob.TraceParent != "first-trace" {
		t.Fatalf("retried inbox=%#v job=%#v err=%v", retriedInbox, retriedJob, err)
	}
}

func claim(messageID string) preprocess.ClaimRequest {
	key := messaging.InboxKey{TenantID: "tenant", Channel: "fake", ExternalAccountID: "account", ExternalMessageID: messageID}
	return preprocess.ClaimRequest{Inbox: messaging.ClaimInboxRequest{InboxKey: key, AgentAppID: "app", SessionID: "session",
		ExternalUserID: "external-user", PayloadDigest: string(make([]byte, 64)), KeyVersion: 1,
		InitialState: messaging.InboxPreprocessPending}, TenantVersion: 1, UserID: "user", TraceParent: "trace"}
}

type recordingDispatcher struct {
	fail     bool
	requests []gateway.DispatchRequest
}

func (d *recordingDispatcher) Dispatch(_ context.Context, request gateway.DispatchRequest) (gateway.ExecutionHandle, error) {
	d.requests = append(d.requests, request)
	if d.fail {
		return gateway.ExecutionHandle{}, errors.New("broker unavailable")
	}
	return gateway.ExecutionHandle{RequestID: request.RequestID, Status: "accepted"}, nil
}
