// Package integration contains opt-in real-backend vertical slice tests.
package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	agentpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	brokerredis "github.com/liuzengh/trpc-agent-service/trpcservice/broker/redis"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	channeldelivery "github.com/liuzengh/trpc-agent-service/trpcservice/channels/delivery"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/fake"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	configpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/config/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	coordredis "github.com/liuzengh/trpc-agent-service/trpcservice/coordination/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	providerpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/provider/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	relayredis "github.com/liuzengh/trpc-agent-service/trpcservice/relay/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker/mockmodel"
	redisclient "github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

const (
	tenantA = "t_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	tenantB = "t_01ARZ3NDEKTSV4RRFFQ69G5FAY"
	appID   = "app_01ARZ3NDEKTSV4RRFFQ69G5FAX"
)

func TestHTTPPostgreSQLRedisTwoWorkerSlice(t *testing.T) {
	if os.Getenv("TRPC_RUNTIME_TEST") != "1" {
		t.Skip("TRPC_RUNTIME_TEST=1 is required")
	}
	db := runtimeTestDB(t)
	redisAddress := os.Getenv("TRPC_REDIS_TEST_ADDR")
	if redisAddress == "" {
		t.Skip("TRPC_REDIS_TEST_ADDR is not set")
	}
	redisClient := redisclient.NewClient(&redisclient.Options{Addr: redisAddress})
	t.Cleanup(func() { _ = redisClient.Close() })
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}

	environment := fmt.Sprintf("runtime_slice_%d", time.Now().UnixNano())
	streamBroker, err := brokerredis.New(redisClient, brokerredis.Config{
		Environment: environment, Group: "workers", ShardCount: 4, ReadBlock: 20 * time.Millisecond, ReclaimIdle: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	leases, err := coordredis.New(redisClient, environment)
	if err != nil {
		t.Fatal(err)
	}
	businessPublisher, err := relayredis.NewPublisher(redisClient, relayredis.Config{Environment: environment})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupRedisNamespace(redisClient, environment) })

	tenants := tenantpostgres.New(db)
	apps := agentpostgres.New(db)
	configs := configpostgres.New(db, tenants)
	catalog, err := provider.NewCatalog(provider.Schema{Kind: provider.KindModel, Name: "mock", SchemaVersion: 1, AllowedModels: []string{"mock"}, SecretRequirement: "forbidden"})
	if err != nil {
		t.Fatal(err)
	}
	providerProfiles := providerpostgres.New(db, catalog)
	var appTable, revisionTable, configTable bool
	if err := db.QueryRow(`SELECT to_regclass('public.agent_app') IS NOT NULL,
to_regclass('public.agent_app_revision') IS NOT NULL,to_regclass('public.config_snapshot') IS NOT NULL`).
		Scan(&appTable, &revisionTable, &configTable); err != nil {
		t.Fatal(err)
	}
	if !appTable || !revisionTable || !configTable {
		t.Fatalf("migration tables app=%t revision=%t config=%t", appTable, revisionTable, configTable)
	}
	snapshots := []profile.ExecutionProfileSnapshot{
		prepareTenant(t, providerProfiles, tenants, apps, configs, tenantA, "runtime-a", "fake-a"),
		prepareTenant(t, providerProfiles, tenants, apps, configs, tenantB, "runtime-b", "fake-b"),
	}
	profiles := profilememory.NewResolver(snapshots...)
	tasks := gatewaypostgres.NewTaskStore(db)
	sessions := sessionpostgres.New(db)
	inbox := messagingpostgres.New(db)
	payloads := messagingpostgres.NewWithPayloadKey(db, bytes.Repeat([]byte{0x5a}, 32), 1)
	model := &delayedModel{inner: mockmodel.New(), delay: 150 * time.Millisecond}
	agentFactory := serviceagent.Factory{Profiles: profiles, Models: staticModelResolver{model: model}}
	bundles := profilememory.NewBundleManager(func(ctx context.Context, key profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		snapshot, err := profiles.Resolve(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		root, err := agentFactory.Build(ctx, snapshot)
		if err != nil {
			return nil, nil, err
		}
		return &serviceagent.Bundle{AppName: snapshot.AppName, Root: root}, nil, nil
	})
	workersDone := make(chan error, 3)
	usedWorkers := &workerSet{ids: make(map[string]struct{})}
	executionStarted := make(chan executionStart, 32)
	workerStops := make(map[string]context.CancelFunc)
	shards := []broker.Shard{0, 1, 2, 3}
	for _, workerID := range []string{"worker-1", "worker-2", "worker-3"} {
		workerCtx, stopWorker := context.WithCancel(context.Background())
		workerStops[workerID] = stopWorker
		executor := recordingExecutor{
			workerID: workerID, used: usedWorkers, started: executionStarted,
			inner: worker.RunnerExecutor{
				Tasks: tasks, Profiles: profiles, Bundles: bundles, Sessions: sessions,
				Payloads: payloads, Inputs: worker.JSONTextInputDecoder{},
				EncodeEvent: func(_ context.Context, value *event.Event) (string, string, error) {
					return "runner", "event://" + value.ID, nil
				},
			},
		}
		consumer := worker.Consumer{
			WorkerID: workerID, Shards: shards, Broker: streamBroker, Leases: leases,
			Sessions: sessions, Parker: tasks, Statuses: tasks, Executor: executor, LeaseTTL: 2 * time.Second, RetryWait: 5 * time.Millisecond, ReclaimInterval: 10 * time.Millisecond,
			OnDeliveryError: func(_ context.Context, delivery broker.Delivery, err error) {
				t.Logf("worker delivery error request=%s: %v", delivery.Envelope.RequestID, err)
			},
		}
		go func(ctx context.Context) { workersDone <- consumer.Run(ctx) }(workerCtx)
	}
	t.Cleanup(func() {
		for _, stopWorker := range workerStops {
			stopWorker()
		}
		for index := 0; index < 3; index++ {
			select {
			case <-workersDone:
			case <-time.After(2 * time.Second):
			}
		}
	})

	tenantRows := map[string]tenant.Tenant{}
	for _, tenantID := range []string{tenantA, tenantB} {
		row, err := tenants.Get(context.Background(), tenantID)
		if err != nil {
			t.Fatal(err)
		}
		tenantRows[tenantID] = row
	}
	relayCtx, stopRelay := context.WithCancel(context.Background())
	relayDone := make(chan error, 3)
	flakyOutbox := &failFirstPublishedMark{inner: inbox}
	dispatchRelay := relay.DispatchRelay{Outbox: flakyOutbox, Tasks: tasks, Broker: streamBroker, Owner: "runtime-relay", ShardCount: 4, PollInterval: 5 * time.Millisecond, ClaimTTL: 40 * time.Millisecond, ClaimRenewInterval: 10 * time.Millisecond}
	go func() { relayDone <- dispatchRelay.Run(relayCtx) }()
	wakeupQueue, err := relayredis.NewWakeupQueue(redisClient, businessPublisher, relayredis.WakeupQueueConfig{Group: "wakeup-workers", ReadBlock: 20 * time.Millisecond, ReclaimIdle: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	wakeupRelay := relay.WakeupRelay{Outbox: inbox, Wakeups: businessPublisher, Owner: "runtime-wakeup-relay", PollInterval: 5 * time.Millisecond, ClaimTTL: 40 * time.Millisecond, ClaimRenewInterval: 10 * time.Millisecond}
	wakeupDispatcher := relay.WakeupDispatcher{ConsumerID: "runtime-wakeup-dispatcher", Wakeups: wakeupQueue, Store: tasks, Dispatch: streamBroker, ShardCount: 4, ReclaimInterval: 10 * time.Millisecond}
	go func() { relayDone <- wakeupRelay.Run(relayCtx) }()
	go func() { relayDone <- wakeupDispatcher.Run(relayCtx) }()
	t.Cleanup(func() {
		stopRelay()
		for index := 0; index < 3; index++ {
			select {
			case <-relayDone:
			case <-time.After(2 * time.Second):
			}
		}
	})
	dispatcher := gateway.BrokerDispatcher{Tasks: tasks, Bindings: configs}
	handler := fake.NewHandler(dispatcher,
		fake.Binding{Locator: "a", ExternalAccountID: "shared-account", Tenant: tenant.Context{TenantID: tenantA, TenantVersion: tenantRows[tenantA].Version, AgentAppID: appID, SubjectID: "subject", Channel: "fake", TrustedSource: "channel_binding:fake-a"}, IdentityKey: []byte("tenant-a-key")},
		fake.Binding{Locator: "b", ExternalAccountID: "shared-account", Tenant: tenant.Context{TenantID: tenantB, TenantVersion: tenantRows[tenantB].Version, AgentAppID: appID, SubjectID: "subject", Channel: "fake", TrustedSource: "channel_binding:fake-b"}, IdentityKey: []byte("tenant-b-key")},
	)
	handler.Inbox = inbox
	handler.Payloads = payloads
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	first := postMessage(t, server.URL, "a", "same-message", "same-chat")
	var killedWorker string
	select {
	case started := <-executionStarted:
		if started.requestID != first.RequestID {
			t.Fatalf("first execution start=%#v request=%s", started, first.RequestID)
		}
		killedWorker = started.workerID
	case <-time.After(2 * time.Second):
		t.Fatal("first execution did not start")
	}
	duplicate := postMessage(t, server.URL, "a", "same-message", "same-chat")
	crossTenant := postMessage(t, server.URL, "b", "same-message", "same-chat")
	if first.RequestID != duplicate.RequestID || first.RequestID == crossTenant.RequestID {
		t.Fatalf("request isolation first=%s duplicate=%s cross=%s", first.RequestID, duplicate.RequestID, crossTenant.RequestID)
	}
	persistedPayload, err := payloads.GetPayload(context.Background(), tenantA, first.RequestID)
	if err != nil || len(persistedPayload.Content) == 0 || persistedPayload.PayloadRef == "" {
		t.Fatalf("persisted payload=%#v err=%v", persistedPayload, err)
	}
	var storedCiphertext []byte
	if err := db.QueryRow(`SELECT payload_ciphertext FROM inbound_payload WHERE tenant_id=$1 AND request_id=$2`, tenantA, first.RequestID).Scan(&storedCiphertext); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(storedCiphertext, persistedPayload.Content) {
		t.Fatal("payload was stored as plaintext")
	}

	type indexedHandle struct {
		index  int
		handle gateway.ExecutionHandle
	}
	concurrent := make(chan indexedHandle, 4)
	for index := 0; index < 4; index++ {
		index := index
		go func() {
			chat := "same-chat"
			if index >= 2 {
				chat = fmt.Sprintf("parallel-chat-%d", index)
			}
			concurrent <- indexedHandle{index: index, handle: postMessage(t, server.URL, "a", fmt.Sprintf("message-%d", index), chat)}
		}()
	}
	concurrentHandles := make([]gateway.ExecutionHandle, 4)
	for index := 0; index < 4; index++ {
		result := <-concurrent
		concurrentHandles[result.index] = result.handle
	}
	// Keep the first owner alive while other Workers receive later inputs, then
	// kill it. At least one future input must take the real Worker->PostgreSQL
	// park path before input 1 is reclaimed and the session converges in order.
	time.Sleep(20 * time.Millisecond)
	workerStops[killedWorker]()
	handles := append([]gateway.ExecutionHandle{first, crossTenant}, concurrentHandles...)
	for _, handle := range handles {
		status := waitForTerminal(t, tasks, tenantForRequest(handle.RequestID, first.RequestID, crossTenant.RequestID), handle.RequestID)
		if status.Outcome != runtime.OutcomeSucceeded || status.ResultRef == "" {
			t.Fatalf("status=%#v", status)
		}
		if calls := model.inner.Calls(status.Envelope.TenantID, handle.RequestID); calls != 1 {
			t.Fatalf("request=%s model calls=%d", handle.RequestID, calls)
		}
		stored, err := payloads.GetResult(context.Background(), status.Envelope.TenantID, handle.RequestID)
		if err != nil || stored.ResultRef != status.ResultRef || len(stored.Content) == 0 {
			t.Fatalf("request=%s result payload=%#v err=%v", handle.RequestID, stored, err)
		}
		var replies int
		if err := db.QueryRow(`SELECT count(*) FROM outbox WHERE tenant_id=$1 AND aggregate_id=$2 AND kind='reply' AND payload_ref=$3`, status.Envelope.TenantID, handle.RequestID, status.ResultRef).Scan(&replies); err != nil || replies != 1 {
			t.Fatalf("request=%s reply outbox=%d err=%v", handle.RequestID, replies, err)
		}
	}

	// Reply publication is at-least-once. Inject a crash after Redis XADD but
	// before the Outbox mark, then prove the Delivery Ledger suppresses the
	// duplicate provider effect for the repeated delivery key.
	providerAdapter := &deliveryAdapterStub{}
	deliveryService := channeldelivery.Service{Results: payloads, Ledger: inbox, Adapters: deliveryAdapterResolver{adapter: providerAdapter}, Owner: "runtime-adapter"}
	delivering := deliveringReplyPublisher{stream: businessPublisher, delivery: deliveryService}
	flakyReplyOutbox := &failFirstPublishedMark{inner: inbox}
	replyRelay := relay.ReplyRelay{Outbox: flakyReplyOutbox, Results: payloads, Routes: inbox, Replies: delivering,
		Owner: "runtime-reply-relay", ClaimTTL: 40 * time.Millisecond, ClaimRenewInterval: 10 * time.Millisecond}
	if count, err := replyRelay.RunOnce(context.Background()); err == nil || count != len(handles)-1 {
		// One publish succeeds but its injected mark fails; the other rows are
		// marked normally in the same batch.
		t.Fatalf("first reply relay count=%d want=%d err=%v", count, len(handles)-1, err)
	}
	// Allow the database claim lease and the relay's default one-second retry
	// horizon to elapse on slower CI hosts before declaring the replay lost.
	replayDeadline := time.Now().Add(3 * time.Second)
	replayed := 0
	for replayed == 0 && time.Now().Before(replayDeadline) {
		time.Sleep(20 * time.Millisecond)
		count, err := replyRelay.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		replayed += count
	}
	if replayed != 1 {
		t.Fatalf("replayed reply relay count=%d", replayed)
	}
	if calls := providerAdapter.callCount(); calls != len(handles) {
		t.Fatalf("provider effects=%d want=%d", calls, len(handles))
	}
	var publishedReplies int
	if err := db.QueryRow(`SELECT count(*) FROM outbox WHERE kind='reply' AND state='published'`).Scan(&publishedReplies); err != nil || publishedReplies != len(handles) {
		t.Fatalf("published replies=%d want=%d err=%v", publishedReplies, len(handles), err)
	}

	// A node receiving stale/out-of-order TenantControl events must reload the
	// authoritative tenant row and converge directly to the newest version.
	currentTenant, err := tenants.Get(context.Background(), tenantA)
	if err != nil {
		t.Fatal(err)
	}
	meta := tenant.ChangeMetadata{ActorType: "test", ActorID: "slice", ReasonCode: "control", CorrelationID: "control", TraceID: "control"}
	suspended, err := tenants.TransitionStatus(context.Background(), tenant.TransitionStatusInput{TenantID: tenantA, ExpectedVersion: currentTenant.Version, NextStatus: tenant.StatusSuspended, ChangeMetadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	active, err := tenants.TransitionStatus(context.Background(), tenant.TransitionStatusInput{TenantID: tenantA, ExpectedVersion: suspended.Tenant.Version, NextStatus: tenant.StatusActive, ChangeMetadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	controlVersions := []uint64{uint64(suspended.Tenant.Version), uint64(currentTenant.Version), uint64(active.Tenant.Version)}
	for _, version := range controlVersions {
		event := relay.TenantControlEvent{TenantID: tenantA, Kind: "tenant-control", AggregateID: tenantA,
			IdempotencyKey: fmt.Sprintf("control:%d", version), PayloadRef: fmt.Sprintf("tenant://%s/%d", tenantA, version), Version: version}
		if err := businessPublisher.PublishTenantControl(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	controlMessages, err := redisClient.XRange(context.Background(), businessPublisher.TenantControlStream(), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	controlSink := &tenantControlSinkStub{}
	controlConsumer := &relay.MonotonicTenantControlConsumer{Reader: postgresTenantControlReader{tenants: tenants}, Sink: controlSink}
	for _, message := range controlMessages {
		encoded, ok := redisValueString(message.Values["event"])
		if !ok {
			t.Fatalf("control event payload=%#v", message.Values)
		}
		var event relay.TenantControlEvent
		if err := json.Unmarshal([]byte(encoded), &event); err != nil {
			t.Fatal(err)
		}
		if err := controlConsumer.Consume(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	if watermark := controlConsumer.Watermark(tenantA, "tenant-control", tenantA); watermark != uint64(active.Tenant.Version) {
		t.Fatalf("tenant control watermark=%d want=%d", watermark, active.Tenant.Version)
	}
	if applied := controlSink.snapshot(); len(applied) != 1 || applied[0] != uint64(active.Tenant.Version) {
		t.Fatalf("tenant control applied=%v", applied)
	}

	var sessionID string
	var sequences []uint64
	var parkAttempts []int
	rows, err := db.Query(`SELECT session_id,input_seq,park_attempt FROM execution_record
WHERE tenant_id=$1 AND request_id IN ($2,$3,$4) ORDER BY input_seq`, tenantA, first.RequestID, handles[2].RequestID, handles[3].RequestID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var value uint64
		var currentSession string
		var parkAttempt int
		if err := rows.Scan(&currentSession, &value, &parkAttempt); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if sessionID != "" && currentSession != sessionID {
			rows.Close()
			t.Fatalf("same chat produced different sessions: %s %s", sessionID, currentSession)
		}
		sessionID = currentSession
		sequences = append(sequences, value)
		parkAttempts = append(parkAttempts, parkAttempt)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(sequences) != 3 || sequences[1] != sequences[0]+1 || sequences[2] != sequences[1]+1 || parkAttempts[1]+parkAttempts[2] < 1 {
		t.Fatalf("same-session sequences=%v park_attempts=%v", sequences, parkAttempts)
	}
	// Reconcile a PostgreSQL park whose Wakeup Stream entry was lost. The
	// reconciler must reuse the same publish->CAS transition as the consumer.
	lostWakeupSession := "lost-wakeup-session"
	if _, err := db.Exec(`INSERT INTO session_head(tenant_id,agent_app_id,session_id) VALUES($1,$2,$3)`, tenantA, appID, lostWakeupSession); err != nil {
		t.Fatal(err)
	}
	insertFenceExecution(t, db, tenantA, first.RequestID, lostWakeupSession, "lost-wakeup-request", 1)
	if _, err := db.Exec(`UPDATE execution_record SET outcome='pending',park_attempt=1,not_before=now()-interval '1 second',
park_deadline=now()+interval '1 minute',version=version+1 WHERE tenant_id=$1 AND request_id=$2`, tenantA, "lost-wakeup-request"); err != nil {
		t.Fatal(err)
	}
	repairBroker := &recordingBroker{}
	repairer := relay.ParkedInputReconciler{Store: tasks, Dispatch: repairBroker, ShardCount: 4}
	if count, err := repairer.RunOnce(context.Background()); err != nil || count != 1 {
		t.Fatalf("park repair count=%d err=%v", count, err)
	}
	repaired, err := tasks.GetExecution(context.Background(), gateway.ExecutionKey{TenantID: tenantA, RequestID: "lost-wakeup-request"})
	if err != nil || repaired.Outcome != runtime.OutcomeQueued || repairBroker.calls != 1 {
		t.Fatalf("repaired=%#v publish calls=%d err=%v", repaired, repairBroker.calls, err)
	}
	// A cancel request persists intent without manufacturing a terminal outside
	// CommitTurn. Non-cancelled commits are rejected atomically, then the Worker
	// uses its valid fence to commit cancelled and advance the input gate.
	cancelSession := "cancel-session"
	if _, err := db.Exec(`INSERT INTO session_head(tenant_id,agent_app_id,session_id) VALUES($1,$2,$3)`, tenantA, appID, cancelSession); err != nil {
		t.Fatal(err)
	}
	insertFenceExecution(t, db, tenantA, first.RequestID, cancelSession, "cancel-request", 1)
	cancelStatus, err := tasks.GetExecution(context.Background(), gateway.ExecutionKey{TenantID: tenantA, RequestID: "cancel-request"})
	if err != nil {
		t.Fatal(err)
	}
	cancelResult, err := tasks.RequestCancel(context.Background(), gateway.CancelRequest{TenantID: tenantA, RequestID: "cancel-request", ExpectedVersion: cancelStatus.Version,
		ActorID: "runtime-test", ReasonCode: "requested", TraceParent: "00-00000000000000000000000000000001-0000000000000001-01"})
	if err != nil || !cancelResult.Accepted || cancelResult.CancelVersion != 1 {
		t.Fatalf("cancel result=%#v err=%v", cancelResult, err)
	}
	retryResult, retryErr := tasks.RequestCancel(context.Background(), gateway.CancelRequest{TenantID: tenantA, RequestID: "cancel-request", ExpectedVersion: cancelStatus.Version,
		ActorID: "runtime-test", ReasonCode: "requested", TraceParent: "00-00000000000000000000000000000001-0000000000000001-01"})
	if retryErr != nil || !retryResult.Accepted || retryResult.Version != cancelResult.Version || retryResult.CancelVersion != cancelResult.CancelVersion {
		t.Fatalf("cancel retry=%#v err=%v", retryResult, retryErr)
	}
	cancelStatus, err = tasks.GetExecution(context.Background(), gateway.ExecutionKey{TenantID: tenantA, RequestID: "cancel-request"})
	if err != nil || cancelStatus.Outcome != runtime.OutcomeQueued || !cancelStatus.CancelRequested {
		t.Fatalf("cancel intent status=%#v err=%v", cancelStatus, err)
	}
	cancelKey := sessionstore.SessionKey{TenantID: tenantA, AgentAppID: appID, SessionID: cancelSession}
	cancelHead, err := sessions.OpenForRun(context.Background(), sessionstore.OpenForRunRequest{SessionKey: cancelKey, RequestID: "cancel-request", InputSeq: 1, Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.CommitTurn(context.Background(), sessionstore.CommitTurnRequest{SessionKey: cancelKey, RequestID: "cancel-request",
		CommitID: "cancel-request:forged-success", Stage: "terminal", InputSeq: 1, Fence: 1,
		ExpectedVersion: cancelHead.Version, Outcome: runtime.OutcomeSucceeded}); !errors.Is(err, runtime.ErrCancelRequested) {
		t.Fatalf("success after cancel intent=%v", err)
	}
	if err := (worker.RunnerExecutor{Tasks: tasks, Sessions: sessions}).CancelWithLease(context.Background(), cancelStatus.Envelope, 1, nil); err != nil {
		t.Fatal(err)
	}
	cancelStatus, err = tasks.GetExecution(context.Background(), gateway.ExecutionKey{TenantID: tenantA, RequestID: "cancel-request"})
	if err != nil || cancelStatus.Outcome != runtime.OutcomeCancelled {
		t.Fatalf("cancel terminal status=%#v err=%v", cancelStatus, err)
	}
	var cancelNext uint64
	if err := db.QueryRow(`SELECT next_input_seq FROM session_head WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3`, tenantA, appID, cancelSession).Scan(&cancelNext); err != nil || cancelNext != 2 {
		t.Fatalf("cancel next input=%d err=%v", cancelNext, err)
	}
	// A parked future input must become blocked and emit an audit fact even if
	// no predecessor ever commits and therefore no Wakeup Outbox is produced.
	blockedSession := "park-deadline-session"
	if _, err := db.Exec(`INSERT INTO session_head(tenant_id,agent_app_id,session_id) VALUES($1,$2,$3)`, tenantA, appID, blockedSession); err != nil {
		t.Fatal(err)
	}
	insertFenceExecution(t, db, tenantA, first.RequestID, blockedSession, "park-deadline-request", 2)
	deadlineTasks, err := gatewaypostgres.NewTaskStoreWithParkPolicy(db, gateway.ParkPolicy{BaseDelay: time.Second, MaxDelay: time.Second, Deadline: time.Second, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	parked, err := deadlineTasks.ParkInput(context.Background(), gateway.ParkRequest{TenantID: tenantA, RequestID: "park-deadline-request", InputSeq: 2})
	if err != nil || parked.Disposition != gateway.ParkedInput || parked.Attempt != 1 {
		t.Fatalf("deadline park=%#v err=%v", parked, err)
	}
	duplicatePark, err := deadlineTasks.ParkInput(context.Background(), gateway.ParkRequest{TenantID: tenantA, RequestID: "park-deadline-request", InputSeq: 2})
	if err != nil || duplicatePark.Attempt != parked.Attempt || duplicatePark.Version != parked.Version {
		t.Fatalf("duplicate park=%#v first=%#v err=%v", duplicatePark, parked, err)
	}
	time.Sleep(1100 * time.Millisecond)
	blocked, err := deadlineTasks.InspectWakeup(context.Background(), gateway.ExecutionKey{TenantID: tenantA, RequestID: "park-deadline-request"})
	if err != nil || !blocked.Blocked || blocked.Execution.Outcome != runtime.OutcomeBlocked {
		t.Fatalf("blocked candidate=%#v err=%v", blocked, err)
	}
	var blockedAudit int
	if err := db.QueryRow(`SELECT count(*) FROM outbox WHERE tenant_id=$1 AND kind='audit' AND idempotency_key=$2`, tenantA, "park-blocked:park-deadline-request").Scan(&blockedAudit); err != nil || blockedAudit != 1 {
		t.Fatalf("blocked audit=%d err=%v", blockedAudit, err)
	}
	// Lose both Redis coordination keys, then prove the durable PostgreSQL
	// watermark calibrates the recreated counter before a new owner acquires.
	coordinationKey := coordination.SessionKey{TenantID: tenantA, AgentAppID: appID, SessionID: sessionID}
	persistedFence, err := sessions.ReadLastFence(context.Background(), sessionstore.SessionKey(coordinationKey))
	if err != nil || persistedFence == 0 {
		t.Fatalf("persisted fence=%d err=%v", persistedFence, err)
	}
	digest := sha256.Sum256([]byte(coordinationKey.TenantID + "\x00" + coordinationKey.AgentAppID + "\x00" + coordinationKey.SessionID))
	prefix := fmt.Sprintf("trpc:%s:{%s}", environment, hex.EncodeToString(digest[:]))
	if err := redisClient.Del(context.Background(), prefix+":lease", prefix+":fence").Err(); err != nil {
		t.Fatal(err)
	}
	if err := leases.EnsureFenceAtLeast(context.Background(), coordinationKey, persistedFence); err != nil {
		t.Fatal(err)
	}
	recoveredLease, err := leases.Acquire(context.Background(), coordinationKey, "recovery-worker", time.Second)
	if err != nil || recoveredLease.Fence <= persistedFence {
		t.Fatalf("recovered lease=%#v persisted=%d err=%v", recoveredLease, persistedFence, err)
	}
	nextFenceInput := sequences[len(sequences)-1] + 1
	insertFenceExecution(t, db, tenantA, first.RequestID, sessionID, "fence-recovery-new", nextFenceInput)
	insertFenceExecution(t, db, tenantA, first.RequestID, sessionID, "fence-recovery-stale", nextFenceInput+1)
	fenceSessionKey := sessionstore.SessionKey(coordinationKey)
	head, err := sessions.OpenForRun(context.Background(), sessionstore.OpenForRunRequest{SessionKey: fenceSessionKey,
		RequestID: "fence-recovery-new", InputSeq: nextFenceInput, Fence: recoveredLease.Fence})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.CommitTurn(context.Background(), sessionstore.CommitTurnRequest{SessionKey: fenceSessionKey,
		RequestID: "fence-recovery-new", CommitID: "fence-recovery-new:terminal:0", Stage: "terminal",
		InputSeq: nextFenceInput, Fence: recoveredLease.Fence, ExpectedVersion: head.Version, Outcome: runtime.OutcomeDenied}); err != nil {
		t.Fatal(err)
	}
	if terminalPark, err := tasks.ParkInput(context.Background(), gateway.ParkRequest{TenantID: tenantA, RequestID: "fence-recovery-new", InputSeq: nextFenceInput}); err != nil || terminalPark.Disposition != gateway.ParkInputTerminal {
		t.Fatalf("terminal park=%#v err=%v", terminalPark, err)
	}
	if _, err := sessions.OpenForRun(context.Background(), sessionstore.OpenForRunRequest{SessionKey: fenceSessionKey,
		RequestID: "fence-recovery-stale", InputSeq: nextFenceInput + 1, Fence: persistedFence}); !errors.Is(err, runtime.ErrStaleFence) {
		t.Fatalf("old fence after recovery commit=%v", err)
	}
	if err := leases.Release(context.Background(), recoveredLease); err != nil {
		t.Fatal(err)
	}
	var durableEvents int
	if err := db.QueryRow(`SELECT count(*) FROM session_event WHERE tenant_id=$1 AND session_id=$2 AND event_payload IS NOT NULL`, tenantA, sessionID).Scan(&durableEvents); err != nil || durableEvents == 0 {
		t.Fatalf("durable session events=%d err=%v", durableEvents, err)
	}
	if usedWorkers.count() < 2 {
		t.Fatalf("expected multiple workers to execute, used=%v", usedWorkers.snapshot())
	}
	if killedWorker == "" || flakyOutbox.failures() != 1 {
		t.Fatalf("killed worker=%q mark failures=%d", killedWorker, flakyOutbox.failures())
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		var unpublished int
		err := db.QueryRow(`SELECT count(*) FROM outbox WHERE kind='dispatch' AND state<>'published'`).Scan(&unpublished)
		if err == nil && unpublished == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("unpublished dispatch=%d err=%v", unpublished, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type recordingBroker struct {
	mu        sync.Mutex
	calls     int
	published runtime.ExecutionEnvelope
}

func (b *recordingBroker) Publish(_ context.Context, _ broker.Shard, envelope runtime.ExecutionEnvelope) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	b.published = envelope
	return nil
}
func (*recordingBroker) Consume(context.Context, broker.ConsumerOptions, func(context.Context, broker.Delivery) error) error {
	return runtime.ErrCapabilityUnsupported
}
func (*recordingBroker) Ack(context.Context, broker.Delivery) error {
	return runtime.ErrCapabilityUnsupported
}
func (*recordingBroker) Reclaim(context.Context, broker.ReclaimOptions) ([]broker.Delivery, error) {
	return nil, runtime.ErrCapabilityUnsupported
}

func insertFenceExecution(t *testing.T, db *sql.DB, tenantID, templateRequestID, sessionID, requestID string, inputSeq uint64) {
	t.Helper()
	payloadRef := "inbound://" + tenantID + "/" + requestID
	_, err := db.Exec(`INSERT INTO inbox(tenant_id,channel,external_account_id,external_message_id,request_id,
agent_app_id,session_id,input_seq,state,payload_ref,payload_digest,key_version)
VALUES($1,'fake','fence-recovery',$2,$2,$3,$4,$5,'dispatch_ready',$6,$7,1)`,
		tenantID, requestID, appID, sessionID, inputSeq, payloadRef, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO execution_record(tenant_id,request_id,tenant_version,agent_app_id,agent_app_version,
agent_app_revision,agent_content_digest,config_version,policy_version,session_id,user_id,channel,input_seq,payload_ref,traceparent)
SELECT tenant_id,$3,tenant_version,agent_app_id,agent_app_version,agent_app_revision,agent_content_digest,
config_version,policy_version,$4,user_id,channel,$5,$6,traceparent
FROM execution_record WHERE tenant_id=$1 AND request_id=$2`, tenantID, templateRequestID, requestID, sessionID, inputSeq, payloadRef)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE session_head SET last_allocated_input_seq=GREATEST(last_allocated_input_seq,$4)
WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3`, tenantID, appID, sessionID, inputSeq); err != nil {
		t.Fatal(err)
	}
}

func prepareTenant(t *testing.T, providerProfiles *providerpostgres.Repository, tenants *tenantpostgres.Repository, apps *agentpostgres.Repository, configs *configpostgres.Repository, tenantID, tenantKey, bindingID string) profile.ExecutionProfileSnapshot {
	t.Helper()
	ctx := context.Background()
	meta := tenant.ChangeMetadata{ActorType: "test", ActorID: "slice", ReasonCode: "setup", CorrelationID: tenantKey, TraceID: tenantKey}
	created, err := tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: tenantID, TenantKey: tenantKey, DisplayName: tenantKey}, ChangeMetadata: meta})
	if err != nil {
		t.Fatalf("create tenant %s: %v", tenantID, err)
	}
	if _, err := providerProfiles.PublishModel(ctx, provider.ModelProfileSnapshot{TenantID: tenantID, ProfileID: "mock", ProfileKey: "runtime-model", DisplayName: "Runtime Model", Status: "active", SchemaVersion: 1, Provider: "mock", Model: "mock", Version: 1}); err != nil {
		t.Fatal(err)
	}
	appMeta := agentapp.ChangeMetadata{ActorType: "test", ActorID: "slice", Reason: "setup", CorrelationID: tenantKey, TraceID: tenantKey}
	app, err := apps.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: tenantID, AgentAppID: appID, AgentAppKey: "assistant", DisplayName: "Assistant"}, ChangeMetadata: appMeta})
	if err != nil {
		t.Fatalf("create app %s: %v", tenantID, err)
	}
	draft, err := apps.CreateDraft(ctx, agentapp.CreateDraftInput{
		TenantID: tenantID, AgentAppID: appID, ExpectedAppVersion: app.Version,
		Revision: agentapp.Revision{
			AgentKind: "llm", Instruction: "help", ModelProfileID: "mock", ModelProfileVersion: 1,
		}, ChangeMetadata: appMeta,
	})
	if err != nil {
		t.Fatalf("create draft %s: %v", tenantID, err)
	}
	if draft.GenerationConfig == nil || draft.RuntimePolicy == nil {
		t.Fatalf("draft JSON objects were not normalized: %#v", draft)
	}
	publishedApp, err := apps.Publish(ctx, agentapp.PublishInput{TenantID: tenantID, AgentAppID: appID, Revision: draft.Revision, ExpectedAppVersion: 2, ExpectedDraftVersion: 1, ChangeMetadata: appMeta})
	if err != nil {
		t.Fatalf("publish app %s: %v", tenantID, err)
	}
	publishedConfig, err := configs.Publish(ctx, config.PublishInput{
		TenantID: tenantID, ExpectedTenantVersion: created.Version, Metadata: meta,
		Payload: config.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: appID, PolicyVersion: 1, ChannelBindings: []config.ChannelBinding{{
			BindingID: bindingID, Channel: "fake", ExternalAccountID: "shared-account", AgentAppID: appID,
			SecretRef: secrets.SecretRef{Ref: "secret://fake", Version: 1},
		}}},
	})
	if err != nil {
		t.Fatalf("publish config %s: %v", tenantID, err)
	}
	key := profile.ExecutionProfileKey{
		TenantID: tenantID, AgentAppID: appID, AgentAppRevision: publishedApp.Revision.Revision,
		ContentDigest: publishedApp.Revision.ContentDigest, ConfigVersion: publishedConfig.Snapshot.ConfigVersion, PolicyVersion: 1,
	}
	return profile.ExecutionProfileSnapshot{
		Key: key, TenantVersion: publishedConfig.Tenant.Version, AgentAppVersion: publishedApp.App.Version,
		ContentDigest: key.ContentDigest, AppName: tenantID + "/" + appID,
		AgentKind: agentapp.AgentKindLLM, Instruction: "help", ModelProfileRef: profile.VersionedRef{ID: "mock", Version: 1},
	}
}

func runtimeTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var name string
	var major int
	if err := db.QueryRow(`SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&name, &major); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(name, "trpc_agent_service_test_") || major != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", name, major)
	}
	return db
}

func postMessage(t *testing.T, baseURL, binding, messageID, chatID string) gateway.ExecutionHandle {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"external_message_id":%q,"external_user_id":"same-user","external_chat_id":%q,"text":"hello"}`, messageID, chatID))
	response, err := http.Post(baseURL+"/bindings/"+binding, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Errorf("post: %v", err)
		return gateway.ExecutionHandle{}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Errorf("post status=%d", response.StatusCode)
		return gateway.ExecutionHandle{}
	}
	var handle gateway.ExecutionHandle
	if err := json.NewDecoder(response.Body).Decode(&handle); err != nil {
		t.Errorf("decode handle: %v", err)
	}
	return handle
}

func waitForTerminal(t *testing.T, tasks *gatewaypostgres.TaskStore, tenantID, requestID string) gateway.ExecutionStatus {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		status, err := tasks.GetExecution(context.Background(), gateway.ExecutionKey{TenantID: tenantID, RequestID: requestID})
		if err == nil && status.Outcome == runtime.OutcomeSucceeded {
			return status
		}
		if err != nil && !errors.Is(err, runtime.ErrNotFound) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("request %s did not become terminal", requestID)
	return gateway.ExecutionStatus{}
}

func tenantForRequest(requestID, first, cross string) string {
	if requestID == cross && cross != first {
		return tenantB
	}
	return tenantA
}

func cleanupRedisNamespace(client *redisclient.Client, environment string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, pattern := range []string{"trpc:{" + environment + "}:*", "trpc:" + environment + ":*"} {
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				_ = client.Del(ctx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
}

type delayedModel struct {
	inner *mockmodel.Model
	delay time.Duration
}

func (m *delayedModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	timer := time.NewTimer(m.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return m.inner.GenerateContent(ctx, request)
	}
}

func (m *delayedModel) Info() model.Info { return m.inner.Info() }

type staticModelResolver struct{ model model.Model }

func (r staticModelResolver) ResolveModel(context.Context, string, profile.VersionedRef) (model.Model, error) {
	return r.model, nil
}

type workerSet struct {
	mu  sync.Mutex
	ids map[string]struct{}
}

func (s *workerSet) add(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids[id] = struct{}{}
}

func (s *workerSet) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ids)
}

func (s *workerSet) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]string, 0, len(s.ids))
	for id := range s.ids {
		result = append(result, id)
	}
	return result
}

type recordingExecutor struct {
	workerID string
	used     *workerSet
	started  chan<- executionStart
	inner    worker.LeaseExecutor
}

type executionStart struct{ workerID, requestID string }

func (e recordingExecutor) ExecuteWithLease(ctx context.Context, envelope runtime.ExecutionEnvelope, fence uint64, beforeCommit func(context.Context) error) error {
	e.used.add(e.workerID)
	if e.started != nil {
		e.started <- executionStart{workerID: e.workerID, requestID: envelope.RequestID}
	}
	return e.inner.ExecuteWithLease(ctx, envelope, fence, beforeCommit)
}

func (e recordingExecutor) CancelWithLease(ctx context.Context, envelope runtime.ExecutionEnvelope, fence uint64, beforeCommit func(context.Context) error) error {
	canceller, ok := e.inner.(worker.CancellationExecutor)
	if !ok {
		return runtime.ErrCapabilityUnsupported
	}
	return canceller.CancelWithLease(ctx, envelope, fence, beforeCommit)
}

type failFirstPublishedMark struct {
	inner  messaging.OutboxStore
	mu     sync.Mutex
	failed bool
}

func (s *failFirstPublishedMark) ClaimOutbox(ctx context.Context, kind string, limit int, owner string, until time.Time) ([]messaging.OutboxRecord, error) {
	return s.inner.ClaimOutbox(ctx, kind, limit, owner, until)
}
func (s *failFirstPublishedMark) RenewOutboxClaim(ctx context.Context, tenantID, outboxID string, version uint64, owner string, until time.Time) (uint64, error) {
	return s.inner.RenewOutboxClaim(ctx, tenantID, outboxID, version, owner, until)
}
func (s *failFirstPublishedMark) MarkPublished(ctx context.Context, tenantID, outboxID string, version uint64) error {
	s.mu.Lock()
	if !s.failed {
		s.failed = true
		s.mu.Unlock()
		return errors.New("injected crash after publish before mark")
	}
	s.mu.Unlock()
	return s.inner.MarkPublished(ctx, tenantID, outboxID, version)
}
func (s *failFirstPublishedMark) MarkRetry(ctx context.Context, tenantID, outboxID string, version uint64, next time.Time) error {
	return s.inner.MarkRetry(ctx, tenantID, outboxID, version, next)
}
func (s *failFirstPublishedMark) failures() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return 1
	}
	return 0
}

type deliveringReplyPublisher struct {
	stream   channel.ReplyPublisher
	delivery channeldelivery.Service
}

func (p deliveringReplyPublisher) PublishReply(ctx context.Context, destination channel.ReplyDestination, event channel.ReplyEvent) error {
	if err := p.stream.PublishReply(ctx, destination, event); err != nil {
		return err
	}
	return p.delivery.Deliver(ctx, event)
}

type deliveryAdapterResolver struct{ adapter channel.Adapter }

func (r deliveryAdapterResolver) ResolveAdapter(context.Context, string, string) (channel.Adapter, error) {
	return r.adapter, nil
}

type deliveryAdapterStub struct {
	mu    sync.Mutex
	calls int
}

func (*deliveryAdapterStub) ID() string                                            { return "fake-delivery" }
func (*deliveryAdapterStub) Run(context.Context) error                             { return nil }
func (*deliveryAdapterStub) Verify(context.Context, channel.CallbackRequest) error { return nil }
func (*deliveryAdapterStub) Decode(context.Context, channel.CallbackRequest) ([]channel.ProviderEvent, error) {
	return nil, nil
}
func (a *deliveryAdapterStub) Deliver(_ context.Context, request channel.DeliveryRequest) (channel.DeliveryResult, error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	return channel.DeliveryResult{ProviderMessageID: "provider:" + request.Event.DeliveryKey, Delivered: true}, nil
}
func (*deliveryAdapterStub) Capabilities() channel.Capabilities {
	return channel.Capabilities{Text: true}
}
func (a *deliveryAdapterStub) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

type postgresTenantControlReader struct{ tenants *tenantpostgres.Repository }

func (r postgresTenantControlReader) ReadTenantControl(ctx context.Context, event relay.TenantControlEvent) (relay.TenantControlState, error) {
	current, err := r.tenants.Get(ctx, event.TenantID)
	if err != nil {
		return relay.TenantControlState{}, err
	}
	return relay.TenantControlState{TenantID: current.TenantID, Kind: event.Kind, AggregateID: event.AggregateID,
		Status: string(current.Status), Version: uint64(current.Version)}, nil
}

type tenantControlSinkStub struct {
	mu       sync.Mutex
	versions []uint64
}

func (s *tenantControlSinkStub) ApplyTenantControl(_ context.Context, state relay.TenantControlState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versions = append(s.versions, state.Version)
	return nil
}

func (s *tenantControlSinkStub) snapshot() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.versions...)
}

func redisValueString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}
