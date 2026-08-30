package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	artifactmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker/mockmodel"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type staticModelResolver struct{ model model.Model }

func (r staticModelResolver) ResolveModel(context.Context, string, profile.VersionedRef) (model.Model, error) {
	return r.model, nil
}

type cancelledTaskStub struct{ taskStub }

func (s cancelledTaskStub) GetExecution(context.Context, gateway.ExecutionKey) (gateway.ExecutionStatus, error) {
	return gateway.ExecutionStatus{Envelope: s.envelope, Outcome: runtime.OutcomeRunning, Version: 2, CancelRequested: true, CancelVersion: 1}, nil
}

func TestRunnerExecutorRejectsWorkAndCommitsDurableCancellation(t *testing.T) {
	envelope := runtime.ExecutionEnvelope{
		SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1,
		AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1,
		RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1,
		PayloadRef: "payload://request", CreatedAt: time.Now().UTC(),
	}
	sessions := sessionmemory.New()
	executor := RunnerExecutor{Tasks: cancelledTaskStub{taskStub{envelope: envelope}}, Sessions: sessions,
		Profiles: profilememory.NewResolver(), Bundles: profilememory.NewBundleManager(nil), Payloads: messagingmemory.New(),
		Inputs: JSONTextInputDecoder{}, EncodeEvent: func(context.Context, *event.Event) (string, string, error) { return "event", "event://cancel", nil }}
	if err := executor.ExecuteWithLease(context.Background(), envelope, 7, nil); !errors.Is(err, runtime.ErrCancelRequested) {
		t.Fatalf("execute after cancel=%v", err)
	}
	if err := executor.CancelWithLease(context.Background(), envelope, 7, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	terminal, err := sessions.GetTerminalByInputSeq(context.Background(), sessionstore.TerminalKey{
		SessionKey: sessionstore.SessionKey{TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID, SessionID: envelope.SessionID}, InputSeq: 1,
	})
	if err != nil || terminal.Outcome != runtime.OutcomeCancelled {
		t.Fatalf("terminal=%#v err=%v", terminal, err)
	}
	_, outbox, _ := sessions.SnapshotEffects(sessionstore.SessionKey{TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID, SessionID: envelope.SessionID})
	if len(outbox) != 1 || outbox[0].Kind != "audit" || outbox[0].IdempotencyKey != "cancel-terminal:request" {
		t.Fatalf("outbox=%#v", outbox)
	}
}

func TestDrainRunnerEventsUnblocksProducer(t *testing.T) {
	events := make(chan *event.Event)
	produced := make(chan struct{})
	go func() {
		events <- &event.Event{}
		events <- &event.Event{}
		close(events)
		close(produced)
	}()
	drainRunnerEvents(events, time.Second)
	select {
	case <-produced:
	case <-time.After(time.Second):
		t.Fatal("event producer remained blocked")
	}
}

type blockingRunner struct{ release chan struct{} }

func (r blockingRunner) Close() error { <-r.release; return nil }

func TestCloseRunnerHonorsBoundedTimeout(t *testing.T) {
	runner := blockingRunner{release: make(chan struct{})}
	started := time.Now()
	closeRunner(runner, nil, 10*time.Millisecond)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("closeRunner blocked for %s", elapsed)
	}
	close(runner.release)
}

func TestRunnerExecutorUsesUpstreamRunnerAndKeepsRedeliveryIdempotent(t *testing.T) {
	envelope := runtime.ExecutionEnvelope{
		SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1,
		AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1,
		RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1,
		PayloadRef: "payload://request", CreatedAt: time.Now().UTC(),
	}
	key := profile.ExecutionProfileKey{
		TenantID: envelope.TenantID, TenantVersion: envelope.TenantVersion,
		AgentAppID: envelope.AgentAppID, AgentAppVersion: envelope.AgentAppVersion,
		AgentAppRevision: envelope.AgentAppRevision, ContentDigest: envelope.AgentContentDigest,
		ConfigVersion: envelope.ConfigVersion, PolicyVersion: envelope.PolicyVersion,
	}
	snapshot := profile.ExecutionProfileSnapshot{
		Key: key, TenantVersion: envelope.TenantVersion, AgentAppVersion: envelope.AgentAppVersion,
		ContentDigest: envelope.AgentContentDigest, AppName: "tenant-a/app",
		AgentKind: agentapp.AgentKindLLM, Instruction: "answer", ModelProfileRef: profile.VersionedRef{ID: "mock", Version: 1},
	}
	profiles := profilememory.NewResolver(snapshot)
	mock := mockmodel.New()
	factory := serviceagent.Factory{Profiles: profiles, Models: staticModelResolver{model: mock}}
	bundles := profilememory.NewBundleManager(func(ctx context.Context, requested profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		resolved, err := profiles.Resolve(ctx, requested)
		if err != nil {
			return nil, nil, err
		}
		root, err := factory.Build(ctx, resolved)
		if err != nil {
			return nil, nil, err
		}
		return &serviceagent.Bundle{AppName: resolved.AppName, Root: root}, nil, nil
	})
	payloads := messagingmemory.New()
	if err := payloads.PutPayload(context.Background(), messaging.PayloadRecord{
		TenantID: envelope.TenantID, RequestID: envelope.RequestID, PayloadRef: envelope.PayloadRef,
		ContentDigest: "payload-digest", Content: []byte(`{"text":"hello"}`), KeyVersion: 7,
	}); err != nil {
		t.Fatal(err)
	}
	sessions := sessionmemory.New()
	executor := RunnerExecutor{
		Tasks: taskStub{envelope: envelope}, Profiles: profiles, Bundles: bundles,
		Sessions: sessions, Payloads: payloads,
		Inputs: JSONTextInputDecoder{}, EncodeEvent: func(_ context.Context, value *event.Event) (string, string, error) {
			return "runner", "event://" + value.ID, nil
		},
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := executor.ExecuteWithLease(context.Background(), envelope, 1, nil); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
	if calls := mock.Calls(envelope.TenantID, envelope.RequestID); calls != 1 {
		t.Fatalf("model calls=%d", calls)
	}
	result, err := payloads.GetResult(context.Background(), envelope.TenantID, envelope.RequestID)
	if err != nil || result.KeyVersion != 7 {
		t.Fatalf("result key version=%d err=%v", result.KeyVersion, err)
	}
}

func TestJSONTextInputDecoderHydratesOnlyTenantScopedArtifactRefs(t *testing.T) {
	content := []byte("\x89PNG\r\n\x1a\n")
	contentSum := sha256.Sum256(content)
	sourceSum := sha256.Sum256([]byte("source"))
	id, ref, err := artifact.StableIdentity("tenant-a", "request", 0, hex.EncodeToString(sourceSum[:]))
	if err != nil {
		t.Fatal(err)
	}
	artifacts := artifactmemory.New()
	if _, err := artifacts.PutArtifact(context.Background(), artifact.Record{TenantID: "tenant-a", RequestID: "request", ArtifactID: id, ArtifactRef: ref,
		Ordinal: 0, SourceDigest: hex.EncodeToString(sourceSum[:]), ContentDigest: hex.EncodeToString(contentSum[:]), MediaType: "image/png", Kind: "image",
		Content: content, MalwareScanVersion: "av-1", DLPVersion: "dlp-1"}); err != nil {
		t.Fatal(err)
	}
	prepared, _ := json.Marshal(preprocess.PreparedInput{ExternalMessageID: "message", ExternalUserID: "user", MessageType: "image",
		Media: []preprocess.PreparedMedia{{ArtifactID: id, ArtifactRef: ref, Kind: "image", MediaType: "image/png", ContentDigest: hex.EncodeToString(contentSum[:]), Size: int64(len(content))}}})
	envelope := runtime.ExecutionEnvelope{TenantID: "tenant-a", RequestID: "request"}
	message, err := (JSONTextInputDecoder{}).DecodeInput(context.Background(), envelope, prepared)
	if err != nil || len(message.ContentParts) != 1 || message.ContentParts[0].Image == nil || len(message.ContentParts[0].Image.Data) != 0 {
		t.Fatalf("message=%#v err=%v", message, err)
	}
	executor := RunnerExecutor{Artifacts: artifacts}
	if err := executor.hydrateArtifacts(context.Background(), envelope, &message); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message.ContentParts[0].Image.Data, content) || message.ContentParts[0].Image.URL != "" {
		t.Fatalf("hydrated message=%#v", message)
	}
}
