package worker

import (
	"context"
	"testing"
	"time"

	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker/mockmodel"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type staticModelResolver struct{ model model.Model }

func (r staticModelResolver) ResolveModel(context.Context, string, profile.VersionedRef) (model.Model, error) {
	return r.model, nil
}

func TestRunnerExecutorUsesUpstreamRunnerAndKeepsRedeliveryIdempotent(t *testing.T) {
	envelope := runtime.ExecutionEnvelope{
		SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1,
		AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1,
		RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1,
		PayloadRef: "payload://request", CreatedAt: time.Now().UTC(),
	}
	key := profile.ExecutionProfileKey{
		TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID,
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
		ContentDigest: "payload-digest", Content: []byte(`{"text":"hello"}`), KeyVersion: 1,
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
}
