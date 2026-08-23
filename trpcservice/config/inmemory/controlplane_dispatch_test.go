package inmemory

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaymemory "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker/mockmodel"
)

func TestPublishedControlPlaneDrivesLocalDispatcher(t *testing.T) {
	ctx, _, apps, configs := setup(t)
	published, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: 1, Payload: payload(7), Metadata: metadata("publish")})
	if err != nil {
		t.Fatal(err)
	}
	app, err := apps.Get(ctx, "tenant-a", "app")
	if err != nil {
		t.Fatal(err)
	}
	revision, err := apps.GetRevision(ctx, "tenant-a", "app", app.CurrentRevision)
	if err != nil {
		t.Fatal(err)
	}
	key := profile.ExecutionProfileKey{TenantID: "tenant-a", AgentAppID: "app", AgentAppRevision: revision.Revision, ContentDigest: revision.ContentDigest, ConfigVersion: published.Snapshot.ConfigVersion, PolicyVersion: published.Snapshot.Payload.PolicyVersion}
	profiles := profilememory.NewResolver(profile.ExecutionProfileSnapshot{Key: key, TenantVersion: published.Tenant.Version, AgentAppVersion: app.Version, ContentDigest: revision.ContentDigest, AgentKind: "llm", Instruction: revision.Instruction, ModelProfileRef: profile.VersionedRef{ID: "mock", Version: 1}})
	tasks := gatewaymemory.NewTaskStore()
	sessions := sessionmemory.New()
	model := mockmodel.New()
	dispatcher := gateway.LocalDispatcher{Tasks: tasks, Bindings: configs, Executor: worker.LocalExecutor{Tasks: tasks, Profiles: profiles, Sessions: sessions, Model: model}}
	tc := tenant.Context{TenantID: "tenant-a", TenantVersion: published.Tenant.Version, AgentAppID: "app", SubjectID: "user", Channel: "fake", TrustedSource: "channel_binding:fake-account"}
	handle, err := dispatcher.Dispatch(context.Background(), gateway.DispatchRequest{Tenant: tc, RequestID: "request", SessionID: "session", UserID: "user", PayloadRef: "payload://request"})
	if err != nil {
		t.Fatal(err)
	}
	if handle.Status != "succeeded" || model.Calls("tenant-a", "request") != 1 {
		t.Fatalf("handle=%#v calls=%d", handle, model.Calls("tenant-a", "request"))
	}
}
