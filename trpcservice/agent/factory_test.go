package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker/mockmodel"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type modelResolver struct{ value model.Model }

func (r modelResolver) ResolveModel(context.Context, string, profile.VersionedRef) (model.Model, error) {
	return r.value, nil
}

func TestFactoryBuildsAllAgentKindsFromFixedChildProfiles(t *testing.T) {
	const childDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	childKey := profile.ExecutionProfileKey{TenantID: "tenant-a", AgentAppID: "child", AgentAppRevision: 1, ContentDigest: childDigest, ConfigVersion: 2, PolicyVersion: 3}
	child := profile.ExecutionProfileSnapshot{
		Key: childKey, ContentDigest: childDigest, AgentKind: agentapp.AgentKindLLM,
		Description: "child description", Instruction: "answer", ModelProfileRef: profile.VersionedRef{ID: "model", Version: 1},
	}
	factory := Factory{Profiles: profilememory.NewResolver(child), Models: modelResolver{value: mockmodel.New()}}
	node := agentapp.AgentNodeSpecV1{Key: "worker", FailurePolicy: agentapp.FailurePolicyFailFast, AgentRef: agentapp.PublishedAgentRef{AgentAppID: "child", Revision: 1, ContentDigest: childDigest}}
	tests := []struct {
		kind agentapp.AgentKind
		spec agentapp.AgentSpecV1
	}{
		{agentapp.AgentKindChain, agentapp.AgentSpecV1{Nodes: []agentapp.AgentNodeSpecV1{node}}},
		{agentapp.AgentKindParallel, agentapp.AgentSpecV1{Nodes: []agentapp.AgentNodeSpecV1{node}, MaxConcurrency: 1}},
		{agentapp.AgentKindCycle, agentapp.AgentSpecV1{Nodes: []agentapp.AgentNodeSpecV1{node}, MaxIterations: 2}},
		{agentapp.AgentKindGraph, agentapp.AgentSpecV1{Nodes: []agentapp.AgentNodeSpecV1{node}, EntryNode: "worker", MaxConcurrency: 1}},
	}
	for _, test := range tests {
		t.Run(string(test.kind), func(t *testing.T) {
			root, err := factory.Build(context.Background(), profile.ExecutionProfileSnapshot{
				Key:       profile.ExecutionProfileKey{TenantID: "tenant-a", AgentAppID: "root", AgentAppRevision: 1, ContentDigest: string(test.kind), ConfigVersion: 2, PolicyVersion: 3},
				AgentKind: test.kind, AgentSpec: test.spec, Description: "root description",
			})
			if err != nil {
				t.Fatal(err)
			}
			if root.Info().Name != "root" || len(root.SubAgents()) != 1 || root.SubAgents()[0].Info().Name != "worker" {
				t.Fatalf("unexpected composition: info=%#v children=%d", root.Info(), len(root.SubAgents()))
			}
		})
	}
	llm, err := factory.Build(context.Background(), child)
	if err != nil {
		t.Fatal(err)
	}
	if llm.Info().Description != "child description" {
		t.Fatalf("description=%q", llm.Info().Description)
	}
}

func TestFactoryFailsClosedForRequiredCheckpoint(t *testing.T) {
	const digest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	key := profile.ExecutionProfileKey{TenantID: "tenant-a", AgentAppID: "child", AgentAppRevision: 1, ContentDigest: digest, ConfigVersion: 2, PolicyVersion: 3}
	child := profile.ExecutionProfileSnapshot{Key: key, ContentDigest: digest, AgentKind: agentapp.AgentKindLLM, Instruction: "answer", ModelProfileRef: profile.VersionedRef{ID: "m", Version: 1}}
	factory := Factory{Profiles: profilememory.NewResolver(child), Models: modelResolver{value: mockmodel.New()}}
	_, err := factory.Build(context.Background(), profile.ExecutionProfileSnapshot{
		Key:       profile.ExecutionProfileKey{TenantID: "tenant-a", AgentAppID: "root", AgentAppRevision: 1, ContentDigest: "root", ConfigVersion: 2, PolicyVersion: 3},
		AgentKind: agentapp.AgentKindGraph,
		AgentSpec: agentapp.AgentSpecV1{Nodes: []agentapp.AgentNodeSpecV1{{Key: "worker", FailurePolicy: agentapp.FailurePolicyFailFast, AgentRef: agentapp.PublishedAgentRef{AgentAppID: "child", Revision: 1, ContentDigest: digest}}}, EntryNode: "worker", MaxConcurrency: 1, Checkpoint: agentapp.CheckpointPolicyV1{Required: true, Namespace: "workflow"}},
	})
	if !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("got %v", err)
	}
}
