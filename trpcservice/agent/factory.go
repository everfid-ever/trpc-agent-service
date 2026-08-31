// Package agent builds tenant-scoped tRPC-Agent-Go agents from immutable
// ExecutionProfile snapshots.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/chainagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/cycleagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/graphagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/parallelagent"
	"trpc.group/trpc-go/trpc-agent-go/graph"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/skill"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

const maxCompositionDepth = 32

type ModelResolver interface {
	ResolveModel(context.Context, string, profile.VersionedRef) (model.Model, error)
}

type ToolResolver interface {
	ResolveTools(context.Context, string, []profile.VersionedRef) ([]tool.Tool, error)
}

type SkillResolver interface {
	RepositoryProvider(context.Context, string, []profile.SkillRef) (skill.RepositoryProvider, error)
}

type CheckpointResolver interface {
	ResolveCheckpointSaver(context.Context, string, string, int64) (graph.CheckpointSaver, error)
}

type Callbacks struct {
	Agent *agentcore.Callbacks
	Model *model.Callbacks
	Tool  *tool.Callbacks
}

type Factory struct {
	Profiles      profile.ExecutionProfileResolver
	Models        ModelResolver
	Tools         ToolResolver
	Skills        SkillResolver
	Checkpoints   CheckpointResolver
	Callbacks     Callbacks
	Policies      governance.Repository
	Confirmations governance.ConfirmationCoordinator
	ToolResults   messaging.ToolResultStore
}

func (f Factory) Build(ctx context.Context, snapshot profile.ExecutionProfileSnapshot) (agentcore.Agent, error) {
	if f.Profiles == nil || f.Models == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return f.build(ctx, snapshot, snapshot.Key.AgentAppID, 0, make(map[profile.ExecutionProfileKey]bool))
}

func (f Factory) build(ctx context.Context, snapshot profile.ExecutionProfileSnapshot, name string, depth int, path map[profile.ExecutionProfileKey]bool) (agentcore.Agent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if depth > maxCompositionDepth || path[snapshot.Key] {
		return nil, runtime.ErrInvariantViolation
	}
	path[snapshot.Key] = true
	defer delete(path, snapshot.Key)

	if snapshot.AgentKind == agentapp.AgentKindLLM {
		return f.buildLLM(ctx, snapshot, name)
	}
	subAgents := make([]agentcore.Agent, 0, len(snapshot.AgentSpec.Nodes))
	for _, node := range snapshot.AgentSpec.Nodes {
		if node.FailurePolicy != agentapp.FailurePolicyFailFast {
			return nil, runtime.ErrCapabilityUnsupported
		}
		key := profile.ExecutionProfileKey{
			TenantID:         snapshot.Key.TenantID,
			TenantVersion:    snapshot.Key.TenantVersion,
			AgentAppID:       node.AgentRef.AgentAppID,
			AgentAppRevision: node.AgentRef.Revision,
			ContentDigest:    node.AgentRef.ContentDigest,
			ConfigVersion:    snapshot.Key.ConfigVersion,
			PolicyVersion:    snapshot.Key.PolicyVersion,
		}
		var child profile.ExecutionProfileSnapshot
		var err error
		if resolver, ok := f.Profiles.(profile.ChildExecutionProfileResolver); ok {
			child, err = resolver.ResolveChild(ctx, snapshot, node)
		} else {
			child, err = f.Profiles.Resolve(ctx, key)
		}
		if err != nil {
			return nil, err
		}
		built, err := f.build(ctx, child, node.Key, depth+1, path)
		if err != nil {
			return nil, err
		}
		subAgents = append(subAgents, built)
	}

	switch snapshot.AgentKind {
	case agentapp.AgentKindChain:
		return chainagent.New(name, chainagent.WithSubAgents(subAgents), chainagent.WithAgentCallbacks(f.Callbacks.Agent)), nil
	case agentapp.AgentKindParallel:
		if snapshot.AgentSpec.MaxConcurrency < len(subAgents) {
			return nil, runtime.ErrCapabilityUnsupported
		}
		return parallelagent.New(name, parallelagent.WithSubAgents(subAgents), parallelagent.WithAgentCallbacks(f.Callbacks.Agent)), nil
	case agentapp.AgentKindCycle:
		return cycleagent.New(name, cycleagent.WithSubAgents(subAgents), cycleagent.WithMaxIterations(snapshot.AgentSpec.MaxIterations), cycleagent.WithAgentCallbacks(f.Callbacks.Agent)), nil
	case agentapp.AgentKindGraph:
		return f.buildGraph(ctx, snapshot, name, subAgents)
	default:
		return nil, runtime.ErrCapabilityUnsupported
	}
}

func (f Factory) buildLLM(ctx context.Context, snapshot profile.ExecutionProfileSnapshot, name string) (agentcore.Agent, error) {
	resolvedModel, err := f.Models.ResolveModel(ctx, snapshot.Key.TenantID, snapshot.ModelProfileRef)
	if err != nil {
		return nil, err
	}
	options := []llmagent.Option{
		llmagent.WithDescription(snapshot.Description),
		llmagent.WithInstruction(snapshot.Instruction),
		llmagent.WithGlobalInstruction(snapshot.GlobalInstruction),
		llmagent.WithModel(resolvedModel),
		llmagent.WithAgentCallbacks(f.Callbacks.Agent),
		llmagent.WithModelCallbacks(f.Callbacks.Model),
		llmagent.WithToolCallbacks(f.Callbacks.Tool),
	}
	if len(snapshot.GenerationConfig) != 0 {
		config, err := decodeGenerationConfig(snapshot.GenerationConfig)
		if err != nil {
			return nil, err
		}
		options = append(options, llmagent.WithGenerationConfig(config))
	}
	if len(snapshot.ToolRefs) != 0 {
		if f.Tools == nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
		tools, err := f.Tools.ResolveTools(ctx, snapshot.Key.TenantID, snapshot.ToolRefs)
		if err != nil {
			return nil, err
		}
		refs := make([]governance.VersionedRef, len(snapshot.ToolRefs))
		for index, ref := range snapshot.ToolRefs {
			refs[index] = governance.VersionedRef{ID: ref.ID, Version: ref.Version}
		}
		guarded, err := servicetool.GuardCallablesWithConfirmation(f.Policies, f.Confirmations, f.ToolResults, refs, tools)
		if err != nil {
			return nil, err
		}
		options = append(options, llmagent.WithTools(guarded))
	}
	if len(snapshot.SkillRefs) != 0 {
		if f.Skills == nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
		provider, err := f.Skills.RepositoryProvider(ctx, snapshot.Key.TenantID, snapshot.SkillRefs)
		if err != nil {
			return nil, err
		}
		options = append(options, llmagent.WithSkillRepositoryProvider(provider), llmagent.WithSkillScopeMode(skill.SkillScopeApp))
	}
	return llmagent.New(name, options...), nil
}

// ResolveConfirmedTool returns one exact-version callable behind the same
// non-bypassable policy/grant wrapper used by normal agent construction.
func (f Factory) ResolveConfirmedTool(ctx context.Context, tenantID string, ref governance.VersionedRef) (tool.CallableTool, error) {
	if f.Tools == nil || f.Policies == nil || f.Confirmations == nil || f.ToolResults == nil || tenantID == "" || ref.ID == "" || ref.Version < 1 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	values, err := f.Tools.ResolveTools(ctx, tenantID, []profile.VersionedRef{{ID: ref.ID, Version: ref.Version}})
	if err != nil {
		return nil, err
	}
	guarded, err := servicetool.GuardCallablesWithConfirmation(f.Policies, f.Confirmations, f.ToolResults, []governance.VersionedRef{ref}, values)
	if err != nil {
		return nil, err
	}
	callable, ok := guarded[0].(tool.CallableTool)
	if !ok {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return callable, nil
}

func (f Factory) buildGraph(ctx context.Context, snapshot profile.ExecutionProfileSnapshot, name string, subAgents []agentcore.Agent) (agentcore.Agent, error) {
	for _, edge := range snapshot.AgentSpec.Edges {
		if edge.ConditionRef != nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
	}
	stateGraph := graph.NewStateGraph(graph.NewStateSchema())
	outgoing := make(map[string]bool, len(snapshot.AgentSpec.Nodes))
	for _, node := range snapshot.AgentSpec.Nodes {
		stateGraph.AddAgentNode(node.Key)
	}
	stateGraph.SetEntryPoint(snapshot.AgentSpec.EntryNode)
	for _, edge := range snapshot.AgentSpec.Edges {
		stateGraph.AddEdge(edge.From, edge.To)
		outgoing[edge.From] = true
	}
	for _, node := range snapshot.AgentSpec.Nodes {
		if !outgoing[node.Key] {
			stateGraph.AddEdge(node.Key, graph.End)
		}
	}
	compiled, err := stateGraph.Compile()
	if err != nil {
		return nil, fmt.Errorf("compile graph: %w", err)
	}
	options := []graphagent.Option{
		graphagent.WithDescription(snapshot.Description),
		graphagent.WithSubAgents(subAgents),
		graphagent.WithMaxConcurrency(snapshot.AgentSpec.MaxConcurrency),
		graphagent.WithAgentCallbacks(f.Callbacks.Agent),
	}
	if snapshot.AgentSpec.Checkpoint.Required {
		if f.Checkpoints == nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
		saver, err := f.Checkpoints.ResolveCheckpointSaver(ctx, snapshot.Key.TenantID, snapshot.AgentSpec.Checkpoint.Namespace, snapshot.Key.ConfigVersion)
		if err != nil {
			return nil, err
		}
		if saver == nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
		options = append(options, graphagent.WithCheckpointSaver(saver))
	}
	return graphagent.New(name, compiled, options...)
}

func decodeGenerationConfig(value profile.GenerationConfigV1) (model.GenerationConfig, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return model.GenerationConfig{}, runtime.ErrInvariantViolation
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config model.GenerationConfig
	if err := decoder.Decode(&config); err != nil {
		return model.GenerationConfig{}, fmt.Errorf("%w: generation config: %v", runtime.ErrInvariantViolation, err)
	}
	return config, nil
}
