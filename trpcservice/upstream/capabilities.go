// Package upstream contains compile-time checks for the public
// tRPC-Agent-Go API surface required by the service.
package upstream

import (
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/chainagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/cycleagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/graphagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/parallelagent"
	"trpc.group/trpc-go/trpc-agent-go/graph"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	a2aserver "trpc.group/trpc-go/trpc-agent-go/server/a2a"
	openaiserver "trpc.group/trpc-go/trpc-agent-go/server/openai"
	trpcagentserver "trpc.group/trpc-go/trpc-agent-go/server/trpcagent"
	"trpc.group/trpc-go/trpc-agent-go/skill"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// ModuleVersion is the reviewed upstream release. The dependency-boundary
// check verifies that go.mod resolves this exact version.
const ModuleVersion = "v1.11.2"

// These assignments intentionally fail compilation if an upstream upgrade
// removes or renames an API required by the frozen service contracts.
var (
	_ = runner.NewRunnerWithAgentFactory
	_ = runner.WithAwaitUserReplyRouting
	_ = llmagent.New
	_ = graphagent.New
	_ = chainagent.New
	_ = parallelagent.New
	_ = cycleagent.New
	_ = llmagent.WithSkills
	_ = llmagent.WithSkillRepositoryProvider
	_ = llmagent.WithSkillScopeMode
	_ = llmagent.WithSkillFilter
	_ = llmagent.WithAgentCallbacks
	_ = llmagent.WithModelCallbacks
	_ = llmagent.WithToolCallbacks
	_ = graphagent.WithAgentCallbacks
	_ = chainagent.WithAgentCallbacks
	_ = parallelagent.WithAgentCallbacks
	_ = cycleagent.WithAgentCallbacks
	_ = agent.WithAppName
	_ = agent.WithRequestID
	_ = agent.WithModel
	_ = agent.WithModelName
	_ = agent.WithToolFilter
	_ = agent.WithToolPermissionPolicy
	_ = agent.WithToolExecutionFilter
	_ = agent.AwaitUserReplyRoute{}
	_ = graph.CfgKeyCheckpointID
	_ = model.NewToolMessage
	_ = tool.PermissionActionAsk
	_ = skill.NewFSRepository
	_ = knowledge.New
	_ = plugin.NewManager
	_ = openaiserver.New
	_ = openaiserver.WithRunner
	_ = a2aserver.New
	_ = a2aserver.WithRunner
	_ = trpcagentserver.New
	_ = trpcagentserver.WithRunner
)

var (
	_ skill.RepositoryProvider = skill.RepositoryProviderFunc(nil)
	_ knowledge.Knowledge      = nil
)

// resumeMap keeps the ResumeMap field itself in the compatibility contract;
// referring only to graph.ResumeCommand would not detect removal of the field.
func resumeMap(command graph.ResumeCommand) map[string]any {
	return command.ResumeMap
}
