// Package upstream contains compile-time checks for the public
// tRPC-Agent-Go API surface required by the service.
package upstream

import (
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/graph"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
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
)

// resumeMap keeps the ResumeMap field itself in the compatibility contract;
// referring only to graph.ResumeCommand would not detect removal of the field.
func resumeMap(command graph.ResumeCommand) map[string]any {
	return command.ResumeMap
}
