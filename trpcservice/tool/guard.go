package tool

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// GuardedCallable is the non-bypassable outer wrapper around a raw Tool. Raw
// tools stay private to the resolver; every actual call re-resolves the exact
// policy carried by trusted execution context and denies dangerous ask paths
// until durable confirmation is installed.
type GuardedCallable struct {
	Inner    agenttool.CallableTool
	Policies governance.Repository
	Tool     governance.VersionedRef
}

func (g GuardedCallable) Declaration() *agenttool.Declaration {
	if g.Inner == nil {
		return nil
	}
	return g.Inner.Declaration()
}
func (g GuardedCallable) GovernanceToolRef() governance.VersionedRef { return g.Tool }
func (g GuardedCallable) Call(ctx context.Context, args []byte) (any, error) {
	execution, ok := runtime.ExecutionContextFrom(ctx)
	if !ok || execution.SubjectID == "" || execution.PolicyVersion < 1 || g.Inner == nil || g.Policies == nil || g.Tool.ID == "" || g.Tool.Version < 1 {
		return nil, runtime.ErrTenantScope
	}
	policy, err := g.Policies.GetPolicy(ctx, execution.TenantID, execution.PolicyVersion)
	if err != nil {
		return nil, err
	}
	decision := governance.ToolDecision(policy, g.Tool)
	if decision.Action != governance.ActionAllow {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return g.Inner.Call(ctx, args)
}

func GuardCallables(policies governance.Repository, refs []governance.VersionedRef, values []agenttool.Tool) ([]agenttool.Tool, error) {
	if policies == nil || len(refs) != len(values) {
		return nil, runtime.ErrCapabilityUnsupported
	}
	result := make([]agenttool.Tool, len(values))
	for index, value := range values {
		callable, ok := value.(agenttool.CallableTool)
		if !ok || value == nil || value.Declaration() == nil || value.Declaration().Name != refs[index].ID {
			return nil, runtime.ErrCapabilityUnsupported
		}
		result[index] = GuardedCallable{Inner: callable, Policies: policies, Tool: refs[index]}
	}
	return result, nil
}

var _ agenttool.CallableTool = GuardedCallable{}
var _ governance.VersionedTool = GuardedCallable{}
