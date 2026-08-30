package tool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancememory "github.com/liuzengh/trpc-agent-service/trpcservice/governance/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type callable struct{ calls int }

func (c *callable) Declaration() *agenttool.Declaration       { return &agenttool.Declaration{Name: "safe"} }
func (c *callable) Call(context.Context, []byte) (any, error) { c.calls++; return "ok", nil }
func TestGuardedCallableRequiresTrustedContextAndDeniesDangerousTools(t *testing.T) {
	store := governancememory.New(0, 0)
	value := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled, Tools: []governance.ToolRule{{ToolID: "safe", Version: 1}}}
	digest, _, _ := governance.PolicyDigest(value)
	_ = store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 1, SchemaVersion: 1, Policy: value, ContentDigest: digest, PublishedAt: time.Now()})
	inner := &callable{}
	guard := GuardedCallable{Inner: inner, Policies: store, Tool: governance.VersionedRef{ID: "safe", Version: 1}}
	if _, err := guard.Call(context.Background(), nil); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("untrusted=%v", err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant", RequestID: "request", SubjectID: "user", PolicyVersion: 1})
	if _, err := guard.Call(ctx, nil); err != nil || inner.calls != 1 {
		t.Fatalf("call=%d err=%v", inner.calls, err)
	}
	value.Tools[0].Dangerous = true
	digest, _, _ = governance.PolicyDigest(value)
	_ = store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 2, SchemaVersion: 1, Policy: value, ContentDigest: digest, PublishedAt: time.Now()})
	ctx = runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant", RequestID: "request", SubjectID: "user", PolicyVersion: 2})
	if _, err := guard.Call(ctx, nil); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("dangerous=%v", err)
	}
	if inner.calls != 1 {
		t.Fatalf("dangerous tool executed %d", inner.calls)
	}
}
