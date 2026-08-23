package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type taskStub struct{ envelope runtime.ExecutionEnvelope }

func (s taskStub) PrepareDispatch(context.Context, gateway.PrepareDispatchRequest) (gateway.PreparedDispatch, error) {
	panic("unexpected call")
}
func (s taskStub) GetExecution(context.Context, gateway.ExecutionKey) (gateway.ExecutionStatus, error) {
	return gateway.ExecutionStatus{Envelope: s.envelope}, nil
}
func (s taskStub) RequestCancel(context.Context, gateway.CancelRequest) (gateway.CancelResult, error) {
	panic("unexpected call")
}
func (s taskStub) ParkInput(context.Context, gateway.ParkRequest) error { panic("unexpected call") }

func TestWorkerRejectsEnvelopeVersionForgery(t *testing.T) {
	authoritative := runtime.ExecutionEnvelope{SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1, AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1, RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1, PayloadRef: "payload://request", CreatedAt: time.Now().UTC()}
	forged := authoritative
	forged.ConfigVersion = 2
	err := (LocalExecutor{Tasks: taskStub{envelope: authoritative}}).Execute(context.Background(), forged)
	if !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("got %v", err)
	}
}
