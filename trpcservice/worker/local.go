// Package worker executes trusted envelopes against shared service ports.
package worker

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

type Model interface {
	Generate(context.Context, runtime.ExecutionEnvelope, profile.ExecutionProfileSnapshot) (string, error)
}

type LocalExecutor struct {
	Tasks    gateway.TaskStore
	Profiles profile.ExecutionProfileResolver
	Sessions sessionstore.AtomicSessionStore
	Model    Model
}

func (w LocalExecutor) Execute(ctx context.Context, envelope runtime.ExecutionEnvelope) error {
	if err := envelope.Validate(); err != nil {
		return err
	}
	authoritative, err := w.Tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: envelope.TenantID, RequestID: envelope.RequestID})
	if err != nil {
		return err
	}
	trusted := authoritative.Envelope
	if trusted.TenantID != envelope.TenantID || trusted.AgentAppID != envelope.AgentAppID ||
		trusted.RequestID != envelope.RequestID || trusted.SessionID != envelope.SessionID ||
		trusted.UserID != envelope.UserID || trusted.Channel != envelope.Channel {
		return runtime.ErrTenantScope
	}
	if trusted != envelope {
		return runtime.ErrVersionMismatch
	}
	key := profile.ExecutionProfileKey{TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID, AgentAppRevision: envelope.AgentAppRevision, ContentDigest: envelope.AgentContentDigest, ConfigVersion: envelope.ConfigVersion, PolicyVersion: envelope.PolicyVersion}
	snapshot, err := w.Profiles.Resolve(ctx, key)
	if err != nil {
		return err
	}
	if snapshot.TenantVersion != envelope.TenantVersion || snapshot.AgentAppVersion != envelope.AgentAppVersion {
		return runtime.ErrVersionMismatch
	}
	sk := sessionstore.SessionKey{TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID, SessionID: envelope.SessionID}
	head, err := w.Sessions.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: sk, RequestID: envelope.RequestID, InputSeq: envelope.InputSeq, Fence: 1})
	if err != nil {
		if err == runtime.ErrAlreadyTerminal {
			_, readErr := w.Sessions.GetTerminalByInputSeq(ctx, sessionstore.TerminalKey{SessionKey: sk, InputSeq: envelope.InputSeq})
			return readErr
		}
		return err
	}
	resultRef, err := w.Model.Generate(ctx, envelope, snapshot)
	if err != nil {
		return err
	}
	_, err = w.Sessions.CommitTurn(ctx, sessionstore.CommitTurnRequest{SessionKey: sk, RequestID: envelope.RequestID, CommitID: envelope.RequestID + ":terminal:0", Stage: "terminal", InputSeq: envelope.InputSeq, Fence: 1, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded, ResultRef: resultRef, ReplyCursor: envelope.RequestID + ":1"})
	return err
}
