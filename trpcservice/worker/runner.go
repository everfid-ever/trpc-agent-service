package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type InputDecoder interface {
	DecodeInput(context.Context, runtime.ExecutionEnvelope, []byte) (model.Message, error)
}

type InputDecoderFunc func(context.Context, runtime.ExecutionEnvelope, []byte) (model.Message, error)

func (f InputDecoderFunc) DecodeInput(ctx context.Context, envelope runtime.ExecutionEnvelope, payload []byte) (model.Message, error) {
	return f(ctx, envelope, payload)
}

type ResultRefEncoder func(context.Context, runtime.ExecutionEnvelope, string) (string, error)

type RunnerExecutor struct {
	Tasks        gateway.TaskStore
	Profiles     profile.ExecutionProfileResolver
	Bundles      profile.RuntimeBundleManager
	Sessions     sessionstore.AtomicSessionStore
	Payloads     messaging.PayloadStore
	Inputs       InputDecoder
	EncodeEvent  sessionstore.EventRefEncoder
	EncodeResult ResultRefEncoder
}

func (w RunnerExecutor) Execute(ctx context.Context, envelope runtime.ExecutionEnvelope) error {
	return w.ExecuteWithLease(ctx, envelope, 1, nil)
}

func (w RunnerExecutor) ExecuteWithLease(ctx context.Context, envelope runtime.ExecutionEnvelope, fence uint64, beforeCommit func(context.Context) error) error {
	if w.Tasks == nil || w.Profiles == nil || w.Bundles == nil || w.Sessions == nil ||
		w.Payloads == nil || w.Inputs == nil || w.EncodeEvent == nil {
		return runtime.ErrCapabilityUnsupported
	}
	if err := envelope.Validate(); err != nil {
		return err
	}
	if fence == 0 {
		return runtime.ErrStaleFence
	}
	authoritative, err := w.Tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: envelope.TenantID, RequestID: envelope.RequestID})
	if err != nil {
		return err
	}
	if err := verifyAuthoritativeEnvelope(authoritative.Envelope, envelope); err != nil {
		return err
	}

	key := profile.ExecutionProfileKey{
		TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID,
		AgentAppRevision: envelope.AgentAppRevision, ContentDigest: envelope.AgentContentDigest,
		ConfigVersion: envelope.ConfigVersion, PolicyVersion: envelope.PolicyVersion,
	}
	snapshot, err := w.Profiles.Resolve(ctx, key)
	if err != nil {
		return err
	}
	if snapshot.TenantVersion != envelope.TenantVersion || snapshot.AgentAppVersion != envelope.AgentAppVersion {
		return runtime.ErrVersionMismatch
	}
	appName := snapshot.AppName
	if appName == "" {
		appName = envelope.TenantID + "/" + envelope.AgentAppID
	}

	sessionKey := sessionstore.SessionKey{TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID, SessionID: envelope.SessionID}
	head, err := w.Sessions.OpenForRun(ctx, sessionstore.OpenForRunRequest{
		SessionKey: sessionKey, RequestID: envelope.RequestID, InputSeq: envelope.InputSeq, Fence: fence,
	})
	if err != nil {
		if errors.Is(err, runtime.ErrAlreadyTerminal) {
			_, readErr := w.Sessions.GetTerminalByInputSeq(ctx, sessionstore.TerminalKey{SessionKey: sessionKey, InputSeq: envelope.InputSeq})
			return readErr
		}
		return err
	}

	turn, err := sessionstore.NewDurableBufferedTurnScoped(w.Sessions, sessionKey, appName, envelope.UserID, w.EncodeEvent)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = turn.Rollback(context.Background())
		}
	}()

	payload, err := w.Payloads.GetPayload(ctx, envelope.TenantID, envelope.RequestID)
	if err != nil {
		return err
	}
	if payload.PayloadRef != envelope.PayloadRef {
		return runtime.ErrVersionMismatch
	}
	message, err := w.Inputs.DecodeInput(ctx, envelope, payload.Content)
	if err != nil {
		return err
	}
	if message.Role != model.RoleUser || strings.TrimSpace(message.Content) == "" {
		return runtime.ErrInvalidEnvelope
	}

	lease, err := w.Bundles.Acquire(ctx, key)
	if err != nil {
		return err
	}
	defer lease.Release()
	run, err := lease.Bundle().NewRunner(turn.SessionService())
	if err != nil {
		return err
	}
	defer func() { _ = run.Close() }()

	runCtx := runtime.WithExecutionContext(ctx, runtime.ExecutionContext{TenantID: envelope.TenantID, RequestID: envelope.RequestID})
	events, err := run.Run(runCtx, envelope.UserID, envelope.SessionID, message,
		agentcore.WithAppName(appName), agentcore.WithRequestID(envelope.RequestID))
	if err != nil {
		return err
	}
	content, err := consumeRunnerEvents(ctx, events)
	if err != nil {
		return err
	}
	resultRef, err := encodeResultRef(ctx, w.EncodeResult, envelope, content)
	if err != nil {
		return err
	}
	resultStore, ok := w.Payloads.(messaging.ResultStore)
	if !ok {
		return runtime.ErrCapabilityUnsupported
	}
	resultDigest := sha256.Sum256([]byte(content))
	if err := resultStore.PutResult(ctx, messaging.ResultRecord{TenantID: envelope.TenantID, RequestID: envelope.RequestID, ResultRef: resultRef, ContentDigest: hex.EncodeToString(resultDigest[:]), Content: []byte(content), KeyVersion: 1}); err != nil {
		return err
	}
	if beforeCommit != nil {
		if err := beforeCommit(ctx); err != nil {
			return err
		}
	}
	replyID, err := messaging.StableReplyID(messaging.ReplyCoordinate{
		TenantID: envelope.TenantID, RequestID: envelope.RequestID,
		InputSeq: envelope.InputSeq, Stage: "terminal", Ordinal: 0,
	})
	if err != nil {
		return err
	}
	_, err = turn.Commit(ctx, sessionstore.CommitTurnRequest{
		SessionKey: sessionKey, RequestID: envelope.RequestID,
		CommitID: envelope.RequestID + ":terminal:0", Stage: "terminal",
		InputSeq: envelope.InputSeq, Fence: fence, ExpectedVersion: head.Version,
		Outcome: runtime.OutcomeSucceeded, ResultRef: resultRef, ReplyCursor: envelope.RequestID + ":1",
		Outbox: []sessionstore.OutboxEvent{{Kind: "reply", IdempotencyKey: replyID, PayloadRef: resultRef, EventSeq: 1}},
	})
	if errors.Is(err, runtime.ErrAlreadyTerminal) {
		return nil
	}
	if err == nil {
		committed = true
	}
	return err
}

func verifyAuthoritativeEnvelope(trusted, delivered runtime.ExecutionEnvelope) error {
	if trusted.TenantID != delivered.TenantID || trusted.AgentAppID != delivered.AgentAppID ||
		trusted.RequestID != delivered.RequestID || trusted.SessionID != delivered.SessionID ||
		trusted.UserID != delivered.UserID || trusted.Channel != delivered.Channel {
		return runtime.ErrTenantScope
	}
	if trusted != delivered {
		return runtime.ErrVersionMismatch
	}
	return nil
}

func consumeRunnerEvents(ctx context.Context, events <-chan *event.Event) (string, error) {
	var content strings.Builder
	completed := false
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case value, ok := <-events:
			if !ok {
				if !completed {
					return "", runtime.ErrBackendUnavailable
				}
				return content.String(), nil
			}
			if value == nil {
				continue
			}
			if value.IsTerminalError() {
				return "", runtime.ErrBackendUnavailable
			}
			if value.Response != nil {
				for _, choice := range value.Choices {
					if choice.Delta.Content != "" {
						content.WriteString(choice.Delta.Content)
					} else if choice.Message.Content != "" && !value.IsRunnerCompletion() {
						content.Reset()
						content.WriteString(choice.Message.Content)
					}
				}
			}
			if value.IsRunnerCompletion() {
				completed = true
			}
		}
	}
}

func encodeResultRef(ctx context.Context, encoder ResultRefEncoder, envelope runtime.ExecutionEnvelope, content string) (string, error) {
	if encoder != nil {
		return encoder(ctx, envelope, content)
	}
	if content == "" {
		return "", runtime.ErrBackendUnavailable
	}
	digest := sha256.Sum256([]byte(content))
	return fmt.Sprintf("result://%s/%s/%s", envelope.TenantID, envelope.RequestID, hex.EncodeToString(digest[:])), nil
}

// JSONTextInputDecoder is used by the HTTP fake slice. Production Channel
// decoders provide the same normalized user-message contract.
type JSONTextInputDecoder struct{}

func (JSONTextInputDecoder) DecodeInput(ctx context.Context, _ runtime.ExecutionEnvelope, payload []byte) (model.Message, error) {
	if err := ctx.Err(); err != nil {
		return model.Message{}, err
	}
	var value struct {
		ExternalMessageID string `json:"external_message_id,omitempty"`
		ExternalUserID    string `json:"external_user_id,omitempty"`
		ExternalChatID    string `json:"external_chat_id,omitempty"`
		Text              string `json:"text"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil || strings.TrimSpace(value.Text) == "" {
		return model.Message{}, runtime.ErrInvalidEnvelope
	}
	return model.NewUserMessage(value.Text), nil
}

var _ LeaseExecutor = RunnerExecutor{}
