package worker

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/graph"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type InputDecoder interface {
	DecodeInput(context.Context, runtime.ExecutionEnvelope, []byte) (model.Message, error)
}

type InputDecoderFunc func(context.Context, runtime.ExecutionEnvelope, []byte) (model.Message, error)

func (f InputDecoderFunc) DecodeInput(ctx context.Context, envelope runtime.ExecutionEnvelope, payload []byte) (model.Message, error) {
	return f(ctx, envelope, payload)
}

type ResultRefEncoder func(context.Context, runtime.ExecutionEnvelope, string) (string, error)

type ConfirmedToolResolver interface {
	ResolveConfirmedTool(context.Context, string, governance.VersionedRef) (agenttool.CallableTool, error)
}

type RunnerExecutor struct {
	Tasks             gateway.TaskStore
	Profiles          profile.ExecutionProfileResolver
	Bundles           profile.RuntimeBundleManager
	Sessions          sessionstore.AtomicSessionStore
	Payloads          messaging.PayloadStore
	Artifacts         artifact.Store
	Inputs            InputDecoder
	EncodeEvent       sessionstore.EventRefEncoder
	EncodeResult      ResultRefEncoder
	Governance        governance.RunGuard
	Confirmations     governance.ConfirmationCoordinator
	ContinuationTools ConfirmedToolResolver
	ConfirmationTTL   time.Duration
	EventDrainTimeout time.Duration
	Telemetry         telemetry.Provider
}

func (w RunnerExecutor) Execute(ctx context.Context, envelope runtime.ExecutionEnvelope) error {
	return w.ExecuteWithLease(ctx, envelope, 1, nil)
}

func (w RunnerExecutor) ExecuteWithLease(ctx context.Context, envelope runtime.ExecutionEnvelope, fence uint64, beforeCommit func(context.Context) error) (resultErr error) {
	ctx, finish := telemetry.StartOperation(ctx, w.Telemetry, envelope.TraceParent, telemetry.OperationWorkerExecute,
		telemetry.ComponentAttribute(telemetry.ComponentWorker))
	defer func() { finish(resultErr) }()
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
	if authoritative.CancelRequested {
		return runtime.ErrCancelRequested
	}

	key := profile.ExecutionProfileKey{
		TenantID: envelope.TenantID, TenantVersion: envelope.TenantVersion,
		AgentAppID: envelope.AgentAppID, AgentAppVersion: envelope.AgentAppVersion,
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
	modelRef, err := executionModelRef(ctx, w.Profiles, snapshot)
	if err != nil {
		return err
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
	var continuation *governance.Confirmation
	if w.Confirmations != nil {
		confirmation, confirmationErr := w.Confirmations.GetConfirmationByRequest(ctx, envelope.TenantID, envelope.RequestID)
		if confirmationErr == nil {
			switch confirmation.State {
			case governance.ConfirmationPending:
				return nil
			case governance.ConfirmationDenied, governance.ConfirmationExpired:
				if w.Governance != nil {
					if err := w.Governance.Abort(ctx, envelope, modelRef, string(confirmation.State)); err != nil {
						return err
					}
				}
				outcome := runtime.OutcomeConfirmationDenied
				if confirmation.State == governance.ConfirmationExpired {
					outcome = runtime.OutcomeConfirmationTimeout
				}
				reason := governance.ReasonConfirmationDenied
				if confirmation.State == governance.ConfirmationExpired {
					reason = governance.ReasonConfirmationExpired
				}
				decision := governance.Decision{DecisionID: governance.StableDecisionID(envelope.TenantID, envelope.RequestID, string(confirmation.State), envelope.PolicyVersion),
					TenantID: envelope.TenantID, RequestID: envelope.RequestID, Stage: string(confirmation.State), Action: governance.ActionDeny,
					ReasonCode: reason, PolicyVersion: envelope.PolicyVersion}
				if w.Governance != nil {
					if err := w.Governance.Record(ctx, decision); err != nil {
						return err
					}
				}
				return w.commitGovernanceTerminal(ctx, turn, envelope, head, fence, beforeCommit, outcome, decision)
			case governance.ConfirmationApproved, governance.ConfirmationConsumed:
				continuation = &confirmation
			default:
				return runtime.ErrInvariantViolation
			}
		} else if !errors.Is(confirmationErr, runtime.ErrNotFound) {
			return confirmationErr
		}
	}

	payload, err := w.executionPayload(ctx, envelope)
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
	if message.Role != model.RoleUser || (strings.TrimSpace(message.Content) == "" && len(message.ContentParts) == 0) || !validPreparedMessage(message) {
		return runtime.ErrInvalidEnvelope
	}
	if err := w.hydrateArtifacts(ctx, envelope, &message); err != nil {
		return err
	}
	var permit governance.RunPermit
	if w.Governance != nil {
		permit, err = w.Governance.Begin(ctx, envelope, modelRef, payload.Content)
		if err != nil {
			return err
		}
		if permit.Decision.Action != governance.ActionAllow {
			return w.commitGovernanceTerminal(ctx, turn, envelope, head, fence, beforeCommit, runtime.OutcomeDenied, permit.Decision)
		}
	}

	lease, err := w.Bundles.Acquire(ctx, key)
	if err != nil {
		if w.Governance != nil {
			_ = w.Governance.Refund(ctx, permit, "bundle_acquire_failed")
		}
		return err
	}
	defer lease.Release()
	run, err := lease.Bundle().NewRunner(turn.SessionService())
	if err != nil {
		if w.Governance != nil {
			_ = w.Governance.Refund(ctx, permit, "runner_create_failed")
		}
		return err
	}
	var events <-chan *event.Event
	runnerClosed := false
	defer func() {
		if !runnerClosed {
			closeRunner(run, events, w.EventDrainTimeout)
		}
	}()

	runCtx := runtime.WithExecutionContext(ctx, runtime.ExecutionContext{TenantID: envelope.TenantID, RequestID: envelope.RequestID, SubjectID: envelope.UserID, PolicyVersion: envelope.PolicyVersion, PayloadKeyVersion: payload.KeyVersion})
	runOptions := []agentcore.RunOption{agentcore.WithAppName(appName), agentcore.WithRequestID(envelope.RequestID)}
	if w.Governance != nil {
		toolRule := func(value agenttool.Tool) governance.Decision {
			versioned, ok := value.(governance.VersionedTool)
			if !ok || value == nil || value.Declaration() == nil {
				return governance.Decision{Action: governance.ActionDeny, ReasonCode: governance.ReasonToolDenied}
			}
			ref := versioned.GovernanceToolRef()
			if ref.ID != value.Declaration().Name {
				return governance.Decision{Action: governance.ActionDeny, ReasonCode: governance.ReasonToolDenied}
			}
			return governance.ToolDecision(permit.Policy, ref)
		}
		runOptions = append(runOptions,
			agentcore.WithToolFilter(func(_ context.Context, value agenttool.Tool) bool {
				action := toolRule(value).Action
				return action == governance.ActionAllow || action == governance.ActionAsk
			}),
			agentcore.WithToolExecutionFilter(func(_ context.Context, value agenttool.Tool) bool {
				return toolRule(value).Action == governance.ActionAllow
			}),
			agentcore.WithToolPermissionPolicyFunc(func(permissionCtx context.Context, request *agenttool.PermissionRequest) (agenttool.PermissionDecision, error) {
				if request == nil || toolRule(request.Tool).Action != governance.ActionAllow {
					serviceagent.FinishToolOperation(permissionCtx, runtime.ErrCapabilityUnsupported)
					return agenttool.DenyPermission(governance.ReasonToolDenied), nil
				}
				return agenttool.AllowPermission(), nil
			}))
	}
	usageOffset := governance.Usage{}
	if continuation != nil {
		var graphResume *graphContinuationCoordinate
		if snapshot.AgentKind == agentapp.AgentKindGraph {
			coordinate, coordinateErr := decodeGraphContinuationRef(continuation.CheckpointRef)
			if coordinateErr != nil {
				return coordinateErr
			}
			graphResume = &coordinate
		}
		call, callErr := confirmedToolCall(ctx, w.Sessions, sessionKey, *continuation)
		if callErr != nil {
			return callErr
		}
		grant, grantErr := w.Confirmations.GetGrantByConfirmation(ctx, envelope.TenantID, continuation.ConfirmationID)
		if grantErr != nil {
			return grantErr
		}
		if grant.RequestID != envelope.RequestID || grant.SubjectID != envelope.UserID || grant.Tool != continuation.Tool || grant.ToolCallID != call.ID || grant.ArgsDigest != continuation.ArgsDigest || grant.PolicyVersion != envelope.PolicyVersion {
			return runtime.ErrTenantScope
		}
		toolResults, ok := w.Payloads.(messaging.ToolResultStore)
		if !ok {
			return runtime.ErrCapabilityUnsupported
		}
		var encoded []byte
		if continuation.State == governance.ConfirmationApproved {
			if w.ContinuationTools == nil {
				return runtime.ErrCapabilityUnsupported
			}
			callable, resolveErr := w.ContinuationTools.ResolveConfirmedTool(ctx, envelope.TenantID, continuation.Tool)
			if resolveErr != nil {
				return resolveErr
			}
			confirmedCtx := runtime.WithExecutionContext(ctx, runtime.ExecutionContext{TenantID: envelope.TenantID, RequestID: envelope.RequestID,
				SubjectID: envelope.UserID, PolicyVersion: envelope.PolicyVersion, GrantID: grant.GrantID, GrantVersion: grant.Version,
				ToolCallID: call.ID, ArgsDigest: continuation.ArgsDigest, PayloadKeyVersion: payload.KeyVersion})
			result, callErr := callable.Call(confirmedCtx, call.Function.Arguments)
			if callErr != nil {
				attempt, attemptErr := w.Confirmations.GetToolAttempt(ctx, envelope.TenantID, grant.GrantID)
				if attemptErr == nil && attempt.State == governance.ToolAttemptFailed {
					return w.commitContinuationFailure(ctx, turn, envelope, head, fence, beforeCommit, modelRef, governance.ReasonToolAttemptFailed)
				}
				// A storage/response failure after the external effect is not proof
				// that the tool failed. Leave the request retryable; the consumed
				// path will recover a durable result or terminalize effect_unknown.
				return callErr
			}
			encoded, err = json.Marshal(result)
			if err != nil {
				return runtime.ErrInvariantViolation
			}
		} else {
			attempt, attemptErr := w.Confirmations.GetToolAttempt(ctx, envelope.TenantID, grant.GrantID)
			if attemptErr != nil {
				return attemptErr
			}
			if attempt.State == governance.ToolAttemptFailed {
				return w.commitContinuationFailure(ctx, turn, envelope, head, fence, beforeCommit, modelRef, governance.ReasonToolAttemptFailed)
			}
			stored, storedErr := toolResults.GetToolResult(ctx, envelope.TenantID, grant.GrantID)
			if storedErr != nil {
				return w.commitContinuationFailure(ctx, turn, envelope, head, fence, beforeCommit, modelRef, governance.ReasonToolEffectUnknown)
			}
			if stored.RequestID != envelope.RequestID || stored.ResultRef != attempt.ResultRef && attempt.State == governance.ToolAttemptSucceeded {
				return runtime.ErrVersionMismatch
			}
			if attempt.State == governance.ToolAttemptEffectUnknown {
				if _, finishErr := w.Confirmations.FinishToolAttempt(ctx, governance.FinishToolAttemptRequest{TenantID: envelope.TenantID, GrantID: grant.GrantID,
					State: governance.ToolAttemptSucceeded, ResultRef: stored.ResultRef}); finishErr != nil {
					return finishErr
				}
			}
			encoded = stored.Content
		}
		if graphResume != nil {
			if graphResume.ToolCallID != call.ID || graphResume.ToolName != call.Function.Name {
				return runtime.ErrVersionMismatch
			}
			message = model.NewUserMessage("resume")
			resumeValue := map[string]any{"schema_version": 1, "kind": "tool_result", "tool_call_id": call.ID,
				"tool_name": call.Function.Name, "result": string(encoded)}
			runOptions = append(runOptions, agentcore.WithRuntimeState(map[string]any{
				graph.CfgKeyLineageID:    graphResume.LineageID,
				graph.CfgKeyCheckpointID: graphResume.CheckpointID,
				graph.CfgKeyCheckpointNS: graphResume.Namespace,
				graph.StateKeyCommand: graph.NewResumeCommand().AddResumeValue(
					graphResume.TaskID, resumeValue),
			}))
		} else {
			message = model.NewToolMessage(call.ID, call.Function.Name, string(encoded))
		}
		usageOffset = continuation.Usage
	}
	events, err = run.Run(runCtx, envelope.UserID, envelope.SessionID, message, runOptions...)
	if err != nil {
		return err
	}
	runResult, err := consumeRunnerEvents(ctx, events)
	if err != nil {
		closeRunner(run, events, w.EventDrainTimeout)
		runnerClosed = true
		return err
	}
	if continuation != nil {
		if err := addUsage(&runResult.Usage, usageOffset); err != nil {
			return err
		}
	}
	if len(runResult.ToolCalls) != 0 {
		if w.Governance == nil || w.Confirmations == nil || len(runResult.ToolCalls) != 1 {
			return runtime.ErrCapabilityUnsupported
		}
		call := runResult.ToolCalls[0]
		toolRef, ok := confirmationToolRef(ctx, w.Profiles, snapshot, permit.Policy, call.Function.Name)
		if !ok || call.ID == "" {
			return runtime.ErrCapabilityUnsupported
		}
		_, argsDigest, canonicalErr := governance.CanonicalArguments(call.Function.Arguments)
		if canonicalErr != nil {
			return canonicalErr
		}
		bindingID, bindingErr := confirmationBindingID(payload.Content)
		if bindingErr != nil {
			return bindingErr
		}
		confirmationID, idErr := governance.StableConfirmationID(envelope.TenantID, envelope.RequestID, call.ID)
		if idErr != nil {
			return idErr
		}
		ttl := w.ConfirmationTTL
		if ttl <= 0 {
			ttl = 15 * time.Minute
		}
		expiresAt := envelope.CreatedAt.UTC().Add(ttl)
		promptRef := "confirmation://" + envelope.TenantID + "/" + confirmationID
		prompt, promptErr := json.Marshal(map[string]any{"schema_version": 1, "kind": "tool_confirmation", "confirmation_id": confirmationID,
			"tool_id": toolRef.ID, "tool_version": toolRef.Version, "expires_at": expiresAt.Format(time.RFC3339Nano)})
		if promptErr != nil {
			return promptErr
		}
		interactions, ok := w.Payloads.(messaging.InteractionStore)
		if !ok {
			return runtime.ErrCapabilityUnsupported
		}
		promptDigest := sha256.Sum256(prompt)
		if err := interactions.PutInteraction(ctx, messaging.InteractionRecord{TenantID: envelope.TenantID, RequestID: envelope.RequestID,
			ContentRef: promptRef, ContentDigest: hex.EncodeToString(promptDigest[:]), Content: prompt, KeyVersion: payload.KeyVersion}); err != nil {
			return err
		}
		if beforeCommit != nil {
			if err := beforeCommit(ctx); err != nil {
				return err
			}
		}
		checkpointRef := "continuation://" + envelope.TenantID + "/" + envelope.RequestID + "/" + call.ID
		if snapshot.AgentKind == agentapp.AgentKindGraph {
			if runResult.GraphInterrupt == nil || runResult.GraphInterrupt.ToolCallID != call.ID ||
				runResult.GraphInterrupt.ToolName != call.Function.Name {
				return runtime.ErrCapabilityUnsupported
			}
			checkpointRef, err = encodeGraphContinuationRef(*runResult.GraphInterrupt)
			if err != nil {
				return err
			}
		}
		commit := sessionstore.CommitTurnRequest{SessionKey: sessionKey, RequestID: envelope.RequestID,
			CommitID: envelope.RequestID + ":waiting:" + call.ID, Stage: "waiting", InputSeq: envelope.InputSeq, Fence: fence,
			ExpectedVersion: head.Version, Outcome: runtime.OutcomeWaitingConfirmation, Events: turn.Events(), StateDelta: turn.StateDelta(),
			ResultRef: checkpointRef,
			Outbox: []sessionstore.OutboxEvent{{Kind: "audit", IdempotencyKey: "confirmation:" + confirmationID,
				PayloadRef: promptRef, EventSeq: 1, TraceParent: telemetry.EffectiveTraceParent(ctx, envelope.TraceParent)}, {Kind: "reply", IdempotencyKey: "confirmation-reply:" + confirmationID,
				PayloadRef: promptRef, EventSeq: 1, TraceParent: telemetry.EffectiveTraceParent(ctx, envelope.TraceParent)}}}
		_, err = w.Confirmations.Suspend(ctx, commit, governance.SuspensionRequest{ConfirmationID: confirmationID, TenantID: envelope.TenantID,
			RequestID: envelope.RequestID, AgentAppID: envelope.AgentAppID, SessionID: envelope.SessionID, InputSeq: envelope.InputSeq, Fence: fence,
			SubjectID: envelope.UserID, ChannelBindingID: bindingID, Tool: toolRef, ToolCallID: call.ID, ArgsDigest: argsDigest,
			CheckpointRef: commit.ResultRef, PolicyVersion: envelope.PolicyVersion, Usage: runResult.Usage, ExpiresAt: expiresAt})
		if err == nil {
			committed = true
		}
		return err
	}
	content := runResult.Content
	if w.Governance != nil {
		decision, finishErr := w.Governance.Finish(ctx, permit, runResult.Usage, []byte(content))
		if finishErr != nil {
			decision = governance.Decision{DecisionID: governance.StableDecisionID(envelope.TenantID, envelope.RequestID, "settlement", envelope.PolicyVersion), TenantID: envelope.TenantID,
				RequestID: envelope.RequestID, Stage: "settlement", Action: governance.ActionDeny, ReasonCode: governance.ReasonUsageUnavailable, PolicyVersion: envelope.PolicyVersion, ReservationID: permit.Reservation.ReservationID}
			if recordErr := w.Governance.Record(ctx, decision); recordErr != nil {
				return recordErr
			}
			return w.commitGovernanceTerminal(ctx, turn, envelope, head, fence, beforeCommit, runtime.OutcomeFailed, decision)
		}
		if decision.Action != governance.ActionAllow {
			return w.commitGovernanceTerminal(ctx, turn, envelope, head, fence, beforeCommit, runtime.OutcomeDenied, decision)
		}
	}
	latest, err := w.Tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: envelope.TenantID, RequestID: envelope.RequestID})
	if err != nil {
		return err
	}
	if err := verifyAuthoritativeEnvelope(latest.Envelope, envelope); err != nil {
		return err
	}
	if latest.CancelRequested {
		return runtime.ErrCancelRequested
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
	if err := resultStore.PutResult(ctx, messaging.ResultRecord{TenantID: envelope.TenantID, RequestID: envelope.RequestID, ResultRef: resultRef, ContentDigest: hex.EncodeToString(resultDigest[:]), Content: []byte(content), KeyVersion: payload.KeyVersion}); err != nil {
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
		Outbox: []sessionstore.OutboxEvent{{Kind: "reply", IdempotencyKey: replyID, PayloadRef: resultRef, EventSeq: 1,
			TraceParent: telemetry.EffectiveTraceParent(ctx, envelope.TraceParent)}},
	})
	if errors.Is(err, runtime.ErrAlreadyTerminal) {
		return nil
	}
	if err == nil {
		committed = true
	}
	return err
}

// CancelWithLease turns a durable cancellation intent into the only
// authoritative cancelled terminal: a fenced CommitTurn that advances the
// session input gate and emits an audit fact.
func (w RunnerExecutor) CancelWithLease(ctx context.Context, envelope runtime.ExecutionEnvelope, fence uint64, beforeCommit func(context.Context) error) (resultErr error) {
	ctx, finish := telemetry.StartOperation(ctx, w.Telemetry, envelope.TraceParent, telemetry.OperationWorkerExecute,
		telemetry.ComponentAttribute(telemetry.ComponentWorker))
	defer func() { finish(resultErr) }()
	if w.Tasks == nil || w.Sessions == nil {
		return runtime.ErrCapabilityUnsupported
	}
	if err := envelope.Validate(); err != nil {
		return err
	}
	if fence == 0 {
		return runtime.ErrStaleFence
	}
	status, err := w.Tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: envelope.TenantID, RequestID: envelope.RequestID})
	if err != nil {
		return err
	}
	if err := verifyAuthoritativeEnvelope(status.Envelope, envelope); err != nil {
		return err
	}
	if status.Outcome.Terminal() {
		return nil
	}
	if !status.CancelRequested || status.CancelVersion < 1 {
		return runtime.ErrInvariantViolation
	}
	if w.Governance != nil && w.Profiles != nil {
		key := profile.ExecutionProfileKey{TenantID: envelope.TenantID, TenantVersion: envelope.TenantVersion,
			AgentAppID: envelope.AgentAppID, AgentAppVersion: envelope.AgentAppVersion, AgentAppRevision: envelope.AgentAppRevision,
			ContentDigest: envelope.AgentContentDigest, ConfigVersion: envelope.ConfigVersion, PolicyVersion: envelope.PolicyVersion}
		snapshot, resolveErr := w.Profiles.Resolve(ctx, key)
		if resolveErr != nil {
			return resolveErr
		}
		abortErr := w.Governance.Abort(ctx, envelope, governance.VersionedRef{ID: snapshot.ModelProfileRef.ID, Version: snapshot.ModelProfileRef.Version}, "cancelled")
		if abortErr != nil && !errors.Is(abortErr, runtime.ErrNotFound) {
			return abortErr
		}
	}
	sessionKey := sessionstore.SessionKey{TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID, SessionID: envelope.SessionID}
	head, err := w.Sessions.OpenForRun(ctx, sessionstore.OpenForRunRequest{
		SessionKey: sessionKey, RequestID: envelope.RequestID, InputSeq: envelope.InputSeq, Fence: fence,
	})
	if errors.Is(err, runtime.ErrAlreadyTerminal) {
		return nil
	}
	if err != nil {
		return err
	}
	if beforeCommit != nil {
		if err := beforeCommit(ctx); err != nil {
			return err
		}
	}
	_, err = w.Sessions.CommitTurn(ctx, sessionstore.CommitTurnRequest{
		SessionKey: sessionKey, RequestID: envelope.RequestID,
		CommitID: envelope.RequestID + ":cancelled", Stage: "terminal",
		InputSeq: envelope.InputSeq, Fence: fence, ExpectedVersion: head.Version,
		Outcome: runtime.OutcomeCancelled,
		Outbox: []sessionstore.OutboxEvent{{Kind: "audit", IdempotencyKey: "cancel-terminal:" + envelope.RequestID,
			PayloadRef: "execution://" + envelope.TenantID + "/" + envelope.RequestID, EventSeq: uint64(status.CancelVersion),
			TraceParent: telemetry.EffectiveTraceParent(ctx, envelope.TraceParent)}},
	})
	if errors.Is(err, runtime.ErrAlreadyTerminal) {
		return nil
	}
	return err
}

func verifyAuthoritativeEnvelope(trusted, delivered runtime.ExecutionEnvelope) error {
	if trusted.TenantID != delivered.TenantID || trusted.AgentAppID != delivered.AgentAppID ||
		trusted.RequestID != delivered.RequestID || trusted.SessionID != delivered.SessionID ||
		trusted.UserID != delivered.UserID || trusted.Channel != delivered.Channel {
		return runtime.ErrTenantScope
	}
	// time.Time is comparable as a Go struct, but its location/monotonic
	// representation can differ after a PostgreSQL -> JSON -> Redis hop even
	// when both values denote the same instant. Compare the envelope fields
	// structurally and use time.Time.Equal for the timestamp instead.
	trustedCreatedAt, deliveredCreatedAt := trusted.CreatedAt, delivered.CreatedAt
	trusted.CreatedAt, delivered.CreatedAt = time.Time{}, time.Time{}
	if trusted != delivered || !trustedCreatedAt.Equal(deliveredCreatedAt) {
		return runtime.ErrVersionMismatch
	}
	return nil
}

func (w RunnerExecutor) commitGovernanceTerminal(ctx context.Context, turn *sessionstore.BufferedTurn, envelope runtime.ExecutionEnvelope, head sessionstore.SessionHead,
	fence uint64, beforeCommit func(context.Context) error, outcome runtime.Outcome, decision governance.Decision) error {
	if turn != nil {
		_ = turn.Rollback(ctx)
	}
	if beforeCommit != nil {
		if err := beforeCommit(ctx); err != nil {
			return err
		}
	}
	_, err := w.Sessions.CommitTurn(ctx, sessionstore.CommitTurnRequest{SessionKey: head.SessionKey, RequestID: envelope.RequestID,
		CommitID: envelope.RequestID + ":" + string(outcome) + ":governance", Stage: "terminal", InputSeq: envelope.InputSeq, Fence: fence,
		ExpectedVersion: head.Version, Outcome: outcome, Outbox: []sessionstore.OutboxEvent{{Kind: "audit", IdempotencyKey: "governance:" + decision.DecisionID,
			PayloadRef: "governance://" + envelope.TenantID + "/" + decision.DecisionID, EventSeq: 1,
			TraceParent: telemetry.EffectiveTraceParent(ctx, envelope.TraceParent)}}})
	if errors.Is(err, runtime.ErrAlreadyTerminal) {
		return nil
	}
	return err
}

type runnerResult struct {
	Content        string
	Usage          governance.Usage
	ToolCalls      []model.ToolCall
	GraphInterrupt *graphContinuationCoordinate
}

type graphContinuationCoordinate struct {
	SchemaVersion int    `json:"schema_version"`
	Kind          string `json:"kind"`
	LineageID     string `json:"lineage_id"`
	CheckpointID  string `json:"checkpoint_id"`
	Namespace     string `json:"checkpoint_namespace"`
	TaskID        string `json:"task_id"`
	ToolCallID    string `json:"tool_call_id"`
	ToolName      string `json:"tool_name"`
}

const graphContinuationRefPrefix = "graph-continuation:v1:"

func consumeRunnerEvents(ctx context.Context, events <-chan *event.Event) (runnerResult, error) {
	var content strings.Builder
	var resultUsage governance.Usage
	var toolCalls []model.ToolCall
	var graphInterrupt *graphContinuationCoordinate
	usageEvents := make(map[string]struct{})
	completed := false
	for {
		select {
		case <-ctx.Done():
			return runnerResult{}, ctx.Err()
		case value, ok := <-events:
			if !ok {
				if !completed && graphInterrupt == nil {
					return runnerResult{}, runtime.ErrBackendUnavailable
				}
				return runnerResult{Content: content.String(), Usage: resultUsage, ToolCalls: toolCalls, GraphInterrupt: graphInterrupt}, nil
			}
			if value == nil {
				continue
			}
			if value.IsTerminalError() {
				return runnerResult{}, runtime.ErrBackendUnavailable
			}
			if coordinate, ok := graphContinuationFromEvent(value); ok {
				graphInterrupt = &coordinate
			}
			if value.Response != nil {
				if value.Usage != nil && value.ID != "" {
					if _, seen := usageEvents[value.ID]; !seen {
						usageEvents[value.ID] = struct{}{}
						resultUsage.InputTokens += int64(value.Usage.PromptTokens)
						resultUsage.OutputTokens += int64(value.Usage.CompletionTokens)
						resultUsage.CachedInputTokens += int64(value.Usage.PromptTokensDetails.CachedTokens)
					}
				}
				for _, choice := range value.Choices {
					if len(choice.Message.ToolCalls) != 0 {
						toolCalls = append([]model.ToolCall(nil), choice.Message.ToolCalls...)
					}
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

func graphContinuationFromEvent(value *event.Event) (graphContinuationCoordinate, bool) {
	if value == nil || value.Object != graph.ObjectTypeGraphPregelStep || value.StateDelta == nil {
		return graphContinuationCoordinate{}, false
	}
	var metadata graph.PregelStepMetadata
	if err := json.Unmarshal(value.StateDelta[graph.MetadataKeyPregel], &metadata); err != nil || metadata.InterruptValue == nil ||
		metadata.LineageID == "" || metadata.CheckpointID == "" || metadata.CheckpointNS == "" || metadata.InterruptKey == "" {
		return graphContinuationCoordinate{}, false
	}
	encoded, err := json.Marshal(metadata.InterruptValue)
	if err != nil {
		return graphContinuationCoordinate{}, false
	}
	var interrupt struct {
		SchemaVersion int    `json:"schema_version"`
		Kind          string `json:"kind"`
		ToolCallID    string `json:"tool_call_id"`
		ToolName      string `json:"tool_name"`
	}
	if err := json.Unmarshal(encoded, &interrupt); err != nil || interrupt.SchemaVersion != 1 || interrupt.Kind != "tool_confirmation" ||
		interrupt.ToolCallID == "" || interrupt.ToolName == "" {
		return graphContinuationCoordinate{}, false
	}
	return graphContinuationCoordinate{SchemaVersion: 1, Kind: "graph_tool_confirmation", LineageID: metadata.LineageID,
		CheckpointID: metadata.CheckpointID, Namespace: metadata.CheckpointNS, TaskID: metadata.InterruptKey,
		ToolCallID: interrupt.ToolCallID, ToolName: interrupt.ToolName}, true
}

func encodeGraphContinuationRef(value graphContinuationCoordinate) (string, error) {
	if err := validateGraphContinuationCoordinate(value); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", runtime.ErrInvariantViolation
	}
	return graphContinuationRefPrefix + base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeGraphContinuationRef(value string) (graphContinuationCoordinate, error) {
	if !strings.HasPrefix(value, graphContinuationRefPrefix) {
		return graphContinuationCoordinate{}, runtime.ErrVersionMismatch
	}
	encoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, graphContinuationRefPrefix))
	if err != nil || len(encoded) == 0 || len(encoded) > 8<<10 {
		return graphContinuationCoordinate{}, runtime.ErrInvalidEnvelope
	}
	var coordinate graphContinuationCoordinate
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&coordinate); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return graphContinuationCoordinate{}, runtime.ErrInvalidEnvelope
	}
	if err := validateGraphContinuationCoordinate(coordinate); err != nil {
		return graphContinuationCoordinate{}, err
	}
	return coordinate, nil
}

func validateGraphContinuationCoordinate(value graphContinuationCoordinate) error {
	if value.SchemaVersion != 1 || value.Kind != "graph_tool_confirmation" || value.LineageID == "" || value.CheckpointID == "" ||
		value.Namespace == "" || value.TaskID == "" || value.ToolCallID == "" || value.ToolName == "" || value.TaskID != value.ToolCallID {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func confirmationToolRef(ctx context.Context, resolver profile.ExecutionProfileResolver, snapshot profile.ExecutionProfileSnapshot,
	policy governance.PolicySnapshot, name string,
) (governance.VersionedRef, bool) {
	refs := make(map[governance.VersionedRef]struct{})
	if err := walkExecutionProfiles(ctx, resolver, snapshot, 0, make(map[profile.ExecutionProfileKey]bool),
		func(value profile.ExecutionProfileSnapshot) error {
			for _, candidate := range value.ToolRefs {
				if candidate.ID == name {
					refs[governance.VersionedRef{ID: candidate.ID, Version: candidate.Version}] = struct{}{}
				}
			}
			return nil
		}); err != nil || len(refs) != 1 {
		return governance.VersionedRef{}, false
	}
	for ref := range refs {
		if governance.ToolDecision(policy, ref).Action == governance.ActionAsk {
			return ref, true
		}
	}
	return governance.VersionedRef{}, false
}

func executionModelRef(ctx context.Context, resolver profile.ExecutionProfileResolver,
	snapshot profile.ExecutionProfileSnapshot,
) (governance.VersionedRef, error) {
	refs := make(map[governance.VersionedRef]struct{})
	err := walkExecutionProfiles(ctx, resolver, snapshot, 0, make(map[profile.ExecutionProfileKey]bool),
		func(value profile.ExecutionProfileSnapshot) error {
			if value.AgentKind != agentapp.AgentKindLLM {
				return nil
			}
			ref := governance.VersionedRef{ID: value.ModelProfileRef.ID, Version: value.ModelProfileRef.Version}
			if ref.ID == "" || ref.Version < 1 {
				return runtime.ErrVersionMismatch
			}
			refs[ref] = struct{}{}
			return nil
		})
	if err != nil {
		return governance.VersionedRef{}, err
	}
	if len(refs) != 1 {
		return governance.VersionedRef{}, runtime.ErrCapabilityUnsupported
	}
	for ref := range refs {
		return ref, nil
	}
	return governance.VersionedRef{}, runtime.ErrCapabilityUnsupported
}

func walkExecutionProfiles(ctx context.Context, resolver profile.ExecutionProfileResolver, snapshot profile.ExecutionProfileSnapshot,
	depth int, path map[profile.ExecutionProfileKey]bool, visit func(profile.ExecutionProfileSnapshot) error,
) error {
	if resolver == nil || visit == nil || depth > 32 || path[snapshot.Key] {
		return runtime.ErrInvariantViolation
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path[snapshot.Key] = true
	defer delete(path, snapshot.Key)
	if err := visit(snapshot); err != nil {
		return err
	}
	for _, node := range snapshot.AgentSpec.Nodes {
		child, err := resolveChildExecutionProfile(ctx, resolver, snapshot, node)
		if err != nil {
			return err
		}
		if err := walkExecutionProfiles(ctx, resolver, child, depth+1, path, visit); err != nil {
			return err
		}
	}
	return nil
}

func resolveChildExecutionProfile(ctx context.Context, resolver profile.ExecutionProfileResolver,
	parent profile.ExecutionProfileSnapshot, node agentapp.AgentNodeSpecV1,
) (profile.ExecutionProfileSnapshot, error) {
	if childResolver, ok := resolver.(profile.ChildExecutionProfileResolver); ok {
		return childResolver.ResolveChild(ctx, parent, node)
	}
	return resolver.Resolve(ctx, profile.ExecutionProfileKey{TenantID: parent.Key.TenantID,
		TenantVersion: parent.Key.TenantVersion, AgentAppID: node.AgentRef.AgentAppID,
		AgentAppRevision: node.AgentRef.Revision, ContentDigest: node.AgentRef.ContentDigest,
		ConfigVersion: parent.Key.ConfigVersion, PolicyVersion: parent.Key.PolicyVersion})
}

func confirmationBindingID(payload []byte) (string, error) {
	var value map[string]json.RawMessage
	canonical, _, err := governance.CanonicalArguments(payload)
	if err != nil {
		return "", runtime.ErrInvalidEnvelope
	}
	if err := json.Unmarshal(canonical, &value); err != nil {
		return "", runtime.ErrInvalidEnvelope
	}
	var bindingID string
	if err := json.Unmarshal(value["channel_binding_id"], &bindingID); err != nil || strings.TrimSpace(bindingID) == "" {
		return "", runtime.ErrCapabilityUnsupported
	}
	return bindingID, nil
}

func confirmedToolCall(ctx context.Context, sessions sessionstore.AtomicSessionStore, key sessionstore.SessionKey, confirmation governance.Confirmation) (model.ToolCall, error) {
	snapshot, err := sessions.LoadSession(ctx, key)
	if err != nil {
		return model.ToolCall{}, err
	}
	for eventIndex := len(snapshot.Events) - 1; eventIndex >= 0; eventIndex-- {
		var value event.Event
		if err := json.Unmarshal(snapshot.Events[eventIndex], &value); err != nil {
			return model.ToolCall{}, runtime.ErrInvariantViolation
		}
		if value.Response == nil {
			continue
		}
		for _, choice := range value.Choices {
			for _, call := range choice.Message.ToolCalls {
				if call.ID != confirmation.ToolCallID {
					continue
				}
				if call.Function.Name != confirmation.Tool.ID {
					return model.ToolCall{}, runtime.ErrVersionMismatch
				}
				_, digest, digestErr := governance.CanonicalArguments(call.Function.Arguments)
				if digestErr != nil || digest != confirmation.ArgsDigest {
					return model.ToolCall{}, runtime.ErrVersionMismatch
				}
				return call, nil
			}
		}
	}
	return model.ToolCall{}, runtime.ErrNotFound
}

func addUsage(target *governance.Usage, prior governance.Usage) error {
	if target == nil || prior.InputTokens < 0 || prior.OutputTokens < 0 || prior.CachedInputTokens < 0 ||
		target.InputTokens > math.MaxInt64-prior.InputTokens || target.OutputTokens > math.MaxInt64-prior.OutputTokens || target.CachedInputTokens > math.MaxInt64-prior.CachedInputTokens {
		return runtime.ErrInvariantViolation
	}
	target.InputTokens += prior.InputTokens
	target.OutputTokens += prior.OutputTokens
	target.CachedInputTokens += prior.CachedInputTokens
	if target.CachedInputTokens > target.InputTokens {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func (w RunnerExecutor) commitContinuationFailure(ctx context.Context, turn *sessionstore.BufferedTurn, envelope runtime.ExecutionEnvelope,
	head sessionstore.SessionHead, fence uint64, beforeCommit func(context.Context) error, modelRef governance.VersionedRef, reason string) error {
	if w.Governance != nil {
		if err := w.Governance.Abort(ctx, envelope, modelRef, reason); err != nil {
			return err
		}
	}
	decision := governance.Decision{DecisionID: governance.StableDecisionID(envelope.TenantID, envelope.RequestID, reason, envelope.PolicyVersion),
		TenantID: envelope.TenantID, RequestID: envelope.RequestID, Stage: "tool_continuation", Action: governance.ActionDeny,
		ReasonCode: reason, PolicyVersion: envelope.PolicyVersion}
	if w.Governance != nil {
		if err := w.Governance.Record(ctx, decision); err != nil {
			return err
		}
	}
	return w.commitGovernanceTerminal(ctx, turn, envelope, head, fence, beforeCommit, runtime.OutcomeFailed, decision)
}

type closableRunner interface{ Close() error }

// closeRunner gives a runner a bounded chance to stop while draining any
// terminal events it may still be trying to publish. Runner.Close has no
// context parameter and third-party implementations are allowed to block, so
// the worker must never wait indefinitely here.
func closeRunner(run closableRunner, events <-chan *event.Event, timeout time.Duration) {
	if run == nil {
		return
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	closeDone := make(chan struct{})
	go func() {
		_ = run.Close()
		close(closeDone)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-closeDone:
			return
		case _, ok := <-events:
			if !ok {
				select {
				case <-closeDone:
				case <-timer.C:
				}
				return
			}
		case <-timer.C:
			return
		}
	}
}

func drainRunnerEvents(events <-chan *event.Event, timeout time.Duration) {
	if timeout <= 0 {
		timeout = time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-timer.C:
			return
		}
	}
}

func (w RunnerExecutor) executionPayload(ctx context.Context, envelope runtime.ExecutionEnvelope) (messaging.PayloadRecord, error) {
	if strings.HasPrefix(envelope.PayloadRef, "prepared://") {
		prepared, ok := w.Payloads.(messaging.PreparedPayloadStore)
		if !ok {
			return messaging.PayloadRecord{}, runtime.ErrCapabilityUnsupported
		}
		return prepared.GetPreparedPayload(ctx, envelope.TenantID, envelope.RequestID, envelope.PayloadRef)
	}
	return w.Payloads.GetPayload(ctx, envelope.TenantID, envelope.RequestID)
}

func validPreparedMessage(message model.Message) bool {
	for _, part := range message.ContentParts {
		if part.ContentRef == nil || part.ContentRef.ArtifactRef == "" || part.ContentRef.ArtifactName == "" || part.ContentRef.ArtifactVersion < 1 {
			return false
		}
		switch part.Type {
		case model.ContentTypeImage:
			if part.Image == nil || len(part.Image.Data) != 0 || part.Image.URL != "" || part.File != nil {
				return false
			}
		case model.ContentTypeFile:
			if part.File == nil || len(part.File.Data) != 0 || part.File.URL != "" || part.File.FileID != "" || part.Image != nil {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func (w RunnerExecutor) hydrateArtifacts(ctx context.Context, envelope runtime.ExecutionEnvelope, message *model.Message) error {
	if len(message.ContentParts) == 0 {
		return nil
	}
	if w.Artifacts == nil {
		return runtime.ErrCapabilityUnsupported
	}
	for index := range message.ContentParts {
		part := &message.ContentParts[index]
		record, err := w.Artifacts.GetArtifact(ctx, envelope.TenantID, part.ContentRef.ArtifactName)
		if err != nil {
			return err
		}
		if record.RequestID != envelope.RequestID || record.ArtifactRef != part.ContentRef.ArtifactRef || record.ContentDigest != part.ContentRef.SHA256 || record.MediaType != part.ContentRef.MimeType || int64(len(record.Content)) != part.ContentRef.SizeBytes {
			return runtime.ErrVersionMismatch
		}
		switch part.Type {
		case model.ContentTypeImage:
			part.Image.Data = append([]byte(nil), record.Content...)
			part.Image.Format = strings.TrimPrefix(record.MediaType, "image/")
		case model.ContentTypeFile:
			part.File.Data = append([]byte(nil), record.Content...)
			part.File.MimeType = record.MediaType
		}
	}
	return nil
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

func (JSONTextInputDecoder) DecodeInput(ctx context.Context, envelope runtime.ExecutionEnvelope, payload []byte) (model.Message, error) {
	if err := ctx.Err(); err != nil {
		return model.Message{}, err
	}
	var value struct {
		SchemaVersion     uint16                     `json:"schema_version,omitempty"`
		ExternalMessageID string                     `json:"external_message_id,omitempty"`
		ExternalUserID    string                     `json:"external_user_id,omitempty"`
		ExternalChatID    string                     `json:"external_chat_id,omitempty"`
		ChannelBindingID  string                     `json:"channel_binding_id,omitempty"`
		ExternalAccountID string                     `json:"external_account_id,omitempty"`
		ConfigVersion     int64                      `json:"config_version,omitempty"`
		MessageType       string                     `json:"message_type,omitempty"`
		Text              string                     `json:"text,omitempty"`
		MediaRefs         []channel.MediaRef         `json:"media_refs,omitempty"`
		Media             []preprocess.PreparedMedia `json:"media,omitempty"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return model.Message{}, runtime.ErrInvalidEnvelope
	}
	if value.SchemaVersion != 0 && value.SchemaVersion != 1 {
		return model.Message{}, runtime.ErrInvalidEnvelope
	}
	// Normalized text payloads carry the frozen config version. It is part of
	// the authenticated execution contract and must agree with the envelope;
	// accepting a different version would allow a payload/profile mix-up.
	if value.ConfigVersion != 0 && value.ConfigVersion != envelope.ConfigVersion {
		return model.Message{}, runtime.ErrVersionMismatch
	}
	// Raw media references are an ingress/preprocess concern. Worker execution
	// only accepts PreparedInput media, whose artifact identities have already
	// passed malware/DLP checks and tenant-scoped hydration.
	if len(value.MediaRefs) > 0 {
		return model.Message{}, runtime.ErrInvalidEnvelope
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return model.Message{}, runtime.ErrInvalidEnvelope
	}
	message := model.NewUserMessage(strings.TrimSpace(value.Text))
	if len(value.Media) > 1 || (len(value.Media) > 0 && value.MessageType != "image" && value.MessageType != "file") {
		return model.Message{}, runtime.ErrInvalidEnvelope
	}
	for _, media := range value.Media {
		if media.ArtifactID == "" || media.ArtifactRef == "" || media.MediaType == "" || media.ContentDigest == "" || media.Size <= 0 ||
			(media.Kind != "image" && media.Kind != "file") {
			return model.Message{}, runtime.ErrInvalidEnvelope
		}
		if value.MessageType != media.Kind {
			return model.Message{}, runtime.ErrInvalidEnvelope
		}
		ref := &model.ContentRef{ArtifactRef: media.ArtifactRef, ArtifactName: media.ArtifactID, ArtifactVersion: 1,
			MimeType: media.MediaType, SizeBytes: media.Size, SHA256: media.ContentDigest}
		if media.Kind == "image" {
			message.ContentParts = append(message.ContentParts, model.ContentPart{Type: model.ContentTypeImage, Image: &model.Image{}, ContentRef: ref})
		} else {
			message.ContentParts = append(message.ContentParts, model.ContentPart{Type: model.ContentTypeFile, File: &model.File{}, ContentRef: ref})
		}
	}
	if strings.TrimSpace(message.Content) == "" && len(message.ContentParts) == 0 {
		return model.Message{}, runtime.ErrInvalidEnvelope
	}
	return message, nil
}

var _ LeaseExecutor = RunnerExecutor{}
var _ CancellationExecutor = RunnerExecutor{}
