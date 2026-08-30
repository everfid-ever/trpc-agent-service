package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
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
	Tasks             gateway.TaskStore
	Profiles          profile.ExecutionProfileResolver
	Bundles           profile.RuntimeBundleManager
	Sessions          sessionstore.AtomicSessionStore
	Payloads          messaging.PayloadStore
	Artifacts         artifact.Store
	Inputs            InputDecoder
	EncodeEvent       sessionstore.EventRefEncoder
	EncodeResult      ResultRefEncoder
	EventDrainTimeout time.Duration
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

	lease, err := w.Bundles.Acquire(ctx, key)
	if err != nil {
		return err
	}
	defer lease.Release()
	run, err := lease.Bundle().NewRunner(turn.SessionService())
	if err != nil {
		return err
	}
	var events <-chan *event.Event
	runnerClosed := false
	defer func() {
		if !runnerClosed {
			closeRunner(run, events, w.EventDrainTimeout)
		}
	}()

	runCtx := runtime.WithExecutionContext(ctx, runtime.ExecutionContext{TenantID: envelope.TenantID, RequestID: envelope.RequestID})
	events, err = run.Run(runCtx, envelope.UserID, envelope.SessionID, message,
		agentcore.WithAppName(appName), agentcore.WithRequestID(envelope.RequestID))
	if err != nil {
		return err
	}
	content, err := consumeRunnerEvents(ctx, events)
	if err != nil {
		closeRunner(run, events, w.EventDrainTimeout)
		runnerClosed = true
		return err
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

// CancelWithLease turns a durable cancellation intent into the only
// authoritative cancelled terminal: a fenced CommitTurn that advances the
// session input gate and emits an audit fact.
func (w RunnerExecutor) CancelWithLease(ctx context.Context, envelope runtime.ExecutionEnvelope, fence uint64, beforeCommit func(context.Context) error) error {
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
			PayloadRef: "execution://" + envelope.TenantID + "/" + envelope.RequestID, EventSeq: uint64(status.CancelVersion), TraceParent: envelope.TraceParent}},
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

func (JSONTextInputDecoder) DecodeInput(ctx context.Context, _ runtime.ExecutionEnvelope, payload []byte) (model.Message, error) {
	if err := ctx.Err(); err != nil {
		return model.Message{}, err
	}
	var value struct {
		SchemaVersion     uint16                     `json:"schema_version,omitempty"`
		ExternalMessageID string                     `json:"external_message_id,omitempty"`
		ExternalUserID    string                     `json:"external_user_id,omitempty"`
		ExternalChatID    string                     `json:"external_chat_id,omitempty"`
		MessageType       string                     `json:"message_type,omitempty"`
		Text              string                     `json:"text,omitempty"`
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
