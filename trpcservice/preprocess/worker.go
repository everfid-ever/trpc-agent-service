package preprocess

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type NormalizedInput struct {
	ExternalMessageID string             `json:"external_message_id"`
	ExternalUserID    string             `json:"external_user_id"`
	ExternalChatID    string             `json:"external_chat_id"`
	ChannelBindingID  string             `json:"channel_binding_id,omitempty"`
	ExternalAccountID string             `json:"external_account_id,omitempty"`
	MessageType       string             `json:"message_type,omitempty"`
	Text              string             `json:"text,omitempty"`
	MediaRefs         []channel.MediaRef `json:"media_refs,omitempty"`
}

// NormalizedText preserves the source-compatible name used by the initial
// text-only fixtures while sharing the version-one normalized input schema.
type NormalizedText = NormalizedInput

type Worker struct {
	Store       Store
	Payloads    messaging.PayloadStore
	Dispatcher  gateway.Dispatcher
	Owner       string
	LeaseTTL    time.Duration
	RetryDelay  time.Duration
	MaxAttempts int
	Now         func() time.Time
}

func (w Worker) RunOnce(ctx context.Context, limit int) (int, error) {
	if w.Store == nil || w.Payloads == nil || w.Dispatcher == nil || w.Owner == "" || limit < 1 {
		return 0, runtime.ErrInvariantViolation
	}
	now := w.now()
	ttl := w.LeaseTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	jobs, err := w.Store.ClaimJobs(ctx, ClaimOptions{Owner: w.Owner, Now: now, TTL: ttl, Limit: limit})
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, job := range jobs {
		processed++
		if err := w.preprocess(ctx, job); err != nil {
			return processed, err
		}
	}
	ready, err := w.Store.ClaimReadyForDispatch(ctx, ClaimOptions{Owner: w.Owner, Now: w.now(), TTL: ttl, Limit: limit})
	if err != nil {
		return processed, err
	}
	for _, job := range ready {
		if err := w.dispatch(ctx, job); err != nil {
			return processed, err
		}
	}
	return processed, nil
}

func (w Worker) preprocess(ctx context.Context, job Job) error {
	payload, err := w.Payloads.GetPayload(ctx, job.TenantID, job.RequestID)
	if err != nil {
		return w.retry(ctx, job, "payload_unavailable")
	}
	sum := sha256.Sum256(payload.Content)
	if payload.PayloadRef != job.PayloadRef || payload.ContentDigest != hex.EncodeToString(sum[:]) {
		_, finishErr := w.Store.FinishRejected(ctx, job, "payload_integrity")
		return finishErr
	}
	var normalized NormalizedInput
	decoder := json.NewDecoder(strings.NewReader(string(payload.Content)))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&normalized)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if decodeErr != nil || !errors.Is(trailingErr, io.EOF) || !validNormalizedInput(normalized) {
		_, finishErr := w.Store.FinishRejected(ctx, job, "invalid_text_payload")
		return finishErr
	}
	// A provider media identifier is an opaque credential-scoped reference, not
	// executable input. Keep the job recoverable until the staged ArtifactRef is
	// persisted as the prepared payload consumed by dispatch.
	if len(normalized.MediaRefs) > 0 {
		return w.retry(ctx, job, "media_prepared_payload_unavailable")
	}
	_, err = w.Store.FinishReady(ctx, job)
	return err
}

func validNormalizedInput(value NormalizedInput) bool {
	if value.ExternalMessageID == "" || value.ExternalUserID == "" || len(value.MediaRefs) > 1 {
		return false
	}
	messageType := value.MessageType
	if messageType == "" {
		messageType = "text"
	}
	switch messageType {
	case "text":
		return strings.TrimSpace(value.Text) != "" && len(value.MediaRefs) == 0
	case "image", "file":
		return strings.TrimSpace(value.Text) == "" && value.ChannelBindingID != "" && value.ExternalAccountID != "" &&
			len(value.MediaRefs) == 1 && value.MediaRefs[0].ID != "" &&
			value.MediaRefs[0].Kind == messageType && value.MediaRefs[0].Size >= 0
	default:
		return false
	}
}

func (w Worker) retry(ctx context.Context, job Job, reason string) error {
	max := w.MaxAttempts
	if max <= 0 {
		max = 8
	}
	if job.Attempt >= max {
		_, err := w.Store.FinishRejected(ctx, job, "retry_exhausted:"+reason)
		return err
	}
	delay := w.RetryDelay
	if delay <= 0 {
		delay = time.Second
	}
	_, err := w.Store.FinishRetry(ctx, job, w.now().Add(delay), reason)
	return err
}

func (w Worker) dispatch(ctx context.Context, job Job) error {
	_, err := w.Dispatcher.Dispatch(ctx, gateway.DispatchRequest{
		Tenant: tenant.Context{TenantID: job.TenantID, TenantVersion: job.TenantVersion, AgentAppID: job.AgentAppID,
			SubjectID: job.UserID, Channel: job.Channel, TrustedSource: "verified-channel-ingress"},
		RequestID: job.RequestID, SessionID: job.SessionID, UserID: job.UserID, PayloadRef: job.PayloadRef, TraceParent: job.TraceParent,
	})
	if err != nil {
		return err
	}
	_, err = w.Store.MarkDispatched(ctx, job, w.now())
	return err
}

func (w Worker) now() time.Time {
	if w.Now != nil {
		return w.Now().UTC()
	}
	return time.Now().UTC()
}
