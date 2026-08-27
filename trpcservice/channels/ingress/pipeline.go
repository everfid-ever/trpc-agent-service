package ingress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type IdentityMapper interface {
	Map(context.Context, channel.VerifiedBinding, channel.ProviderEvent) (identity.Result, error)
}

type Verifier interface {
	VerifyAndDecode(context.Context, channel.CallbackRequest) (VerifiedIngress, error)
}

type AcceptedEvent struct {
	RequestID, PayloadRef, PreprocessJobID string
}

// Pipeline persists every verified event before returning. Payload persistence
// happens before the atomic Inbox+PreprocessJob claim; an interrupted attempt
// is safely completed by the provider's duplicate callback.
type Pipeline struct {
	Verification Verifier
	Identity     IdentityMapper
	Intake       preprocess.Store
	Payloads     messaging.PayloadStore
	KeyVersion   int64
}

func (p Pipeline) Accept(ctx context.Context, request channel.CallbackRequest) ([]AcceptedEvent, error) {
	if p.Verification == nil || p.Identity == nil || p.Intake == nil || p.Payloads == nil {
		return nil, runtime.ErrInvariantViolation
	}
	verified, err := p.Verification.VerifyAndDecode(ctx, request)
	if err != nil {
		return nil, err
	}
	keyVersion := p.KeyVersion
	if keyVersion < 1 {
		keyVersion = 1
	}
	accepted := make([]AcceptedEvent, 0, len(verified.Events))
	for _, event := range verified.Events {
		if event.Channel != verified.Binding.Channel || event.ExternalAccountID != verified.Binding.ExternalAccountID ||
			event.MessageType != "text" || len(event.MediaRefs) != 0 {
			return nil, runtime.ErrInvalidEnvelope
		}
		ids, err := p.Identity.Map(ctx, verified.Binding, event)
		if err != nil {
			return nil, err
		}
		normalized, err := json.Marshal(preprocess.NormalizedText{ExternalMessageID: event.ExternalMessageID,
			ExternalUserID: event.ExternalUserID, ExternalChatID: event.ExternalChatID, Text: event.Text})
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(normalized)
		key := messaging.InboxKey{TenantID: verified.Binding.TenantID, Channel: event.Channel,
			ExternalAccountID: event.ExternalAccountID, ExternalMessageID: event.ExternalMessageID}
		requestID, payloadRef := messaging.StableInboxIdentity(key)
		contentDigest := hex.EncodeToString(digest[:])
		if err := p.Payloads.PutPayload(ctx, messaging.PayloadRecord{TenantID: key.TenantID, RequestID: requestID,
			PayloadRef: payloadRef, ContentDigest: contentDigest, Content: normalized, KeyVersion: keyVersion}); err != nil {
			return nil, err
		}
		inbox, job, err := p.Intake.ClaimInboxAndSchedule(ctx, preprocess.ClaimRequest{
			Inbox: messaging.ClaimInboxRequest{InboxKey: key, AgentAppID: verified.Binding.AgentAppID, SessionID: ids.SessionID,
				ExternalChatID: event.ExternalChatID, ExternalUserID: event.ExternalUserID, PayloadDigest: contentDigest,
				KeyVersion: keyVersion, InitialState: messaging.InboxPreprocessPending},
			TenantVersion: verified.Binding.TenantVersion, UserID: ids.UserID, TraceParent: event.TraceParent,
		})
		if err != nil {
			return nil, err
		}
		if inbox.RequestID != requestID || inbox.PayloadRef != payloadRef || job.RequestID != requestID || job.PayloadRef != payloadRef {
			return nil, runtime.ErrIdempotencyCollision
		}
		accepted = append(accepted, AcceptedEvent{RequestID: requestID, PayloadRef: payloadRef, PreprocessJobID: job.JobID})
	}
	return accepted, nil
}
