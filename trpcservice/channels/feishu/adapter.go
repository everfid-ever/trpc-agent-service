// Package feishu implements the service-owned Feishu Channel Adapter.
package feishu

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu/protocol"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const routeKeyQuery = "route_key"

type TextSender interface {
	ReplyText(context.Context, channel.ReplyDestination, string, string, string) (string, error)
}

type Adapter struct {
	Protocol protocol.Verifier
	Sender   TextSender
}

func (a *Adapter) ID() string { return "feishu" }

func (a *Adapter) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (a *Adapter) PublicRoute(_ context.Context, request channel.CallbackRequest) (channel.PublicRouteHint, error) {
	routeKey := request.Query[routeKeyQuery]
	if routeKey == "" || len(request.Body) == 0 {
		return channel.PublicRouteHint{}, runtime.ErrInvalidEnvelope
	}
	timestamp := mapValue(request.Headers, "X-Lark-Request-Timestamp")
	nonce := mapValue(request.Headers, "X-Lark-Request-Nonce")
	if timestamp == "" || nonce == "" {
		return channel.PublicRouteHint{}, runtime.ErrInvalidEnvelope
	}
	attempt := sha256.Sum256(append([]byte(timestamp+"\x00"+nonce+"\x00"), request.Body...))
	return channel.PublicRouteHint{Channel: a.ID(), ExternalAccountHint: routeKey,
		RouteKeyDigest: protocol.RouteKeyDigest(routeKey), IngressAttemptID: hex.EncodeToString(attempt[:16])}, nil
}

func (a *Adapter) IsChallenge(request channel.CallbackRequest) bool {
	return protocol.IsChallengeRequest(request)
}

func (a *Adapter) PublicChallengeRoute(_ context.Context, request channel.CallbackRequest) (channel.PublicRouteHint, error) {
	routeKey := request.Query[routeKeyQuery]
	if routeKey == "" || len(request.Body) == 0 || !a.IsChallenge(request) {
		return channel.PublicRouteHint{}, runtime.ErrInvalidEnvelope
	}
	attempt := sha256.Sum256(append([]byte("feishu-challenge\x00"), request.Body...))
	return channel.PublicRouteHint{Channel: a.ID(), ExternalAccountHint: routeKey,
		RouteKeyDigest: protocol.RouteKeyDigest(routeKey), IngressAttemptID: hex.EncodeToString(attempt[:16])}, nil
}

func (a *Adapter) VerifyChallenge(ctx context.Context, request channel.CallbackRequest, handle channel.ScopedVerifierHandle) (channel.HTTPResponse, channel.VerificationReceipt, error) {
	if handle == nil {
		return channel.HTTPResponse{}, channel.VerificationReceipt{}, runtime.ErrInvariantViolation
	}
	callback, receipt, err := handle.Verify(ctx, request, a.Protocol.VerifyChallenge)
	if err != nil {
		return channel.HTTPResponse{}, channel.VerificationReceipt{}, err
	}
	return channel.HTTPResponse{ContentType: callback.Headers["content-type"], Body: callback.Body}, receipt, nil
}

func (a *Adapter) CallbackACK() channel.HTTPResponse {
	return channel.HTTPResponse{ContentType: "application/json", Body: []byte(`{"code":0}`)}
}

func (a *Adapter) Verify(ctx context.Context, request channel.CallbackRequest, handle channel.ScopedVerifierHandle) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	if handle == nil {
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, runtime.ErrInvariantViolation
	}
	return handle.Verify(ctx, request, a.Protocol.Verify)
}

func (a *Adapter) Decode(_ context.Context, callback channel.VerifiedCallback) ([]channel.ProviderEvent, error) {
	decoded, err := protocol.DecodeMessage(callback)
	if err != nil {
		return nil, err
	}
	return []channel.ProviderEvent{decoded.Event}, nil
}

func (a *Adapter) Deliver(ctx context.Context, request channel.DeliveryRequest) (channel.DeliveryResult, error) {
	if a.Sender == nil || request.Event.TenantID == "" || request.Event.ChannelBindingID == "" || request.Target.Channel != "feishu" ||
		request.Target.ExternalMessageID == "" || len(request.Content) == 0 || request.ClientRequestID == "" {
		return channel.DeliveryResult{}, runtime.ErrInvariantViolation
	}
	sum := sha256.Sum256(request.Content)
	if request.ContentDigest != hex.EncodeToString(sum[:]) {
		return channel.DeliveryResult{}, runtime.ErrVersionMismatch
	}
	destination := channel.ReplyDestination{TenantID: request.Event.TenantID, Channel: request.Target.Channel,
		ChannelBindingID: request.Event.ChannelBindingID, ExternalAccountID: request.Target.ExternalAccountID}
	messageID, err := a.Sender.ReplyText(ctx, destination, request.Target.ExternalMessageID, string(request.Content), request.ClientRequestID)
	if err != nil {
		return channel.DeliveryResult{}, err
	}
	if messageID == "" {
		return channel.DeliveryResult{}, channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	return channel.DeliveryResult{ProviderMessageID: messageID, Delivered: true}, nil
}

func (a *Adapter) Capabilities() channel.Capabilities { return channel.Capabilities{Text: true} }

type ClientCredentials struct {
	AppID, AppSecret string
	Version          int64
}

// CredentialsResolver must resolve app credentials under channel_send scope.
type CredentialsResolver interface {
	ResolveFeishuSendCredentials(context.Context, channel.ReplyDestination) (ClientCredentials, error)
}

// ClientProvider returns a shared SDK client and owns its credential generation.
// The pinned SDK currently has a process-global token cache, but rebuilding a
// client per delivery still loses explicit lifecycle and adds avoidable work.
type ClientProvider interface {
	ResolveFeishuClient(context.Context, channel.ReplyDestination) (*lark.Client, error)
}

type cachedClient struct {
	appID   string
	version int64
	client  *lark.Client
}

// ClientCache keeps one SDK client per binding and credential generation.
// Credential rotation replaces the old entry instead of growing the cache.
type ClientCache struct {
	Credentials CredentialsResolver
	NewClient   func(string, string) *lark.Client

	mu      sync.Mutex
	clients map[string]cachedClient
}

func (c *ClientCache) ResolveFeishuClient(ctx context.Context, destination channel.ReplyDestination) (*lark.Client, error) {
	if c == nil || c.Credentials == nil || destination.TenantID == "" || destination.ChannelBindingID == "" || destination.ExternalAccountID == "" {
		return nil, runtime.ErrInvariantViolation
	}
	credentials, err := c.Credentials.ResolveFeishuSendCredentials(ctx, destination)
	if err != nil {
		return nil, err
	}
	if credentials.AppID == "" || credentials.AppID != destination.ExternalAccountID || credentials.AppSecret == "" || credentials.Version < 1 {
		return nil, runtime.ErrVersionMismatch
	}
	key := destination.TenantID + "\x00" + destination.ChannelBindingID + "\x00" + destination.ExternalAccountID
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.clients[key]; ok {
		if existing.version > credentials.Version || (existing.version == credentials.Version && existing.appID != credentials.AppID) {
			return nil, runtime.ErrVersionMismatch
		}
		if existing.version == credentials.Version && existing.client != nil {
			return existing.client, nil
		}
	}
	newClient := c.NewClient
	if newClient == nil {
		newClient = func(id, secret string) *lark.Client { return lark.NewClient(id, secret) }
	}
	client := newClient(credentials.AppID, credentials.AppSecret)
	if client == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	if c.clients == nil {
		c.clients = make(map[string]cachedClient)
	}
	c.clients[key] = cachedClient{appID: credentials.AppID, version: credentials.Version, client: client}
	return client, nil
}

type OfficialSender struct {
	Clients ClientProvider
}

func (s OfficialSender) ReplyText(ctx context.Context, destination channel.ReplyDestination, replyMessageID, text, uuid string) (string, error) {
	if s.Clients == nil || destination.TenantID == "" || destination.ChannelBindingID == "" || replyMessageID == "" || text == "" || uuid == "" {
		return "", runtime.ErrInvariantViolation
	}
	client, err := s.Clients.ResolveFeishuClient(ctx, destination)
	if err != nil {
		return "", err
	}
	if client == nil {
		return "", runtime.ErrVersionMismatch
	}
	content, _ := json.Marshal(struct {
		Text string `json:"text"`
	}{Text: text})
	body := larkim.NewReplyMessageReqBodyBuilder().Content(string(content)).MsgType("text").Uuid(uuid).Build()
	req := larkim.NewReplyMessageReqBuilder().MessageId(replyMessageID).Body(body).Build()
	resp, err := client.Im.Message.Reply(ctx, req)
	if err != nil {
		return "", channel.AmbiguousDeliveryError{Err: err}
	}
	if resp == nil {
		return "", channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	if !resp.Success() {
		status := 0
		if resp.ApiResp != nil {
			status = resp.ApiResp.StatusCode
		}
		providerErr := fmt.Errorf("feishu reply code=%d status=%d: %s", resp.Code, status, resp.Msg)
		switch {
		case resp.Code == 99991400 || resp.Code == 99991401 || status == 429:
			delay := time.Second
			if resp.ApiResp != nil {
				if seconds, parseErr := strconv.Atoi(resp.ApiResp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
					delay = time.Duration(seconds) * time.Second
				}
			}
			return "", channel.RetryableDeliveryError{Err: providerErr, RetryAfter: delay}
		case resp.Code == 99991663 || resp.Code == 99991664 || resp.Code == 99991671 || status == 408 || status >= 500:
			return "", channel.RetryableDeliveryError{Err: providerErr}
		default:
			return "", channel.PermanentDeliveryError{Err: providerErr, Class: "provider_rejected"}
		}
	}
	if resp.Data == nil || resp.Data.MessageId == nil || *resp.Data.MessageId == "" {
		return "", channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	return *resp.Data.MessageId, nil
}

func mapValue(values map[string]string, name string) string {
	for key, value := range values {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}

var (
	_ channel.Adapter     = (*Adapter)(nil)
	_ channel.HTTPAdapter = (*Adapter)(nil)
)
