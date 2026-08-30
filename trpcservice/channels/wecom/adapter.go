// Package wecom implements the service-owned WeCom Channel Adapter.
package wecom

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom/protocol"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const (
	routeKeyQuery       = "route_key"
	defaultAPIBaseURL   = "https://qyapi.weixin.qq.com"
	maxTextContentBytes = 2048
)

type TextSender interface {
	SendText(context.Context, channel.ReplyDestination, string, string, string) (string, error)
}

type Adapter struct {
	Protocol protocol.Verifier
	Sender   TextSender
}

func (a *Adapter) ID() string { return "wecom" }

func (a *Adapter) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (a *Adapter) PublicRoute(_ context.Context, request channel.CallbackRequest) (channel.PublicRouteHint, error) {
	routeKey := mapValue(request.Query, routeKeyQuery)
	timestamp := mapValue(request.Query, "timestamp")
	nonce := mapValue(request.Query, "nonce")
	signature := mapValue(request.Query, "msg_signature")
	if routeKey == "" || timestamp == "" || nonce == "" || signature == "" || len(request.Body) == 0 {
		return channel.PublicRouteHint{}, runtime.ErrInvalidEnvelope
	}
	attempt := sha256.Sum256(append([]byte(timestamp+"\x00"+nonce+"\x00"+signature+"\x00"), request.Body...))
	return channel.PublicRouteHint{Channel: a.ID(), ExternalAccountHint: routeKey,
		RouteKeyDigest: protocol.RouteKeyDigest(routeKey), IngressAttemptID: hex.EncodeToString(attempt[:16])}, nil
}

func (a *Adapter) IsChallenge(request channel.CallbackRequest) bool {
	return protocol.IsChallengeRequest(request)
}

func (a *Adapter) PublicChallengeRoute(_ context.Context, request channel.CallbackRequest) (channel.PublicRouteHint, error) {
	routeKey := mapValue(request.Query, routeKeyQuery)
	timestamp := mapValue(request.Query, "timestamp")
	nonce := mapValue(request.Query, "nonce")
	signature := mapValue(request.Query, "msg_signature")
	echo := mapValue(request.Query, "echostr")
	if routeKey == "" || timestamp == "" || nonce == "" || signature == "" || echo == "" {
		return channel.PublicRouteHint{}, runtime.ErrInvalidEnvelope
	}
	attempt := sha256.Sum256([]byte("wecom-challenge\x00" + timestamp + "\x00" + nonce + "\x00" + signature + "\x00" + echo))
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
	return channel.HTTPResponse{ContentType: "text/plain; charset=utf-8", Body: []byte("success")}
}

func (a *Adapter) Verify(ctx context.Context, request channel.CallbackRequest, handle channel.ScopedVerifierHandle) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	if handle == nil {
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, runtime.ErrInvariantViolation
	}
	return handle.Verify(ctx, request, a.Protocol.Verify)
}

func (a *Adapter) Decode(_ context.Context, callback channel.VerifiedCallback) ([]channel.ProviderEvent, error) {
	event, err := protocol.DecodeMessage(callback)
	if err != nil {
		return nil, err
	}
	return []channel.ProviderEvent{event}, nil
}

func (a *Adapter) Deliver(ctx context.Context, request channel.DeliveryRequest) (channel.DeliveryResult, error) {
	if a.Sender == nil || request.Event.TenantID == "" || request.Event.ChannelBindingID == "" ||
		request.Target.Channel != "wecom" || request.Target.ExternalAccountID == "" || request.Target.ExternalUserID == "" ||
		len(request.Content) == 0 || request.ClientRequestID == "" {
		return channel.DeliveryResult{}, runtime.ErrInvariantViolation
	}
	sum := sha256.Sum256(request.Content)
	if request.ContentDigest != hex.EncodeToString(sum[:]) {
		return channel.DeliveryResult{}, runtime.ErrVersionMismatch
	}
	destination := channel.ReplyDestination{TenantID: request.Event.TenantID, Channel: request.Target.Channel,
		ChannelBindingID: request.Event.ChannelBindingID, ExternalAccountID: request.Target.ExternalAccountID,
		ConfigVersion: request.Event.ConfigVersion}
	messageID, err := a.Sender.SendText(ctx, destination, request.Target.ExternalUserID, string(request.Content), request.ClientRequestID)
	if err != nil {
		return channel.DeliveryResult{}, err
	}
	if messageID == "" {
		return channel.DeliveryResult{}, channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	return channel.DeliveryResult{ProviderMessageID: messageID, Delivered: true}, nil
}

func (a *Adapter) Capabilities() channel.Capabilities { return channel.Capabilities{Text: true} }

// AccessTokenProvider owns credential resolution and the shared token cache.
// forceRefresh is true only after WeCom explicitly rejects the first token.
type AccessTokenProvider interface {
	ResolveWeComAccessToken(context.Context, channel.ReplyDestination, bool) (accessToken string, agentID int64, err error)
}

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type OfficialSender struct {
	Tokens  AccessTokenProvider
	Client  HTTPClient
	BaseURL string
}

func (s OfficialSender) SendText(ctx context.Context, destination channel.ReplyDestination, externalUserID, text, clientRequestID string) (string, error) {
	if s.Tokens == nil || destination.TenantID == "" || destination.ChannelBindingID == "" || destination.ExternalAccountID == "" || destination.ConfigVersion < 1 ||
		externalUserID == "" || text == "" || clientRequestID == "" {
		return "", runtime.ErrInvariantViolation
	}
	if !utf8.ValidString(text) || len([]byte(text)) > maxTextContentBytes {
		return "", channel.PermanentDeliveryError{Err: runtime.ErrInvalidEnvelope, Class: "invalid_text"}
	}
	for attempt := 0; attempt < 2; attempt++ {
		token, agentID, err := s.Tokens.ResolveWeComAccessToken(ctx, destination, attempt == 1)
		if err != nil {
			return "", err
		}
		if token == "" || agentID <= 0 {
			return "", runtime.ErrVersionMismatch
		}
		messageID, invalidToken, err := s.send(ctx, token, agentID, externalUserID, text)
		if invalidToken && attempt == 0 {
			continue
		}
		return messageID, err
	}
	return "", channel.RetryableDeliveryError{Err: runtime.ErrBackendUnavailable}
}

func (s OfficialSender) send(ctx context.Context, token string, agentID int64, externalUserID, text string) (string, bool, error) {
	body, err := json.Marshal(struct {
		ToUser      string `json:"touser"`
		MessageType string `json:"msgtype"`
		AgentID     int64  `json:"agentid"`
		Text        struct {
			Content string `json:"content"`
		} `json:"text"`
		Safe                   int `json:"safe"`
		EnableDuplicateCheck   int `json:"enable_duplicate_check"`
		DuplicateCheckInterval int `json:"duplicate_check_interval"`
	}{ToUser: externalUserID, MessageType: "text", AgentID: agentID, Text: struct {
		Content string `json:"content"`
	}{Content: text}, EnableDuplicateCheck: 1, DuplicateCheckInterval: 1800})
	if err != nil {
		return "", false, err
	}
	baseURL := strings.TrimRight(s.BaseURL, "/")
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	endpoint, err := url.Parse(baseURL + "/cgi-bin/message/send")
	if err != nil {
		return "", false, runtime.ErrInvariantViolation
	}
	query := endpoint.Query()
	query.Set("access_token", token)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return "", false, err
	}
	request.Header.Set("Content-Type", "application/json")
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return "", false, ctx.Err()
		}
		// A net/http url.Error includes the full URL, including access_token.
		// Preserve the ambiguous classification without allowing that secret to
		// escape through logs or a Delivery Ledger error string.
		return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	if response == nil {
		return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if readErr != nil {
		return "", false, channel.AmbiguousDeliveryError{Err: readErr}
	}
	if response.StatusCode == http.StatusTooManyRequests {
		return "", false, channel.RetryableDeliveryError{Err: providerError(response.StatusCode, 0, "rate limited"), RetryAfter: retryAfter(response.Header)}
	}
	if response.StatusCode >= 500 {
		return "", false, channel.RetryableDeliveryError{Err: providerError(response.StatusCode, 0, "server error")}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", false, channel.PermanentDeliveryError{Err: providerError(response.StatusCode, 0, "http request rejected"), Class: "provider_rejected"}
	}
	var result struct {
		ErrorCode      int64  `json:"errcode"`
		ErrorMessage   string `json:"errmsg"`
		MessageID      string `json:"msgid"`
		InvalidUser    string `json:"invaliduser"`
		UnlicensedUser string `json:"unlicenseduser"`
	}
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	if err := decoder.Decode(&result); err != nil {
		return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrInvalidEnvelope}
	}
	if result.ErrorCode == 0 {
		if result.InvalidUser != "" || result.UnlicensedUser != "" {
			return "", false, channel.PermanentDeliveryError{Err: providerError(response.StatusCode, 0, "recipient rejected"), Class: "invalid_recipient"}
		}
		if result.MessageID == "" {
			return "", false, channel.AmbiguousDeliveryError{Err: runtime.ErrBackendUnavailable}
		}
		return result.MessageID, false, nil
	}
	err = providerError(response.StatusCode, result.ErrorCode, result.ErrorMessage)
	switch result.ErrorCode {
	case 40014, 42001, 42007:
		return "", true, channel.RetryableDeliveryError{Err: err}
	case -1, 45002, 45009, 45011:
		return "", false, channel.RetryableDeliveryError{Err: err}
	default:
		return "", false, channel.PermanentDeliveryError{Err: err, Class: "provider_rejected"}
	}
}

func providerError(status int, code int64, message string) error {
	return fmt.Errorf("wecom send status=%d errcode=%d: %s", status, code, message)
}

func retryAfter(headers http.Header) time.Duration {
	seconds, err := strconv.Atoi(headers.Get("Retry-After"))
	if err != nil || seconds <= 0 {
		return time.Second
	}
	return time.Duration(seconds) * time.Second
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
