// Package protocol implements Feishu webhook verification and strict message
// decoding. It does not know tenant, Inbox, Session, or Gateway concepts.
package protocol

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const EventTypeMessageReceive = "im.message.receive_v1"

type VerificationMaterial struct {
	EncryptKey, VerificationToken, AppID, BotOpenID string
}

type Verifier struct {
	Now     func() time.Time
	MaxSkew time.Duration
}

func (v Verifier) Verify(_ context.Context, request channel.CallbackRequest, secret []byte) (channel.VerifiedProtocolPayload, error) {
	var material VerificationMaterial
	decoder := json.NewDecoder(strings.NewReader(string(secret)))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&material)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if decodeErr != nil || trailingErr != io.EOF || material.EncryptKey == "" || material.VerificationToken == "" ||
		material.AppID == "" || material.BotOpenID == "" || len(request.Body) == 0 {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	timestamp := header(request.Headers, larkevent.EventRequestTimestamp)
	nonce := header(request.Headers, larkevent.EventRequestNonce)
	signature := header(request.Headers, larkevent.EventSignature)
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || nonce == "" || signature == "" {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}
	now := time.Now().UTC()
	if v.Now != nil {
		now = v.Now().UTC()
	}
	maxSkew := v.MaxSkew
	if maxSkew <= 0 {
		maxSkew = 5 * time.Minute
	}
	requestTime := time.Unix(seconds, 0).UTC()
	if requestTime.Before(now.Add(-maxSkew)) || requestTime.After(now.Add(maxSkew)) {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}
	expected := larkevent.Signature(timestamp, nonce, material.EncryptKey, string(request.Body))
	if len(signature) != len(expected) || subtle.ConstantTimeCompare([]byte(signature), []byte(expected)) != 1 {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	var wrapper larkevent.EventEncryptMsg
	if err := json.Unmarshal(request.Body, &wrapper); err != nil || wrapper.Encrypt == "" {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}
	plaintext, err := larkevent.EventDecrypt(wrapper.Encrypt, material.EncryptKey)
	if err != nil {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}
	var envelope struct {
		Schema string                 `json:"schema"`
		Header *larkevent.EventHeader `json:"header"`
	}
	if err := json.Unmarshal(plaintext, &envelope); err != nil || envelope.Schema != "2.0" || envelope.Header == nil ||
		envelope.Header.Token != material.VerificationToken || envelope.Header.AppID != material.AppID ||
		envelope.Header.EventID == "" || envelope.Header.EventType != EventTypeMessageReceive || envelope.Header.TenantKey == "" {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	identity := digest(material.AppID, envelope.Header.TenantKey, envelope.Header.EventID)
	return channel.VerifiedProtocolPayload{Body: plaintext, Headers: map[string]string{"x-feishu-bot-open-id": material.BotOpenID}, ProtocolIdentityDigest: identity}, nil
}

type DecodeResult struct {
	Event   channel.ProviderEvent
	Ignored bool
}

func DecodeMessage(payload channel.VerifiedCallback) (DecodeResult, error) {
	var event larkim.P2MessageReceiveV1
	if err := json.Unmarshal(payload.Body, &event); err != nil || event.EventV2Base == nil || event.EventV2Base.Header == nil ||
		event.Event == nil || event.Event.Message == nil || event.Event.Sender == nil || event.Event.Sender.SenderId == nil {
		return DecodeResult{}, runtime.ErrInvalidEnvelope
	}
	headerValue := event.EventV2Base.Header
	message, sender := event.Event.Message, event.Event.Sender
	if headerValue.EventType != EventTypeMessageReceive || headerValue.EventID == "" || headerValue.AppID == "" ||
		message.MessageId == nil || *message.MessageId == "" || message.ChatId == nil || *message.ChatId == "" ||
		message.ChatType == nil || message.MessageType == nil || *message.MessageType != "text" || message.Content == nil ||
		sender.SenderId.OpenId == nil || *sender.SenderId.OpenId == "" || sender.TenantKey == nil || *sender.TenantKey == "" {
		return DecodeResult{}, runtime.ErrInvalidEnvelope
	}
	if *message.ChatType != "p2p" && *message.ChatType != "group" {
		return DecodeResult{}, runtime.ErrInvalidEnvelope
	}
	botOpenID := header(payload.Headers, "x-feishu-bot-open-id")
	if botOpenID == "" {
		return DecodeResult{}, runtime.ErrVersionMismatch
	}
	text, mentionedBot, err := normalizeText(*message.Content, message.Mentions, botOpenID)
	if err != nil {
		return DecodeResult{}, err
	}
	ignored := *message.ChatType == "group" && (!mentionedBot || strings.TrimSpace(text) == "")
	if ignored {
		text = ""
	}
	occurredAt, err := eventTime(message.CreateTime, headerValue.CreateTime)
	if err != nil {
		return DecodeResult{}, err
	}
	return DecodeResult{Ignored: ignored, Event: channel.ProviderEvent{
		SchemaVersion: 1, Channel: "feishu", ExternalAccountID: headerValue.AppID,
		ExternalMessageID: *message.MessageId, ConversationType: *message.ChatType,
		ExternalUserID: *sender.SenderId.OpenId, ExternalChatID: *message.ChatId,
		MessageType: "text", Text: text, OccurredAt: occurredAt,
	}}, nil
}

func normalizeText(content string, mentions []*larkim.MentionEvent, botOpenID string) (string, bool, error) {
	var body struct {
		Text string `json:"text"`
	}
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&body)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if decodeErr != nil || trailingErr != io.EOF || body.Text == "" {
		return "", false, runtime.ErrInvalidEnvelope
	}
	type span struct {
		start, end  int
		replacement string
		bot         bool
	}
	spans := make([]span, 0, len(mentions))
	keys := make(map[string]struct{}, len(mentions))
	for _, mention := range mentions {
		if mention == nil || mention.Key == nil || *mention.Key == "" || mention.TenantKey == nil || *mention.TenantKey == "" {
			return "", false, runtime.ErrInvalidEnvelope
		}
		key := *mention.Key
		if _, exists := keys[key]; exists || strings.Count(body.Text, key) != 1 {
			return "", false, runtime.ErrInvalidEnvelope
		}
		keys[key] = struct{}{}
		start := strings.Index(body.Text, key)
		isAll := key == "@_all" || key == "@all"
		openID := ""
		if mention.Id != nil && mention.Id.OpenId != nil {
			openID = *mention.Id.OpenId
		}
		if !isAll && openID == "" {
			return "", false, runtime.ErrInvalidEnvelope
		}
		name := ""
		if mention.Name != nil {
			name = strings.TrimSpace(*mention.Name)
		}
		isBot := openID == botOpenID
		replacement := ""
		if !isBot {
			if isAll {
				replacement = "@所有人"
			} else if name != "" {
				replacement = "@" + name
			} else {
				return "", false, runtime.ErrInvalidEnvelope
			}
		}
		spans = append(spans, span{start: start, end: start + len(key), replacement: replacement, bot: isBot})
	}
	for _, unresolved := range []string{"@_user_", "@_all", "@all"} {
		if strings.Contains(body.Text, unresolved) {
			found := false
			for key := range keys {
				if strings.Contains(key, unresolved) {
					found = true
				}
			}
			if !found {
				return "", false, runtime.ErrInvalidEnvelope
			}
		}
	}
	for i := 1; i < len(spans); i++ {
		for j := i; j > 0 && spans[j].start < spans[j-1].start; j-- {
			spans[j], spans[j-1] = spans[j-1], spans[j]
		}
	}
	var out strings.Builder
	position, mentionedBot := 0, false
	for _, current := range spans {
		if current.start < position {
			return "", false, runtime.ErrInvalidEnvelope
		}
		out.WriteString(body.Text[position:current.start])
		out.WriteString(current.replacement)
		position = current.end
		mentionedBot = mentionedBot || current.bot
	}
	out.WriteString(body.Text[position:])
	normalized := strings.TrimSpace(out.String())
	if strings.Contains(normalized, "@_user_") || strings.Contains(normalized, "@_all") || strings.Contains(normalized, "@all") {
		return "", false, runtime.ErrInvalidEnvelope
	}
	return normalized, mentionedBot, nil
}

func eventTime(messageTime *string, headerTime string) (time.Time, error) {
	value := headerTime
	if messageTime != nil && *messageTime != "" {
		value = *messageTime
	}
	millis, err := strconv.ParseInt(value, 10, 64)
	if err != nil || millis <= 0 {
		return time.Time{}, runtime.ErrInvalidEnvelope
	}
	return time.UnixMilli(millis).UTC(), nil
}

func header(headers map[string]string, name string) string {
	for key, value := range headers {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}

func digest(fields ...string) string {
	hash := sha256.New()
	for _, field := range fields {
		_, _ = fmt.Fprintf(hash, "%d:%s", len(field), field)
	}
	return base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}

func RouteKeyDigest(routeKey string) string { return digest("feishu-route-v1", routeKey) }
