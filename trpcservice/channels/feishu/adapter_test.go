package feishu_test

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/contracttest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu/protocol"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	ingressmemory "github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress/inmemory"
	preprocessmemory "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretmemory "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

func TestPublicRouteUsesOpaqueRouteKeyAndRawAttempt(t *testing.T) {
	adapter := &feishu.Adapter{}
	hint, err := adapter.PublicRoute(context.Background(), channel.CallbackRequest{Body: []byte("body"), Query: map[string]string{"route_key": "opaque-route"}, Headers: map[string]string{"x-lark-request-timestamp": "1", "x-lark-request-nonce": "nonce"}})
	if err != nil || hint.Channel != "feishu" || hint.RouteKeyDigest != protocol.RouteKeyDigest("opaque-route") || hint.IngressAttemptID == "" {
		t.Fatalf("hint=%#v err=%v", hint, err)
	}
	if hint.RouteKeyDigest == "opaque-route" {
		t.Fatal("public route key was not digested")
	}
}

func TestEncryptedCallbackRunsThroughCommonDurablePipeline(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	material := protocol.VerificationMaterial{EncryptKey: "encrypt-key", VerificationToken: "verify-token", AppID: "cli_app", BotOpenID: "ou_bot"}
	verificationSecret, _ := json.Marshal(material)
	routeKey := "opaque-route"
	route := ingress.BindingRoute{OpaqueBindingID: "opaque-binding", Channel: "feishu", RouteKeyDigest: protocol.RouteKeyDigest(routeKey),
		TenantID: "tenant", AgentAppID: "app", ChannelBindingID: "binding", ExternalAccountID: "cli_app",
		TenantVersion: 1, BindingVersion: 1, SecretRef: secrets.SecretRef{Ref: "secret://verify", Version: 1},
		IdentitySecretRef: secrets.SecretRef{Ref: "secret://identity", Version: 1}, SessionSecretRef: secrets.SecretRef{Ref: "secret://session", Version: 1}, Enabled: true}
	routes := ingressmemory.New()
	if err := routes.PutBindingRoute(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	secretProvider := secretmemory.New()
	secretProvider.Put(secrets.Scope{TenantID: "tenant", Subject: "binding", Purpose: secrets.PurposeChannelVerify, ResourceID: "binding", ResourceVersion: 1}, route.SecretRef, verificationSecret)
	secretProvider.Put(secrets.Scope{TenantID: "tenant", Subject: "tenant", Purpose: secrets.PurposeTenantIdentity, ResourceID: "tenant", ResourceVersion: 1}, route.IdentitySecretRef, []byte("identity-key"))
	secretProvider.Put(secrets.Scope{TenantID: "tenant", Subject: "tenant", Purpose: secrets.PurposeTenantSession, ResourceID: "tenant", ResourceVersion: 1}, route.SessionSecretRef, []byte("session-key"))
	resolver := ingress.Resolver{Store: routes, Secrets: secretProvider, TTL: time.Minute, Now: func() time.Time { return now }}
	adapter := &feishu.Adapter{Protocol: protocol.Verifier{Now: func() time.Time { return now }}}
	payloads := messagingmemory.New()
	pipeline := ingress.Pipeline{Verification: ingress.Service{Adapter: adapter, Bindings: resolver}, Identity: identity.Mapper{Secrets: secretProvider},
		Intake: preprocessmemory.New(), Payloads: payloads, KeyVersion: 1}
	plaintext := feishuPayload(`{"text":"hello"}`)
	body := encrypt(t, plaintext, material.EncryptKey)
	timestamp, nonce := fmt.Sprint(now.Unix()), "nonce"
	accepted, err := pipeline.Accept(context.Background(), channel.CallbackRequest{Body: body, ReceivedAt: now, Query: map[string]string{"route_key": routeKey}, Headers: map[string]string{
		"X-Lark-Request-Timestamp": timestamp, "X-Lark-Request-Nonce": nonce,
		"X-Lark-Signature": larkevent.Signature(timestamp, nonce, material.EncryptKey, string(body)),
	}})
	if err != nil || len(accepted) != 1 {
		t.Fatalf("accepted=%#v err=%v", accepted, err)
	}
	stored, err := payloads.GetPayload(context.Background(), "tenant", accepted[0].RequestID)
	if err != nil || string(stored.Content) != `{"external_message_id":"om_message","external_user_id":"ou_user","external_chat_id":"oc_chat","text":"hello"}` {
		t.Fatalf("payload=%s err=%v", stored.Content, err)
	}
}

func TestDeliverUsesStableUUIDAndClassifiedSender(t *testing.T) {
	sender := &recordingSender{messageID: "om_reply"}
	adapter := &feishu.Adapter{Sender: sender}
	content := []byte("reply")
	sum := sha256.Sum256(content)
	request := channel.DeliveryRequest{Event: channel.ReplyEvent{TenantID: "tenant", ChannelBindingID: "binding"},
		ClientRequestID: "stable-uuid", Target: channel.DeliveryTarget{Channel: "feishu", ExternalAccountID: "cli_app", ExternalMessageID: "om_source"},
		Content: content, ContentDigest: hex.EncodeToString(sum[:])}
	result, err := adapter.Deliver(context.Background(), request)
	if err != nil || !result.Delivered || result.ProviderMessageID != "om_reply" || sender.uuid != "stable-uuid" || sender.destination.TenantID != "tenant" {
		t.Fatalf("result=%#v sender=%#v err=%v", result, sender, err)
	}
	sender.err = channel.RetryableDeliveryError{Err: errors.New("rate limited")}
	if _, err := adapter.Deliver(context.Background(), request); err == nil {
		t.Fatal("classified sender error was swallowed")
	}
}

func TestAdapterDeliveryContract(t *testing.T) {
	contracttest.RunDelivery(t, func(testing.TB) contracttest.DeliveryHarness {
		sender := &recordingSender{messageID: "om_reply"}
		content := []byte("contract reply")
		sum := sha256.Sum256(content)
		result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request",
			ContentDigest: hex.EncodeToString(sum[:]), Content: content, KeyVersion: 1}
		target := channel.DeliveryTarget{Channel: "feishu", ExternalAccountID: "cli_app", ExternalMessageID: "om_source"}
		event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding",
			DeliveryKey: "reply-feishu", ContentRef: result.ResultRef, Target: target, Final: true}
		return contracttest.DeliveryHarness{Adapter: &feishu.Adapter{Sender: sender}, Event: event, Result: result, Observe: func() contracttest.DeliveryObservation {
			return contracttest.DeliveryObservation{Calls: sender.calls, ClientRequestID: sender.uuid, Content: []byte(sender.text),
				Target: channel.DeliveryTarget{Channel: "feishu", ExternalAccountID: sender.destination.ExternalAccountID, ExternalMessageID: sender.replyMessageID}}
		}}
	})
}

func TestOfficialSenderUsesReplyUUIDAndClassifiesHTTPResults(t *testing.T) {
	for _, test := range []struct {
		name     string
		status   int
		response string
		checkErr func(error) bool
	}{
		{name: "success", status: 200, response: `{"code":0,"msg":"ok","data":{"message_id":"om_reply"}}`},
		{name: "rate limit", status: 429, response: `{"code":99991400,"msg":"rate limited"}`, checkErr: func(err error) bool {
			var target channel.RetryableDeliveryError
			return errors.As(err, &target) && target.RetryAfter == 2*time.Second
		}},
		{name: "provider rate code", status: 400, response: `{"code":99991400,"msg":"rate limited"}`, checkErr: func(err error) bool {
			var target channel.RetryableDeliveryError
			return errors.As(err, &target) && target.RetryAfter == 2*time.Second
		}},
		{name: "expired token after sdk retry", status: 400, response: `{"code":99991663,"msg":"tenant token invalid"}`, checkErr: func(err error) bool {
			var target channel.RetryableDeliveryError
			return errors.As(err, &target)
		}},
		{name: "server", status: 503, response: `{"code":1,"msg":"unavailable"}`, checkErr: func(err error) bool { var target channel.RetryableDeliveryError; return errors.As(err, &target) }},
		{name: "invalid message", status: 400, response: `{"code":230001,"msg":"message not found"}`, checkErr: func(err error) bool { var target channel.PermanentDeliveryError; return errors.As(err, &target) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var replyBody string
			httpClient := httpClientFunc(func(r *http.Request) (*http.Response, error) {
				status, response := 200, `{"code":0,"msg":"ok","tenant_access_token":"token","expire":7200}`
				headers := make(http.Header)
				headers.Set("Content-Type", "application/json")
				if r.URL.Path == "/open-apis/im/v1/messages/om_source/reply" {
					body, _ := io.ReadAll(r.Body)
					replyBody = string(body)
					status, response = test.status, test.response
					headers.Set("Retry-After", "2")
				}
				return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(response)), Request: r}, nil
			})
			clients := &feishu.ClientCache{Credentials: staticCredentials{}, NewClient: func(appID, secret string) *lark.Client {
				return lark.NewClient(appID, secret, lark.WithOpenBaseUrl("https://unit.test"), lark.WithOAuthBaseUrl("https://unit.test"), lark.WithHttpClient(httpClient))
			}}
			sender := feishu.OfficialSender{Clients: clients}
			messageID, err := sender.ReplyText(context.Background(), channel.ReplyDestination{TenantID: "tenant", ChannelBindingID: "binding", ExternalAccountID: "cli_app"}, "om_source", "hello", "stable-uuid")
			if test.checkErr != nil {
				if !test.checkErr(err) {
					t.Fatalf("message=%q err=%T %v", messageID, err, err)
				}
				return
			}
			var sent struct {
				Content string `json:"content"`
				UUID    string `json:"uuid"`
			}
			_ = json.Unmarshal([]byte(replyBody), &sent)
			if err != nil || messageID != "om_reply" || sent.UUID != "stable-uuid" || !strings.Contains(sent.Content, `"text":"hello"`) {
				t.Fatalf("message=%q body=%s err=%v", messageID, replyBody, err)
			}
		})
	}
}

func TestClientCacheReusesSDKClientAndTenantToken(t *testing.T) {
	tokenCalls, replyCalls := 0, 0
	var paths []string
	httpClient := httpClientFunc(func(request *http.Request) (*http.Response, error) {
		paths = append(paths, request.URL.Path)
		response := `{"code":0,"msg":"ok","tenant_access_token":"token","expire":7200}`
		if strings.Contains(request.URL.Path, "tenant_access_token") {
			tokenCalls++
		} else {
			replyCalls++
			response = fmt.Sprintf(`{"code":0,"msg":"ok","data":{"message_id":"om_reply_%d"}}`, replyCalls)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(response)), Request: request}, nil
	})
	newClientCalls := 0
	clients := &feishu.ClientCache{Credentials: staticCredentials{appID: "cli_cache_test"}, NewClient: func(appID, secret string) *lark.Client {
		newClientCalls++
		return lark.NewClient(appID, secret, lark.WithOpenBaseUrl("https://unit.test"), lark.WithOAuthBaseUrl("https://unit.test"), lark.WithHttpClient(httpClient))
	}}
	sender := feishu.OfficialSender{Clients: clients}
	destination := channel.ReplyDestination{TenantID: "tenant", ChannelBindingID: "binding", ExternalAccountID: "cli_cache_test"}
	for index := 1; index <= 2; index++ {
		messageID, err := sender.ReplyText(context.Background(), destination, "om_source", "hello", fmt.Sprintf("stable-uuid-%d", index))
		if err != nil || messageID != fmt.Sprintf("om_reply_%d", index) {
			t.Fatalf("send %d message=%q err=%v", index, messageID, err)
		}
	}
	if newClientCalls != 1 || tokenCalls != 1 || replyCalls != 2 {
		t.Fatalf("new_clients=%d token_calls=%d reply_calls=%d paths=%v", newClientCalls, tokenCalls, replyCalls, paths)
	}
}

type recordingSender struct {
	destination     channel.ReplyDestination
	replyMessageID  string
	text            string
	uuid, messageID string
	calls           int
	err             error
}

type staticCredentials struct{ appID string }

func (s staticCredentials) ResolveFeishuSendCredentials(context.Context, channel.ReplyDestination) (feishu.ClientCredentials, error) {
	appID := s.appID
	if appID == "" {
		appID = "cli_app"
	}
	return feishu.ClientCredentials{AppID: appID, AppSecret: "app-secret", Version: 1}, nil
}

type httpClientFunc func(*http.Request) (*http.Response, error)

func (f httpClientFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

func (s *recordingSender) ReplyText(_ context.Context, destination channel.ReplyDestination, replyMessageID, text, uuid string) (string, error) {
	s.destination, s.replyMessageID, s.text, s.uuid = destination, replyMessageID, text, uuid
	s.calls++
	return s.messageID, s.err
}

func feishuPayload(content string) []byte {
	payload := map[string]any{"schema": "2.0", "header": map[string]any{"event_id": "evt_1", "event_type": protocol.EventTypeMessageReceive,
		"app_id": "cli_app", "tenant_key": "tenant-key", "token": "verify-token", "create_time": "1800000000000"},
		"event": map[string]any{"sender": map[string]any{"sender_id": map[string]any{"open_id": "ou_user"}, "sender_type": "user", "tenant_key": "tenant-key"},
			"message": map[string]any{"message_id": "om_message", "chat_id": "oc_chat", "chat_type": "p2p", "message_type": "text", "content": content, "create_time": "1800000000000"}}}
	encoded, _ := json.Marshal(payload)
	return encoded
}

func encrypt(t *testing.T, plaintext []byte, secret string) []byte {
	t.Helper()
	key := sha256.Sum256([]byte(secret))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		t.Fatal(err)
	}
	padding := aes.BlockSize - len(plaintext)%aes.BlockSize
	padded := append(append([]byte(nil), plaintext...), make([]byte, padding)...)
	iv := []byte("0123456789abcdef")
	ciphertext := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, padded)
	value := base64.StdEncoding.EncodeToString(append(append([]byte(nil), iv...), ciphertext...))
	body, _ := json.Marshal(map[string]string{"encrypt": value})
	return body
}
