package webui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	ingressmemory "github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretmemory "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

func TestBrowserHandlerServesUIAndAuthenticatedDurableReplies(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	routes := ingressmemory.New()
	secretProvider := secretmemory.New()
	mailbox := NewMemoryMailbox()
	results := messagingmemory.New()
	secretRef := secrets.SecretRef{Ref: "webui-verify", Version: 1}
	route := ingress.BindingRoute{OpaqueBindingID: "opaque-webui-binding", Channel: "webui", RouteKeyDigest: RouteKeyDigest("local-route"),
		TenantID: "tenant-a", AgentAppID: "app-a", ChannelBindingID: "webui-main", ExternalAccountID: "local-account",
		TenantVersion: 1, BindingVersion: 1, SecretRef: secretRef,
		IdentitySecretRef: secrets.SecretRef{Ref: "identity", Version: 1}, SessionSecretRef: secrets.SecretRef{Ref: "session", Version: 1}, Enabled: true}
	if err := routes.PutBindingRoute(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	token := "0123456789abcdef0123456789abcdef"
	material, _ := json.Marshal(VerificationMaterial{Token: token, ExternalAccountID: route.ExternalAccountID})
	secretProvider.Put(secrets.Scope{TenantID: route.TenantID, Subject: route.ChannelBindingID, Purpose: secrets.PurposeChannelVerify,
		ResourceID: route.ChannelBindingID, ResourceVersion: route.BindingVersion}, secretRef, material)
	content := []byte("durable browser reply")
	digest := sha256.Sum256(content)
	if err := results.PutResult(context.Background(), messaging.ResultRecord{TenantID: route.TenantID, RequestID: "request-1",
		ResultRef: "result://request-1", ContentDigest: hex.EncodeToString(digest[:]), Content: content, KeyVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := mailbox.PutMessage(context.Background(), Message{TenantID: route.TenantID, ConfigVersion: 1,
		ChannelBindingID: route.ChannelBindingID, ExternalAccountID: route.ExternalAccountID,
		ExternalUserID: "user-1", ExternalChatID: "chat-1", RequestID: "request-1", ClientRequestID: "client-1",
		ProviderMessageID: "webui-message-1", ContentRef: "result://request-1", ContentDigest: hex.EncodeToString(digest[:]), CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	callbackCalls := 0
	handler := BrowserHandler{Callback: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		callbackCalls++
		writer.WriteHeader(http.StatusOK)
	}), Routes: routes, Secrets: secretProvider, Messages: mailbox, Results: results, Now: func() time.Time { return now }}

	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/webui/", nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "WebUI IM") || page.Header().Get("Content-Security-Policy") == "" {
		t.Fatalf("page code=%d headers=%v body=%q", page.Code, page.Header(), page.Body.String())
	}

	uri := "/webui/api/replies?route_key=local-route&external_user_id=user-1&external_chat_id=chat-1"
	timestamp, nonce := "1800000000", "nonce-read"
	request := httptest.NewRequest(http.MethodGet, uri, nil)
	request.Header.Set(headerTimestamp, timestamp)
	request.Header.Set(headerNonce, nonce)
	request.Header.Set(headerSignature, signatureFor(token, timestamp, nonce, []byte(http.MethodGet+"\n"+uri)))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "durable browser reply") || !strings.Contains(response.Body.String(), "request-1") {
		t.Fatalf("reply code=%d body=%q", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, uri, nil)
	request.Header.Set(headerTimestamp, timestamp)
	request.Header.Set(headerNonce, nonce)
	request.Header.Set(headerSignature, signatureFor("wrong-token-000000000", timestamp, nonce, []byte(http.MethodGet+"\n"+uri)))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized code=%d", response.Code)
	}

	post := httptest.NewRequest(http.MethodPost, "/webui/api/messages?route_key=local-route", strings.NewReader(`{}`))
	handler.ServeHTTP(httptest.NewRecorder(), post)
	if callbackCalls != 1 {
		t.Fatalf("callback calls=%d", callbackCalls)
	}
}
