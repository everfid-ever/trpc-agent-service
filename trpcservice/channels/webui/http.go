package webui

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type BrowserHandler struct {
	Callback http.Handler
	Routes   ingress.Store
	Secrets  secrets.Provider
	Messages MessageReader
	Results  messaging.ResultStore
	Now      func() time.Time
	MaxSkew  time.Duration
}

type browserReply struct {
	RequestID         string    `json:"request_id"`
	ProviderMessageID string    `json:"provider_message_id"`
	Text              string    `json:"text"`
	CreatedAt         time.Time `json:"created_at"`
}

func (h BrowserHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if h.Callback == nil || h.Routes == nil || h.Secrets == nil || h.Messages == nil || h.Results == nil {
		http.Error(writer, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	switch request.URL.Path {
	case "/webui":
		http.Redirect(writer, request, "/webui/", http.StatusTemporaryRedirect)
	case "/webui/":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer, "GET")
			return
		}
		writeAsset(writer, "text/html; charset=utf-8", webUIHTML)
	case "/webui/app.js":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer, "GET")
			return
		}
		writeAsset(writer, "text/javascript; charset=utf-8", webUIJS)
	case "/webui/style.css":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer, "GET")
			return
		}
		writeAsset(writer, "text/css; charset=utf-8", webUICSS)
	case "/webui/api/messages":
		if request.Method != http.MethodPost {
			methodNotAllowed(writer, "POST")
			return
		}
		h.Callback.ServeHTTP(writer, request)
	case "/webui/api/replies":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer, "GET")
			return
		}
		h.replies(writer, request)
	default:
		http.NotFound(writer, request)
	}
}

func (h BrowserHandler) replies(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	routeKey := strings.TrimSpace(query.Get("route_key"))
	userID := strings.TrimSpace(query.Get("external_user_id"))
	chatID := strings.TrimSpace(query.Get("external_chat_id"))
	if routeKey == "" || userID == "" || chatID == "" {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	route, err := h.authorize(request.Context(), request, routeKey)
	if err != nil {
		writeBrowserError(writer, err)
		return
	}
	messages, err := h.Messages.ListMessages(request.Context(), MessageQuery{TenantID: route.TenantID,
		ChannelBindingID: route.ChannelBindingID, ExternalAccountID: route.ExternalAccountID,
		ExternalUserID: userID, ExternalChatID: chatID, Limit: 100})
	if err != nil {
		writeBrowserError(writer, err)
		return
	}
	replies := make([]browserReply, 0, len(messages))
	for _, message := range messages {
		result, resultErr := h.Results.GetResult(request.Context(), route.TenantID, message.RequestID)
		if resultErr != nil {
			writeBrowserError(writer, resultErr)
			return
		}
		if result.ResultRef != message.ContentRef || result.ContentDigest != message.ContentDigest {
			writeBrowserError(writer, runtime.ErrVersionMismatch)
			return
		}
		replies = append(replies, browserReply{RequestID: message.RequestID, ProviderMessageID: message.ProviderMessageID,
			Text: string(result.Content), CreatedAt: message.CreatedAt})
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	_ = json.NewEncoder(writer).Encode(map[string]any{"messages": replies})
}

func (h BrowserHandler) authorize(ctx context.Context, request *http.Request, routeKey string) (ingress.BindingRoute, error) {
	route, err := h.Routes.ResolveBindingRoute(ctx, "webui", RouteKeyDigest(routeKey))
	if err != nil {
		return ingress.BindingRoute{}, err
	}
	if !route.Enabled || route.Channel != "webui" || route.ExternalAccountID == "" {
		return ingress.BindingRoute{}, runtime.ErrVersionMismatch
	}
	value, err := h.Secrets.Resolve(ctx, secrets.Scope{TenantID: route.TenantID, Subject: route.ChannelBindingID,
		Purpose: secrets.PurposeChannelVerify, ResourceID: route.ChannelBindingID, ResourceVersion: route.BindingVersion}, route.SecretRef)
	if err != nil {
		return ingress.BindingRoute{}, err
	}
	defer clear(value.Bytes)
	if value.Version != route.SecretRef.Version {
		return ingress.BindingRoute{}, runtime.ErrVersionMismatch
	}
	material, err := parseMaterial(value.Bytes)
	if err != nil || material.ExternalAccountID != route.ExternalAccountID {
		return ingress.BindingRoute{}, runtime.ErrVersionMismatch
	}
	now := time.Now().UTC()
	if h.Now != nil {
		now = h.Now().UTC()
	}
	maxSkew := h.MaxSkew
	if maxSkew <= 0 {
		maxSkew = 5 * time.Minute
	}
	payload := []byte(request.Method + "\n" + request.URL.RequestURI())
	if err := verifySignature(material.Token, request.Header.Get(headerTimestamp), request.Header.Get(headerNonce),
		request.Header.Get(headerSignature), payload, now, maxSkew); err != nil {
		return ingress.BindingRoute{}, err
	}
	return route, nil
}

func writeAsset(writer http.ResponseWriter, contentType, content string) {
	writer.Header().Set("Content-Type", contentType)
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	_, _ = writer.Write([]byte(content))
}

func methodNotAllowed(writer http.ResponseWriter, allowed string) {
	writer.Header().Set("Allow", allowed)
	http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
}

func writeBrowserError(writer http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, runtime.ErrInvalidEnvelope), errors.Is(err, runtime.ErrVersionMismatch),
		errors.Is(err, runtime.ErrTenantScope), errors.Is(err, runtime.ErrNotFound):
		status = http.StatusUnauthorized
	case errors.Is(err, runtime.ErrBackendUnavailable):
		status = http.StatusServiceUnavailable
	}
	http.Error(writer, http.StatusText(status), status)
}
