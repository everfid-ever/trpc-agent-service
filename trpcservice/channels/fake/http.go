// Package fake provides an HTTP channel for the P0 two-tenant vertical slice.
package fake

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Binding struct {
	Locator           string
	Tenant            tenant.Context
	IdentityKey       []byte
	ExternalAccountID string
}

type Handler struct {
	Dispatcher gateway.Dispatcher
	Inbox      messaging.InboxClaimer
	mu         sync.RWMutex
	bindings   map[string]Binding
	payloads   map[string][]byte
}

func NewHandler(dispatcher gateway.Dispatcher, bindings ...Binding) *Handler {
	h := &Handler{Dispatcher: dispatcher, bindings: make(map[string]Binding), payloads: make(map[string][]byte)}
	for _, binding := range bindings {
		h.bindings[binding.Locator] = binding
	}
	return h
}

type inbound struct {
	ExternalMessageID string `json:"external_message_id"`
	ExternalUserID    string `json:"external_user_id"`
	ExternalChatID    string `json:"external_chat_id"`
	Text              string `json:"text"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	locator := strings.TrimPrefix(r.URL.Path, "/bindings/")
	h.mu.RLock()
	binding, ok := h.bindings[locator]
	h.mu.RUnlock()
	if !ok {
		http.Error(w, "binding not found", http.StatusNotFound)
		return
	}
	if err := binding.Tenant.Validate(); err != nil {
		http.Error(w, "invalid binding", http.StatusServiceUnavailable)
		return
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	var in inbound
	if err := dec.Decode(&in); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if in.ExternalMessageID == "" || in.ExternalUserID == "" || in.ExternalChatID == "" || in.Text == "" {
		http.Error(w, "missing field", http.StatusBadRequest)
		return
	}
	requestID := stableID("req", binding.Tenant.TenantID, in.ExternalMessageID)
	payload, _ := json.Marshal(in)
	h.mu.Lock()
	if existing, exists := h.payloads[requestID]; exists && !bytes.Equal(existing, payload) {
		h.mu.Unlock()
		http.Error(w, "idempotency collision", http.StatusConflict)
		return
	}
	h.payloads[requestID] = append([]byte(nil), payload...)
	h.mu.Unlock()
	userID := hmacID("u1", binding.IdentityKey, binding.Tenant.TenantID, in.ExternalUserID)
	sessionID := hmacID("s1", binding.IdentityKey, binding.Tenant.TenantID, in.ExternalChatID)
	payloadRef := "fake-payload://" + requestID
	if h.Inbox != nil {
		externalAccountID := binding.ExternalAccountID
		if externalAccountID == "" {
			externalAccountID = binding.Locator
		}
		digest := sha256.Sum256(payload)
		claimed, err := h.Inbox.ClaimInbox(r.Context(), messaging.ClaimInboxRequest{
			InboxKey: messaging.InboxKey{
				TenantID: binding.Tenant.TenantID, Channel: binding.Tenant.Channel,
				ExternalAccountID: externalAccountID, ExternalMessageID: in.ExternalMessageID,
			},
			RequestID: requestID, AgentAppID: binding.Tenant.AgentAppID, SessionID: sessionID,
			PayloadRef: payloadRef, PayloadDigest: hex.EncodeToString(digest[:]), KeyVersion: 1,
			InitialState: messaging.InboxDispatchPending,
		})
		if err != nil {
			if errors.Is(err, runtime.ErrIdempotencyCollision) {
				http.Error(w, "idempotency collision", http.StatusConflict)
				return
			}
			http.Error(w, "inbox claim failed", http.StatusInternalServerError)
			return
		}
		requestID = claimed.RequestID
	}
	handle, err := h.Dispatcher.Dispatch(r.Context(), gateway.DispatchRequest{Tenant: binding.Tenant, RequestID: requestID, SessionID: sessionID, UserID: userID, PayloadRef: payloadRef})
	if err != nil {
		http.Error(w, "dispatch failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(handle)
}

// Payload returns a defensive copy of the locally durable normalized payload.
func (h *Handler) Payload(requestID string) ([]byte, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	payload, ok := h.payloads[requestID]
	return append([]byte(nil), payload...), ok
}

func stableID(prefix string, parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(part)))
		_, _ = h.Write(b[:])
		_, _ = h.Write([]byte(part))
	}
	return prefix + "_" + hex.EncodeToString(h.Sum(nil)[:16])
}
func hmacID(prefix string, key []byte, parts ...string) string {
	m := hmac.New(sha256.New, key)
	for _, part := range parts {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(part)))
		_, _ = m.Write(b[:])
		_, _ = m.Write([]byte(part))
	}
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(m.Sum(nil)[:16]))
}
