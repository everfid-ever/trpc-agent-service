package admin

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type PrincipalResolver interface {
	Resolve(*http.Request) (Principal, error)
}

type Handler struct {
	Service    Service
	Principals PrincipalResolver
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	principal, err := h.Principals.Resolve(r)
	if err != nil {
		writeError(w, ErrUnauthenticated)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 || parts[0] != "v1" || parts[1] != "tenants" || parts[3] != "configs" {
		http.NotFound(w, r)
		return
	}
	tenantID := parts[2]
	switch {
	case r.Method == http.MethodPost && len(parts) == 5 && parts[4] == "validate":
		payload, err := decodePayload(w, r)
		if err == nil {
			err = h.Service.Validate(r.Context(), principal, tenantID, payload)
		}
		if err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && len(parts) == 5 && parts[4] == "publish":
		expected, ok := queryInt64(r, "expected_version")
		if !ok {
			http.Error(w, "invalid expected_version", http.StatusBadRequest)
			return
		}
		payload, err := decodePayload(w, r)
		if err == nil {
			var result config.PublishResult
			result, err = h.Service.Publish(r.Context(), principal, tenantID, expected, payload, metadata(r, principal))
			if err == nil {
				writeJSON(w, http.StatusCreated, result)
				return
			}
		}
		writeError(w, err)
	case r.Method == http.MethodPost && len(parts) == 5 && parts[4] == "rollback":
		expected, ok1 := queryInt64(r, "expected_version")
		target, ok2 := queryInt64(r, "target_version")
		if !ok1 || !ok2 {
			http.Error(w, "invalid version", http.StatusBadRequest)
			return
		}
		result, err := h.Service.Rollback(r.Context(), principal, tenantID, expected, target, metadata(r, principal))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, result)
	case r.Method == http.MethodGet && len(parts) == 4:
		result, err := h.Service.Current(r.Context(), principal, tenantID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	case r.Method == http.MethodGet && len(parts) == 5:
		version, err := strconv.ParseInt(parts[4], 10, 64)
		if err != nil || version < 1 {
			http.Error(w, "invalid version", http.StatusBadRequest)
			return
		}
		result, err := h.Service.Get(r.Context(), principal, tenantID, version)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	default:
		http.NotFound(w, r)
	}
}

func decodePayload(w http.ResponseWriter, r *http.Request) (config.ConfigV1, error) {
	body := http.MaxBytesReader(w, r.Body, 1<<20)
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		return config.ConfigV1{}, config.ErrInvalid
	}
	payload, err := config.DecodeV1(data)
	if err != nil {
		return config.ConfigV1{}, config.ErrInvalid
	}
	return payload, nil
}
func queryInt64(r *http.Request, key string) (int64, bool) {
	value, err := strconv.ParseInt(r.URL.Query().Get(key), 10, 64)
	return value, err == nil && value > 0
}
func metadata(r *http.Request, p Principal) tenant.ChangeMetadata {
	return tenant.ChangeMetadata{ActorType: "admin", ActorID: p.SubjectID, ReasonCode: r.Header.Get("X-Reason-Code"), ReasonRef: r.Header.Get("X-Reason-Ref"), CorrelationID: r.Header.Get("X-Correlation-ID"), TraceID: r.Header.Get("X-Trace-ID")}
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, ErrUnauthenticated):
		status = http.StatusUnauthorized
	case errors.Is(err, ErrForbidden), errors.Is(err, config.ErrTenantScope):
		status = http.StatusForbidden
	case errors.Is(err, config.ErrNotFound), errors.Is(err, tenant.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, config.ErrVersionConflict), errors.Is(err, tenant.ErrVersionConflict):
		status = http.StatusConflict
	}
	http.Error(w, http.StatusText(status), status)
}
