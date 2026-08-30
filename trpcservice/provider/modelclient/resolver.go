// Package modelclient builds production model clients from immutable provider
// profiles and tenant-scoped credentials.
package modelclient

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
)

const (
	defaultTimeout = 60 * time.Second
	maximumTimeout = 10 * time.Minute
)

type ProfileReader interface {
	GetModel(context.Context, string, string, int64) (provider.ModelProfileSnapshot, error)
}

// Resolver implements agent.ModelResolver. It intentionally has no
// environment-variable fallback and does not resolve current/latest profiles.
type Resolver struct {
	Profiles ProfileReader
	Secrets  secrets.Provider
	Subject  string
}

func (r Resolver) ResolveModel(ctx context.Context, tenantID string, ref profile.VersionedRef) (model.Model, error) {
	if r.Profiles == nil || r.Secrets == nil || strings.TrimSpace(r.Subject) != r.Subject || r.Subject == "" {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if tenantID == "" || ref.ID == "" || ref.Version < 1 {
		return nil, runtime.ErrInvalidEnvelope
	}
	value, err := r.Profiles.GetModel(ctx, tenantID, ref.ID, ref.Version)
	if err != nil {
		return nil, err
	}
	if value.TenantID != tenantID || value.ProfileID != ref.ID {
		return nil, runtime.ErrTenantScope
	}
	if value.Version != ref.Version || value.ContentDigest == "" {
		return nil, runtime.ErrVersionMismatch
	}
	if value.Status != "active" && value.Status != "suspended" {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if value.Provider != "deepseek" || value.SchemaVersion != 1 || value.Model == "" || value.SecretRef.Ref == "" || value.SecretRef.Version < 1 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if err := validateEndpoint(value.Endpoint); err != nil {
		return nil, err
	}
	timeout, bufferSize, err := options(value.Options)
	if err != nil {
		return nil, err
	}
	scope := secrets.Scope{TenantID: tenantID, Subject: r.Subject, Purpose: secrets.PurposeModelCall,
		ResourceID: value.ProfileID, ResourceVersion: value.Version}
	credential, err := r.Secrets.Resolve(ctx, scope, value.SecretRef)
	if err != nil {
		clear(credential.Bytes)
		return nil, err
	}
	defer clear(credential.Bytes)
	apiKey := strings.TrimSpace(string(credential.Bytes))
	if credential.Version != value.SecretRef.Version || apiKey == "" || strings.ContainsAny(apiKey, "\r\n\x00") {
		return nil, runtime.ErrVersionMismatch
	}
	opts := []openai.Option{
		openai.WithAPIKey(apiKey),
		openai.WithBaseURL(value.Endpoint),
		openai.WithHTTPClientOptions(model.WithHTTPClientTimeout(timeout)),
	}
	if bufferSize > 0 {
		opts = append(opts, openai.WithChannelBufferSize(bufferSize))
	}
	return openai.New(value.Model, opts...), nil
}

func validateEndpoint(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return runtime.ErrCapabilityUnsupported
	}
	if !strings.EqualFold(parsed.Hostname(), "api.deepseek.com") || parsed.Port() != "" {
		return runtime.ErrCapabilityUnsupported
	}
	path := strings.TrimSuffix(parsed.EscapedPath(), "/")
	if path != "" && path != "/v1" {
		return runtime.ErrCapabilityUnsupported
	}
	return nil
}

func options(input map[string]string) (time.Duration, int, error) {
	timeout := defaultTimeout
	bufferSize := 0
	for name, value := range input {
		switch name {
		case "timeout_ms":
			milliseconds, err := strconv.ParseInt(value, 10, 64)
			if err != nil || milliseconds < 100 || milliseconds > maximumTimeout.Milliseconds() {
				return 0, 0, runtime.ErrCapabilityUnsupported
			}
			timeout = time.Duration(milliseconds) * time.Millisecond
		case "channel_buffer_size":
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 1 || parsed > 4096 {
				return 0, 0, runtime.ErrCapabilityUnsupported
			}
			bufferSize = parsed
		default:
			return 0, 0, runtime.ErrCapabilityUnsupported
		}
	}
	return timeout, bufferSize, nil
}
