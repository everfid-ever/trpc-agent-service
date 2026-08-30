package modelclient

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type profileReaderStub struct{ value provider.ModelProfileSnapshot }

func (s profileReaderStub) GetModel(context.Context, string, string, int64) (provider.ModelProfileSnapshot, error) {
	return s.value, nil
}

type secretProviderStub struct {
	scope secrets.Scope
	ref   secrets.SecretRef
	value secrets.SecretValue
	err   error
}

func (s *secretProviderStub) Resolve(_ context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
	s.scope, s.ref = scope, ref
	return s.value, s.err
}

func TestResolverBuildsExactScopedOpenAIClient(t *testing.T) {
	secret := &secretProviderStub{value: secrets.SecretValue{Bytes: []byte("test-key"), Version: 9}}
	resolver := Resolver{Profiles: profileReaderStub{validProfile()}, Secrets: secret, Subject: "worker-model"}
	resolved, err := resolver.ResolveModel(context.Background(), "tenant-a", profile.VersionedRef{ID: "model", Version: 3})
	if err != nil {
		t.Fatal(err)
	}
	if resolved == nil || secret.scope != (secrets.Scope{TenantID: "tenant-a", Subject: "worker-model", Purpose: secrets.PurposeModelCall,
		ResourceID: "model", ResourceVersion: 3}) || secret.ref != (secrets.SecretRef{Ref: "secret/model", Version: 9}) {
		t.Fatalf("scope=%#v ref=%#v model=%T", secret.scope, secret.ref, resolved)
	}
}

func TestResolverFailsClosedWithoutLeakingIntoProviderFallbacks(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*provider.ModelProfileSnapshot, *secretProviderStub)
		error  error
	}{
		{name: "provider", mutate: func(p *provider.ModelProfileSnapshot, _ *secretProviderStub) { p.Provider = "openai" }, error: runtime.ErrCapabilityUnsupported},
		{name: "endpoint", mutate: func(p *provider.ModelProfileSnapshot, _ *secretProviderStub) {
			p.Endpoint = "http://api.deepseek.com"
		}, error: runtime.ErrCapabilityUnsupported},
		{name: "private endpoint", mutate: func(p *provider.ModelProfileSnapshot, _ *secretProviderStub) { p.Endpoint = "https://127.0.0.1/v1" }, error: runtime.ErrCapabilityUnsupported},
		{name: "endpoint path", mutate: func(p *provider.ModelProfileSnapshot, _ *secretProviderStub) {
			p.Endpoint = "https://api.deepseek.com/beta"
		}, error: runtime.ErrCapabilityUnsupported},
		{name: "unknown option", mutate: func(p *provider.ModelProfileSnapshot, _ *secretProviderStub) { p.Options["retry"] = "3" }, error: runtime.ErrCapabilityUnsupported},
		{name: "empty credential", mutate: func(_ *provider.ModelProfileSnapshot, s *secretProviderStub) { s.value.Bytes = []byte(" \n") }, error: runtime.ErrVersionMismatch},
		{name: "credential version", mutate: func(_ *provider.ModelProfileSnapshot, s *secretProviderStub) { s.value.Version = 8 }, error: runtime.ErrVersionMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := validProfile()
			secret := &secretProviderStub{value: secrets.SecretValue{Bytes: []byte("key"), Version: 9}}
			test.mutate(&value, secret)
			_, err := (Resolver{Profiles: profileReaderStub{value}, Secrets: secret, Subject: "worker-model"}).
				ResolveModel(context.Background(), "tenant-a", profile.VersionedRef{ID: "model", Version: 3})
			if !errors.Is(err, test.error) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestResolverNeverFallsBackToDeepSeekEnvironmentCredential(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "must-not-be-used")
	secret := &secretProviderStub{value: secrets.SecretValue{Bytes: []byte(" \n"), Version: 9}}
	_, err := (Resolver{Profiles: profileReaderStub{validProfile()}, Secrets: secret, Subject: "worker-model"}).
		ResolveModel(context.Background(), "tenant-a", profile.VersionedRef{ID: "model", Version: 3})
	if !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("got %v", err)
	}
}

func TestResolverClearsCredentialBytesOnProviderError(t *testing.T) {
	bytes := []byte("secret-bytes")
	secret := &secretProviderStub{value: secrets.SecretValue{Bytes: bytes, Version: 9}, err: runtime.ErrBackendUnavailable}
	_, err := (Resolver{Profiles: profileReaderStub{validProfile()}, Secrets: secret, Subject: "worker-model"}).
		ResolveModel(context.Background(), "tenant-a", profile.VersionedRef{ID: "model", Version: 3})
	if !errors.Is(err, runtime.ErrBackendUnavailable) {
		t.Fatalf("got %v", err)
	}
	for index, value := range bytes {
		if value != 0 {
			t.Fatalf("credential byte %d was not cleared", index)
		}
	}
}

func validProfile() provider.ModelProfileSnapshot {
	return provider.ModelProfileSnapshot{TenantID: "tenant-a", ProfileID: "model", Status: "active", SchemaVersion: 1,
		Provider: "deepseek", Model: "deepseek-v4-flash", Endpoint: "https://api.deepseek.com",
		Options:   map[string]string{"timeout_ms": "1000", "channel_buffer_size": "32"},
		SecretRef: secrets.SecretRef{Ref: "secret/model", Version: 9}, ContentDigest: "digest", Version: 3}
}
