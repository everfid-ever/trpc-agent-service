package provider

import (
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

func TestCatalogNormalizesModelAndRejectsSecrets(t *testing.T) {
	catalog, err := NewCatalog(Schema{
		Kind: KindModel, Name: "openai", SchemaVersion: 1, AllowedModels: []string{"gpt-test"},
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"api.example.com"}, SecretRequirement: "required",
		OptionRules: map[string]OptionRule{"timeout_ms": {Type: OptionInteger, Default: "1000", Min: 100, Max: 5000}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := catalog.NormalizeModel(ModelProfileSnapshot{
		TenantID: "tenant-a", ProfileID: "model", ProfileKey: "default", Status: "active", Version: 2,
		SchemaVersion: 1, Provider: "openai", Model: "gpt-test", Endpoint: "https://api.example.com/v1",
		SecretRef: secrets.SecretRef{Ref: "kms/model", Version: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if profile.Options["timeout_ms"] != "1000" || len(profile.ContentDigest) != 64 {
		t.Fatalf("not normalized: %#v", profile)
	}
	_, err = catalog.NormalizeModel(ModelProfileSnapshot{
		TenantID: "tenant-a", ProfileID: "model", ProfileKey: "default", Status: "active", Version: 2,
		SchemaVersion: 1, Provider: "openai", Model: "gpt-test", Endpoint: "https://api.example.com/v1",
		Options: map[string]string{"api_key": "secret"}, SecretRef: secrets.SecretRef{Ref: "kms/model", Version: 3},
	})
	if !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("sensitive option: got %v", err)
	}
}

func TestCatalogRejectsEndpointBypassAndCapabilityLie(t *testing.T) {
	catalog, err := NewCatalog(
		Schema{Kind: KindModel, Name: "model", SchemaVersion: 1, AllowedModels: []string{"m"}, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"api.example.com"}, SecretRequirement: "forbidden"},
		Schema{Kind: KindBackend, Name: "postgres", SchemaVersion: 1, SecretRequirement: "required", Capabilities: CapabilitySet{"atomic_turn_commit": true}},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.NormalizeModel(ModelProfileSnapshot{TenantID: "t", ProfileID: "p", ProfileKey: "p", Status: "active", Version: 1, SchemaVersion: 1, Provider: "model", Model: "m", Endpoint: "https://api.example.com@127.0.0.1/v1"})
	if !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("endpoint bypass: got %v", err)
	}
	_, err = catalog.NormalizeBackend(BackendProfileSnapshot{TenantID: "t", ProfileID: "p", ProfileKey: "p", Status: "active", Version: 1, SchemaVersion: 1, Provider: "postgres", CredentialRef: secrets.SecretRef{Ref: "kms/db", Version: 1}, Capabilities: CapabilitySet{"summary_cas": true}})
	if !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("capability lie: got %v", err)
	}
}

func TestDeepSeekModelSchemaPinsOfficialProductionSurface(t *testing.T) {
	catalog, err := NewCatalog(DeepSeekModelSchema())
	if err != nil {
		t.Fatal(err)
	}
	value, err := catalog.NormalizeModel(ModelProfileSnapshot{TenantID: "tenant-a", ProfileID: "model", ProfileKey: "deepseek",
		Status: "active", Version: 1, SchemaVersion: 1, Provider: "deepseek", Model: "deepseek-v4-flash-vision-exp",
		Endpoint: "https://api.deepseek.com", SecretRef: secrets.SecretRef{Ref: "secret/deepseek", Version: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if value.Options["timeout_ms"] != "60000" || value.Options["channel_buffer_size"] != "256" {
		t.Fatalf("defaults=%#v", value.Options)
	}
	value.Model = "deepseek-v4-flash"
	if _, err := catalog.NormalizeModel(value); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("text-only model: %v", err)
	}
}

func TestQdrantVectorSchemaKeepsCredentialsOutOfProfiles(t *testing.T) {
	catalog, err := NewCatalog(QdrantVectorSchema())
	if err != nil {
		t.Fatal(err)
	}
	value, err := catalog.NormalizeBackend(BackendProfileSnapshot{TenantID: "tenant-a", ProfileID: "vector", ProfileKey: "qdrant",
		Status: "active", Version: 1, SchemaVersion: 1, Provider: "qdrant", Configuration: map[string]string{
			"endpoint": "https://qdrant.example.com", "collection": "knowledge", "vector_size": "1536", "snapshot_watermark": "snapshot-a",
		}, CredentialRef: secrets.SecretRef{Ref: "secret/qdrant", Version: 1}, Capabilities: CapabilitySet{"tenant_filter": true}})
	if err != nil {
		t.Fatal(err)
	}
	if value.Configuration["timeout_ms"] != "20000" || len(value.ContentDigest) != 64 {
		t.Fatalf("not normalized: %#v", value)
	}
	value.Configuration["api_key"] = "forbidden"
	if _, err := catalog.NormalizeBackend(value); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("secret-bearing configuration: %v", err)
	}
	value.Configuration = map[string]string{"endpoint": "https://qdrant.example.com?api_key=forbidden", "collection": "knowledge", "vector_size": "1536", "snapshot_watermark": "snapshot-a"}
	if _, err := catalog.NormalizeBackend(value); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("secret-bearing endpoint: %v", err)
	}
}
