package main

import (
	"strings"
	"testing"
	"time"
)

func TestLoadProductionConfigRequiresEveryCriticalDependency(t *testing.T) {
	values := validProductionEnvironment()
	config, err := loadProductionConfig(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if config.ListenAddress != ":8080" || config.RedisDB != 2 || !config.S3PathStyle ||
		config.ArtifactOrphanGrace != 48*time.Hour || config.ArtifactPutTimeout >= config.UploadProtection {
		t.Fatal("parsed production configuration mismatch")
	}
	for _, name := range []string{"TRPC_POSTGRES_DSN", "TRPC_REDIS_ADDRESS", "TRPC_SECRET_ROOT", "TRPC_S3_REGION", "TRPC_S3_BUCKET",
		"TRPC_CLAMAV_ADDRESS", "TRPC_DLP_ENDPOINT", "TRPC_DLP_PROBE_TENANT_ID", "TRPC_DLP_SECRET_REF",
		"TRPC_DLP_SECRET_VERSION", "TRPC_DLP_BACKEND_VERSION"} {
		copy := cloneEnvironment(values)
		delete(copy, name)
		if _, err := loadProductionConfig(mapEnvironment(copy)); err == nil {
			t.Fatalf("missing %s accepted", name)
		}
	}
}

func TestLoadProductionConfigRejectsUnsafeLifecycleValuesWithoutLeakingSecrets(t *testing.T) {
	for name, value := range map[string]string{
		"TRPC_ARTIFACT_PUT_TIMEOUT":          "3m",
		"TRPC_ARTIFACT_LIFECYCLE_BATCH_SIZE": "1001",
		"TRPC_ARTIFACT_ORPHAN_GRACE":         "1s",
		"TRPC_S3_ALLOW_INSECURE":             "sometimes",
	} {
		values := validProductionEnvironment()
		values[name] = value
		_, err := loadProductionConfig(mapEnvironment(values))
		if err == nil {
			t.Fatalf("%s=%q accepted", name, value)
		}
		if strings.Contains(err.Error(), "bearer-secret") {
			t.Fatalf("secret leaked in %v", err)
		}
	}
}

func validProductionEnvironment() map[string]string {
	return map[string]string{
		"TRPC_POSTGRES_DSN":               "postgres://service:secret@postgres/service",
		"TRPC_REDIS_ADDRESS":              "redis:6379",
		"TRPC_REDIS_PASSWORD":             "bearer-secret",
		"TRPC_REDIS_DB":                   "2",
		"TRPC_SECRET_ROOT":                "/var/run/secrets/trpc-agent-service",
		"TRPC_S3_REGION":                  "us-east-1",
		"TRPC_S3_BUCKET":                  "artifacts",
		"TRPC_S3_ENDPOINT":                "https://s3.example.test",
		"TRPC_S3_PATH_STYLE":              "true",
		"TRPC_CLAMAV_ADDRESS":             "clamav:3310",
		"TRPC_DLP_ENDPOINT":               "https://dlp.example.test/",
		"TRPC_DLP_PROBE_TENANT_ID":        "probe-tenant",
		"TRPC_DLP_SECRET_REF":             "secret://dlp/service",
		"TRPC_DLP_SECRET_VERSION":         "3",
		"TRPC_DLP_BACKEND_VERSION":        "5",
		"TRPC_ARTIFACT_ORPHAN_GRACE":      "48h",
		"TRPC_ARTIFACT_UPLOAD_PROTECTION": "2m",
		"TRPC_ARTIFACT_PUT_TIMEOUT":       "30s",
	}
}

func mapEnvironment(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func cloneEnvironment(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
