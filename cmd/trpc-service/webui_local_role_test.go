package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
)

func TestLoadWebUILocalConfigDefaultsAndRejectsUnsafeInput(t *testing.T) {
	base := map[string]string{"TRPC_POSTGRES_DSN": "postgres://local", "TRPC_REDIS_ADDRESS": "redis:6379"}
	value, err := loadWebUILocalConfig(mapEnvironment(base))
	if err != nil {
		t.Fatal(err)
	}
	if value.ListenAddress != ":8080" || value.RouteKey != webUILocalRouteKey || value.Token != webUILocalToken ||
		value.APIKeyFile != "/run/secrets/deepseek_api_key" {
		t.Fatalf("unexpected defaults: %+v", value)
	}
	for _, item := range []struct{ name, value string }{
		{"TRPC_WEBUI_LOCAL_TOKEN", "short"},
		{"TRPC_WEBUI_DEEPSEEK_KEY_FILE", "relative/key"},
		{"TRPC_WEBUI_LOCAL_SECRET_ROOT", "relative/root"},
	} {
		candidate := cloneEnvironment(base)
		candidate[item.name] = item.value
		if _, err := loadWebUILocalConfig(mapEnvironment(candidate)); err == nil {
			t.Fatalf("%s=%q accepted", item.name, item.value)
		}
	}
}

func TestWriteLocalSecretUsesScopedStableFilenameAndRejectsDrift(t *testing.T) {
	root := t.TempDir()
	scope := secrets.Scope{TenantID: webUILocalTenantID, Subject: webUILocalTenantID,
		Purpose: secrets.PurposePayloadEncrypt, ResourceID: "messaging-payload", ResourceVersion: 1}
	ref := secrets.SecretRef{Ref: payloadKeyRef, Version: 1}
	value := bytes.Repeat([]byte{0x5a}, 32)
	if err := writeLocalSecret(root, scope, ref, value); err != nil {
		t.Fatal(err)
	}
	name, err := secretfs.StableFilename(scope, ref)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(filepath.Join(root, name))
	if err != nil || !bytes.Equal(stored, value) {
		t.Fatalf("stored=%x err=%v", stored, err)
	}
	info, err := os.Stat(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("secret mode=%v", info.Mode().Perm())
	}
	if err := writeLocalSecret(root, scope, ref, bytes.Repeat([]byte{0x6b}, 32)); err == nil {
		t.Fatal("same generation secret drift accepted")
	}
}
