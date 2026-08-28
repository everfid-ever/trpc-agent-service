package inmemory_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
	objectmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/inmemory"
)

func TestStoreIsImmutableAndTenantScoped(t *testing.T) {
	sourceDigest := hex.EncodeToString(make([]byte, sha256.Size))
	artifactID, _, err := artifact.StableIdentity("tenant-a", "request-1", 0, sourceDigest)
	if err != nil {
		t.Fatal(err)
	}
	objectKey, err := objectstore.StableKey("tenant-a", artifactID)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("safe object")
	digest := sha256.Sum256(content)
	value := objectstore.Object{TenantID: "tenant-a", ObjectKey: objectKey,
		ContentDigest: hex.EncodeToString(digest[:]), Content: content}
	store := objectmemory.New()
	first, err := store.PutObject(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	value.Content[0] = 'X'
	loaded, err := store.GetObject(context.Background(), "tenant-a", objectKey)
	if err != nil || string(loaded.Content) != "safe object" || first.ObjectKey != objectKey {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	if _, err := store.GetObject(context.Background(), "tenant-b", objectKey); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-tenant err=%v", err)
	}
	collision := loaded
	collision.Content = []byte("other object")
	otherDigest := sha256.Sum256(collision.Content)
	collision.ContentDigest = hex.EncodeToString(otherDigest[:])
	if _, err := store.PutObject(context.Background(), collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("collision err=%v", err)
	}
	if err := store.DeleteObject(context.Background(), "tenant-a", objectKey, collision.ContentDigest); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("wrong digest delete err=%v", err)
	}
	if err := store.DeleteObject(context.Background(), "tenant-a", objectKey, loaded.ContentDigest); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteObject(context.Background(), "tenant-a", objectKey, loaded.ContentDigest); err != nil {
		t.Fatalf("idempotent delete err=%v", err)
	}
	if _, err := store.GetObject(context.Background(), "tenant-a", objectKey); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("deleted object err=%v", err)
	}
}
