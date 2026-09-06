package memory_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/memory"
	memoryinmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/memory/inmemory"
)

func entry(id, ref string) memory.Entry {
	return memory.Entry{Key: memory.Key{TenantID: "tenant-a", Scope: memory.ScopeUser, SubjectID: "user-a", MemoryID: id}, ContentRef: ref, ContentDigest: strings.Repeat("a", 64), Attributes: map[string]string{"kind": "preference"}}
}

func TestReadViewProvidesRequestScopedReadYourWrites(t *testing.T) {
	store := memoryinmemory.New()
	first, err := store.Put(context.Background(), memory.PutRequest{Entry: entry("m1", "memory://old"), ExpectedVersion: 0, MutationID: "mutation-1"})
	if err != nil {
		t.Fatal(err)
	}
	view := memory.NewReadView(store)
	updated := entry("m1", "memory://new")
	value, err := view.Put(context.Background(), memory.PutRequest{Entry: updated, ExpectedVersion: first.Version, MutationID: "mutation-2"})
	if err != nil || value.Version != 2 || value.TenantWatermark != 2 {
		t.Fatalf("put=%#v err=%v", value, err)
	}
	read, err := view.Get(context.Background(), updated.Key)
	if err != nil || read.ContentRef != "memory://new" {
		t.Fatalf("read=%#v err=%v", read, err)
	}
	if _, err = store.Put(context.Background(), memory.PutRequest{Entry: entry("m1", "memory://conflict"), ExpectedVersion: 1, MutationID: "mutation-3"}); err != memory.ErrVersionConflict {
		t.Fatalf("conflict=%v", err)
	}
}

func TestStoreRejectsMutationIDReuseForDifferentWrite(t *testing.T) {
	store := memoryinmemory.New()
	if _, err := store.Put(context.Background(), memory.PutRequest{Entry: entry("m1", "memory://one"), MutationID: "mutation-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), memory.PutRequest{Entry: entry("m1", "memory://other"), MutationID: "mutation-1"}); err != memory.ErrVersionConflict {
		t.Fatalf("mutation reuse error=%v, want %v", err, memory.ErrVersionConflict)
	}
}

func TestReadViewListIsDeterministicAfterOverlayMerge(t *testing.T) {
	store := memoryinmemory.New()
	for _, id := range []string{"m2", "m1"} {
		if _, err := store.Put(context.Background(), memory.PutRequest{Entry: entry(id, "memory://"+id), MutationID: "mutation-" + id}); err != nil {
			t.Fatal(err)
		}
	}
	view := memory.NewReadView(store)
	result, err := view.List(context.Background(), memory.Query{TenantID: "tenant-a", Scope: memory.ScopeUser, SubjectID: "user-a", Limit: 1})
	if err != nil || len(result) != 1 || result[0].MemoryID != "m1" {
		t.Fatalf("list=%#v err=%v", result, err)
	}
}

func TestCacheInvalidatesTenantAndTTLRecoversMissedEvent(t *testing.T) {
	store := memoryinmemory.New()
	first, err := store.Put(context.Background(), memory.PutRequest{Entry: entry("m1", "memory://one"), MutationID: "mutation-1"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	cache := memory.NewCache(store, time.Second)
	cache.Now = func() time.Time { return now }
	if _, err = cache.Get(context.Background(), first.Key); err != nil {
		t.Fatal(err)
	}
	second, err := store.Put(context.Background(), memory.PutRequest{Entry: entry("m1", "memory://two"), ExpectedVersion: first.Version, MutationID: "mutation-2"})
	if err != nil {
		t.Fatal(err)
	}
	stale, err := cache.Get(context.Background(), first.Key)
	if err != nil || stale.ContentRef != "memory://one" {
		t.Fatalf("cache=%#v err=%v", stale, err)
	}
	cache.ApplyInvalidation(memory.Invalidation{TenantID: "tenant-a", Version: second.TenantWatermark})
	fresh, err := cache.Get(context.Background(), first.Key)
	if err != nil || fresh.ContentRef != "memory://two" {
		t.Fatalf("invalidation=%#v err=%v", fresh, err)
	}
	third, err := store.Put(context.Background(), memory.PutRequest{Entry: entry("m1", "memory://three"), ExpectedVersion: second.Version, MutationID: "mutation-3"})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	fresh, err = cache.Get(context.Background(), first.Key)
	if err != nil || fresh.ContentRef != "memory://three" || fresh.TenantWatermark != third.TenantWatermark {
		t.Fatalf("ttl=%#v err=%v", fresh, err)
	}
}
