package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
)

func TestMemoryStorePostgreSQL16(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var database string
	var major int
	if err = db.QueryRowContext(context.Background(), `SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&database, &major); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(database, "trpc_agent_service_test_") || major != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", database, major)
	}
	ctx := context.Background()
	tenantID := "t_01ARZ3NDEKTSV4RRFFQ69G5FAM"
	meta := tenant.ChangeMetadata{ActorType: "test", ActorID: "memory", ReasonCode: "contract", CorrelationID: "contract", TraceID: "contract"}
	if _, err = tenantpostgres.New(db).Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: tenantID, TenantKey: "memory-contract", DisplayName: "Memory Contract"}, ChangeMetadata: meta}); err != nil {
		t.Fatal(err)
	}
	store := New(db)
	key := memory.Key{TenantID: tenantID, Scope: memory.ScopeUser, SubjectID: "user", MemoryID: "preference"}
	put := func(ref, mutation string, expected int64) memory.PutRequest {
		return memory.PutRequest{Entry: memory.Entry{Key: key, ContentRef: ref, ContentDigest: strings.Repeat("a", 64), Attributes: map[string]string{"kind": "preference"}}, ExpectedVersion: expected, MutationID: mutation}
	}
	first, err := store.Put(ctx, put("memory://one", "m1", 0))
	if err != nil || first.Version != 1 || first.TenantWatermark != 1 {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	retry, err := store.Put(ctx, put("memory://one", "m1", 0))
	if err != nil || retry.Version != first.Version || retry.TenantWatermark != first.TenantWatermark || retry.ContentRef != first.ContentRef {
		t.Fatalf("retry=%#v err=%v", retry, err)
	}
	if _, err = store.Put(ctx, put("memory://bad", "m2", 0)); !errors.Is(err, memory.ErrVersionConflict) {
		t.Fatalf("stale=%v", err)
	}
	second, err := store.Put(ctx, put("memory://two", "m2", first.Version))
	if err != nil || second.Version != 2 || second.TenantWatermark != 2 {
		t.Fatalf("second=%#v err=%v", second, err)
	}
	loaded, err := store.Get(ctx, key)
	if err != nil || loaded.ContentRef != "memory://two" {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	watermark, err := store.TenantWatermark(ctx, tenantID)
	if err != nil || watermark != second.TenantWatermark {
		t.Fatalf("watermark=%d err=%v", watermark, err)
	}
	var count int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM memory_index_intent WHERE tenant_id=$1 AND state='pending'`, tenantID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("intents=%d err=%v", count, err)
	}
}
