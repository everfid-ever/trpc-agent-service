package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	objectmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/inmemory"
)

func TestObjectBackedArtifactStorePostgreSQL16(t *testing.T) {
	db := openContractDB(t)
	ctx := context.Background()
	const (
		tenantID  = "t_01ARZ3NDEKTSV4RRFFQ69G5FAY"
		appID     = "app_01ARZ3NDEKTSV4RRFFQ69G5FAY"
		requestID = "artifact-object-request"
	)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM media_artifact WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM inbox WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM agent_app WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM tenant WHERE tenant_id=$1`, tenantID)
	})
	if _, err := db.ExecContext(ctx, `INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES($1,'artifact-contract','Artifact Contract')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_app(tenant_id,agent_app_id,agent_app_key,display_name) VALUES($1,$2,'artifact-contract','Artifact Contract')`, tenantID, appID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO inbox(tenant_id,channel,external_account_id,external_message_id,request_id,agent_app_id,session_id,state,payload_ref,payload_digest,key_version)
VALUES($1,'artifact','account','message',$2,$3,'session','dispatch_pending','inbound://artifact',repeat('a',64),1)`, tenantID, requestID, appID); err != nil {
		t.Fatal(err)
	}

	objects := objectmemory.New()
	store := NewWithObjectStore(db, objects)
	firstInput := contractArtifact(t, tenantID, requestID, 0, []byte("object-backed content"))
	if _, err := NewWithObjectStore(db, nil).PutArtifact(ctx, firstInput); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("nil object store err=%v", err)
	}
	first, err := store.PutArtifact(ctx, firstInput)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := store.PutArtifact(ctx, firstInput)
	if err != nil || repeated.ArtifactRef != first.ArtifactRef {
		t.Fatalf("repeated=%#v err=%v", repeated, err)
	}
	loaded, err := store.GetArtifact(ctx, tenantID, first.ArtifactID)
	if err != nil || string(loaded.Content) != "object-backed content" {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	var storageKind, objectKey string
	var contentIsNull bool
	var contentSize int64
	if err := db.QueryRowContext(ctx, `SELECT storage_kind,object_key,content IS NULL,content_size FROM media_artifact WHERE tenant_id=$1 AND artifact_id=$2`,
		tenantID, first.ArtifactID).Scan(&storageKind, &objectKey, &contentIsNull, &contentSize); err != nil {
		t.Fatal(err)
	}
	if storageKind != storageObject || !contentIsNull || contentSize != int64(len(firstInput.Content)) ||
		strings.Contains(objectKey, tenantID) || strings.Contains(objectKey, requestID) {
		t.Fatalf("kind=%q key=%q null=%t size=%d", storageKind, objectKey, contentIsNull, contentSize)
	}
	if _, err := New(db).GetArtifact(ctx, tenantID, first.ArtifactID); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("object row read without object store err=%v", err)
	}
	if _, err := NewWithObjectStore(db, objectmemory.New()).GetArtifact(ctx, tenantID, first.ArtifactID); !errors.Is(err, runtime.ErrBackendUnavailable) {
		t.Fatalf("missing object err=%v", err)
	}

	legacy := New(db)
	legacyInput := contractArtifact(t, tenantID, requestID, 1, []byte("postgres bytea content"))
	legacyRecord, err := legacy.PutArtifact(ctx, legacyInput)
	if err != nil {
		t.Fatal(err)
	}
	rollingRead, err := store.GetArtifact(ctx, tenantID, legacyRecord.ArtifactID)
	if err != nil || string(rollingRead.Content) != "postgres bytea content" {
		t.Fatalf("rolling read=%#v err=%v", rollingRead, err)
	}
	collision := firstInput
	collision.Content = []byte("changed content")
	digest := sha256.Sum256(collision.Content)
	collision.ContentDigest = hex.EncodeToString(digest[:])
	if _, err := store.PutArtifact(ctx, collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("collision err=%v", err)
	}
}

func contractArtifact(t *testing.T, tenantID, requestID string, ordinal int, content []byte) artifact.Record {
	t.Helper()
	source := sha256.Sum256([]byte("source-" + strconv.Itoa(ordinal)))
	sourceDigest := hex.EncodeToString(source[:])
	artifactID, artifactRef, err := artifact.StableIdentity(tenantID, requestID, ordinal, sourceDigest)
	if err != nil {
		t.Fatal(err)
	}
	contentDigest := sha256.Sum256(content)
	return artifact.Record{TenantID: tenantID, RequestID: requestID, ArtifactID: artifactID, ArtifactRef: artifactRef,
		Ordinal: ordinal, SourceDigest: sourceDigest, ContentDigest: hex.EncodeToString(contentDigest[:]), MediaType: "text/plain",
		Kind: "file", Content: content, MalwareScanVersion: "av-contract-1", DLPVersion: "dlp-contract-1"}
}

func openContractDB(t *testing.T) *sql.DB {
	t.Helper()
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
	t.Cleanup(func() { _ = db.Close() })
	var databaseName string
	var serverMajor int
	if err := db.QueryRowContext(context.Background(), `SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&databaseName, &serverMajor); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(databaseName, "trpc_agent_service_test_") || serverMajor != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", databaseName, serverMajor)
	}
	return db
}
