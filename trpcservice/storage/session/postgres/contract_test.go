package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/contracttest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestAtomicSessionStoreContractPostgreSQL16(t *testing.T) {
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

	contracttest.Run(t, func(tb testing.TB, key sessionstore.SessionKey, prepared map[string]uint64) sessionstore.AtomicSessionStore {
		tb.Helper()
		prepareContractFixture(tb, db, key, prepared)
		return New(db)
	})
}

func TestClaimInboxPrepareDispatchPostgreSQL16(t *testing.T) {
	db := openTestDB(t)
	key := sessionstore.SessionKey{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", AgentAppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV", SessionID: "prepare-session"}
	prepareContractFixture(t, db, key, map[string]uint64{})
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `UPDATE tenant SET status='active',version=version+1 WHERE tenant_id=$1 AND status='suspended'`, key.TenantID); err != nil {
		t.Fatal(err)
	}
	var tenantVersion int64
	if err := db.QueryRowContext(ctx, `UPDATE tenant SET active_config_version=1,default_agent_app_id=$2,version=version+1
WHERE tenant_id=$1 AND active_config_version IS NULL RETURNING version`, key.TenantID, key.AgentAppID).Scan(&tenantVersion); err != nil {
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, `SELECT version FROM tenant WHERE tenant_id=$1`, key.TenantID).Scan(&tenantVersion); err != nil {
			t.Fatal(err)
		}
	}
	inboxes := messagingpostgres.New(db)
	inboxKey := messaging.InboxKey{TenantID: key.TenantID, Channel: "fake", ExternalAccountID: "contract-account", ExternalMessageID: "prepare-message"}
	claim := messaging.ClaimInboxRequest{InboxKey: inboxKey, RequestID: "prepare-request", AgentAppID: key.AgentAppID, SessionID: key.SessionID, PayloadRef: "payload://prepare", PayloadDigest: strings.Repeat("d", 64), KeyVersion: 1, InitialState: messaging.InboxDispatchPending}
	first, err := inboxes.ClaimInbox(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	claim.RequestID = "ignored-duplicate-request"
	again, err := inboxes.ClaimInbox(ctx, claim)
	if err != nil || again.RequestID != first.RequestID {
		t.Fatalf("duplicate claim=%#v err=%v", again, err)
	}
	claim.PayloadDigest = strings.Repeat("e", 64)
	if _, err := inboxes.ClaimInbox(ctx, claim); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("digest collision=%v", err)
	}
	tasks := gatewaypostgres.NewTaskStore(db)
	request := gateway.PrepareDispatchRequest{
		Tenant:    tenant.Context{TenantID: key.TenantID, TenantVersion: tenantVersion, AgentAppID: key.AgentAppID, SubjectID: "user", Channel: "fake", TrustedSource: "channel_binding:contract"},
		Binding:   tenant.ExecutionBinding{AgentAppVersion: 2, AgentAppRevision: 1, AgentContentDigest: strings.Repeat("a", 64), ConfigVersion: 1, PolicyVersion: 1},
		RequestID: first.RequestID, SessionID: key.SessionID, UserID: "user", PayloadRef: "payload://prepare",
	}
	prepared, err := tasks.PrepareDispatch(ctx, request)
	if err != nil || !prepared.Accepted || prepared.Envelope.InputSeq != 1 {
		t.Fatalf("prepare=%#v err=%v", prepared, err)
	}
	repeated, err := tasks.PrepareDispatch(ctx, request)
	if err != nil || repeated.Envelope != prepared.Envelope {
		t.Fatalf("repeat prepare=%#v err=%v", repeated, err)
	}
	var dispatchCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE tenant_id=$1 AND kind='dispatch' AND aggregate_id=$2`, key.TenantID, first.RequestID).Scan(&dispatchCount); err != nil || dispatchCount != 1 {
		t.Fatalf("dispatch outbox=%d err=%v", dispatchCount, err)
	}
	claimedOutbox, err := inboxes.ClaimOutbox(ctx, "dispatch", 10, "contract-relay", time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("claimed outbox=%#v err=%v", claimedOutbox, err)
	}
	targetFound := false
	for _, outboxRecord := range claimedOutbox {
		renewedVersion, renewErr := inboxes.RenewOutboxClaim(ctx, outboxRecord.TenantID, outboxRecord.OutboxID, outboxRecord.Version, "contract-relay", time.Now().Add(2*time.Second))
		if renewErr != nil || renewedVersion <= outboxRecord.Version {
			t.Fatalf("renewed version=%d err=%v", renewedVersion, renewErr)
		}
		if err := inboxes.MarkPublished(ctx, outboxRecord.TenantID, outboxRecord.OutboxID, renewedVersion); err != nil {
			t.Fatal(err)
		}
		if outboxRecord.TenantID == key.TenantID && outboxRecord.AggregateID == first.RequestID {
			targetFound = true
		}
	}
	if !targetFound {
		t.Fatalf("request dispatch was not claimed: %#v", claimedOutbox)
	}
	var suspendedVersion int64
	if err := db.QueryRowContext(ctx, `SELECT transition_tenant_status($1,$2,'suspended','test','contract','test',NULL,'contract','contract',NULL)`, key.TenantID, tenantVersion).Scan(&suspendedVersion); err != nil {
		t.Fatal(err)
	}
	deniedKey := messaging.InboxKey{TenantID: key.TenantID, Channel: "fake", ExternalAccountID: "contract-account", ExternalMessageID: "suspended-message"}
	deniedClaim, err := inboxes.ClaimInbox(ctx, messaging.ClaimInboxRequest{InboxKey: deniedKey, RequestID: "suspended-request", AgentAppID: key.AgentAppID, SessionID: "suspended-session", PayloadRef: "payload://suspended", PayloadDigest: strings.Repeat("f", 64), KeyVersion: 1, InitialState: messaging.InboxDispatchPending})
	if err != nil {
		t.Fatal(err)
	}
	request.Tenant.TenantVersion = suspendedVersion
	request.RequestID, request.SessionID, request.PayloadRef = deniedClaim.RequestID, deniedClaim.SessionID, deniedClaim.PayloadRef
	denied, err := tasks.PrepareDispatch(ctx, request)
	if err != nil || denied.Accepted || denied.TerminalReason != "suspended" {
		t.Fatalf("suspended prepare=%#v err=%v", denied, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM execution_record WHERE tenant_id=$1 AND request_id=$2`, key.TenantID, deniedClaim.RequestID).Scan(&dispatchCount); err != nil || dispatchCount != 0 {
		t.Fatalf("suspended execution count=%d err=%v", dispatchCount, err)
	}
}

func openTestDB(t *testing.T) *sql.DB {
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

func prepareContractFixture(tb testing.TB, db *sql.DB, key sessionstore.SessionKey, prepared map[string]uint64) {
	tb.Helper()
	ctx := context.Background()
	statements := []string{
		`TRUNCATE session_event,session_commit,session_summary,execution_record,inbox,session_head CASCADE`,
		`INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES
('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','atomic-contract','Atomic Contract') ON CONFLICT DO NOTHING`,
		`INSERT INTO agent_app(tenant_id,agent_app_id,agent_app_key,display_name,status,current_revision,next_revision,version)
VALUES('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','atomic-contract','Atomic Contract','disabled',NULL,2,1)
ON CONFLICT DO NOTHING`,
		`INSERT INTO agent_app_revision(tenant_id,agent_app_id,revision,state,draft_version,agent_kind,schema_version,instruction,model_profile_id,model_profile_version,content_digest,published_at)
VALUES('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV',1,'published',1,'llm',1,'contract','model',1,repeat('a',64),now())
ON CONFLICT DO NOTHING`,
		`UPDATE agent_app SET status='active',current_revision=1,version=version+1
WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND agent_app_id='app_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND current_revision IS NULL`,
		`INSERT INTO config_snapshot(tenant_id,config_version,schema_version,payload,content_digest,state,actor_id,reason_code,correlation_id,trace_id,published_at)
VALUES('t_01ARZ3NDEKTSV4RRFFQ69G5FAV',1,1,'{"policy_version":1}'::jsonb,repeat('b',64),'published','contract','test','contract','contract',now())
ON CONFLICT DO NOTHING`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			tb.Fatalf("fixture statement: %v", err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO session_head(tenant_id,agent_app_id,session_id,last_allocated_input_seq)
VALUES($1,$2,$3,$4)`, key.TenantID, key.AgentAppID, key.SessionID, maxInput(prepared)); err != nil {
		tb.Fatal(err)
	}
	for requestID, inputSeq := range prepared {
		externalID := fmt.Sprintf("%s-%d", key.SessionID, inputSeq)
		if _, err := db.ExecContext(ctx, `INSERT INTO inbox(tenant_id,channel,external_account_id,external_message_id,request_id,agent_app_id,session_id,input_seq,state,payload_ref,payload_digest,key_version)
VALUES($1,'fake','contract',$2,$3,$4,$5,$6,'dispatch_ready',$7,repeat('c',64),1)`,
			key.TenantID, externalID, requestID, key.AgentAppID, key.SessionID, inputSeq, "payload://"+requestID); err != nil {
			tb.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO execution_record(tenant_id,request_id,tenant_version,agent_app_id,agent_app_version,agent_app_revision,agent_content_digest,config_version,policy_version,session_id,user_id,channel,input_seq,payload_ref)
VALUES($1,$2,1,$3,2,1,repeat('a',64),1,1,$4,'user','fake',$5,$6)`,
			key.TenantID, requestID, key.AgentAppID, key.SessionID, inputSeq, "payload://"+requestID); err != nil {
			tb.Fatal(err)
		}
	}
}

func maxInput(prepared map[string]uint64) uint64 {
	var result uint64
	for _, input := range prepared {
		if input > result {
			result = input
		}
	}
	return result
}
