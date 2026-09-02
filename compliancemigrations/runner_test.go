package compliancemigrations

import (
	"strings"
	"testing"
)

func allForTest(t *testing.T) []Migration {
	t.Helper()
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	return all
}

func TestComplianceMigrationsAreOrderedAndComplete(t *testing.T) {
	all := allForTest(t)
	if len(all) != 2 {
		t.Fatalf("compliance migrations=%d, want 2", len(all))
	}
	if all[0].Version != "000001" || all[1].Version != "000002" {
		t.Fatalf("unexpected migration order: %s then %s", all[0].Version, all[1].Version)
	}
}

func TestComplianceMigrationFreezesImmutableAuditAndQuarantineTables(t *testing.T) {
	up := allForTest(t)[0].Up
	for _, clause := range []string{"CREATE TABLE compliance.audit_event", "PRIMARY KEY (tenant_id, audit_id)",
		"event_json->>'tenant_id' = tenant_id",
		"CREATE TABLE compliance.quarantine_alert", "resource_kind IN ('upload', 'retention')",
		"compliance_quarantine_alert_open_idx", "compliance.reject_immutable_change"} {
		if !strings.Contains(up, clause) {
			t.Fatalf("compliance migration lacks %q", clause)
		}
	}
}

func TestComplianceRetentionMigrationBuildsGuardedPurgePath(t *testing.T) {
	all := allForTest(t)
	up := all[1].Up
	for _, clause := range []string{
		"CREATE TABLE compliance.audit_retention_floor",
		"CREATE TABLE compliance.audit_retention_policy",
		"CREATE TABLE compliance.audit_legal_hold",
		"CREATE TABLE compliance.audit_quarantine_resolution",
		"CREATE TABLE compliance.audit_purge_batch",
		"CREATE TABLE compliance.audit_purge_certificate",
		"CREATE TABLE compliance.audit_query_record",
		"compliance.execute_audit_purge_batch",
		"compliance.quarantine_audit_purge_batch",
		"compliance.plan_audit_purge_batch",
		"compliance.approve_audit_purge_batch",
		"pg_has_role(session_user, 'compliance_purger', 'MEMBER')",
		"current_setting('compliance.purge_authorized', true)",
		"SET search_path = pg_catalog, compliance",
		"REVOKE ALL ON FUNCTION compliance.execute_audit_purge_batch",
		"GRANT EXECUTE ON FUNCTION compliance.execute_audit_purge_batch(text,text,text) TO compliance_purger",
		"compliance.reject_any_change",
		"ON CONFLICT (tenant_id,batch_id) DO NOTHING",
		"not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int)",
		"RETURN 'completed'",
		"RETURN 'divergence'",
		"RETURN 'unresolved_quarantine'",
		"RETURN 'preview_expired'",
		"RETURN 'claimed_by_another'",
		"LIMIT v_chunk",
		"purge candidate set exceeds max batch size",
	} {
		if !strings.Contains(up, clause) {
			t.Fatalf("compliance retention migration lacks %q", clause)
		}
	}
}

func TestComplianceMigrationsAreTransactionWrapped(t *testing.T) {
	for _, migration := range allForTest(t) {
		if _, err := transactionBody(migration.Up); err != nil {
			t.Fatalf("migration %s up: %v", migration.Version, err)
		}
		if _, err := transactionBody(migration.Down); err != nil {
			t.Fatalf("migration %s down: %v", migration.Version, err)
		}
	}
}

func TestComplianceRetentionDownRefusesAfterPurge(t *testing.T) {
	down := allForTest(t)[1].Down
	if !strings.Contains(down, "cannot roll back compliance retention after purge execution") {
		t.Fatal("down migration must refuse after purge execution")
	}
}
