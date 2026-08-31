package compliancemigrations

import (
	"strings"
	"testing"
)

func TestComplianceMigrationFreezesImmutableAuditAndQuarantineTables(t *testing.T) {
	for _, clause := range []string{"CREATE TABLE compliance.audit_event", "PRIMARY KEY (tenant_id, audit_id)",
		"event_json->>'tenant_id' = tenant_id",
		"CREATE TABLE compliance.quarantine_alert", "resource_kind IN ('upload', 'retention')",
		"compliance_quarantine_alert_open_idx", "compliance.reject_immutable_change"} {
		if !strings.Contains(migration, clause) {
			t.Fatalf("compliance migration lacks %q", clause)
		}
	}
	if _, err := transactionBody(migration); err != nil {
		t.Fatal(err)
	}
	if _, err := transactionBody(downMigration); err != nil {
		t.Fatal(err)
	}
}
