package migrations

import (
	"strings"
	"testing"
)

func TestControlPlaneMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("migrations=%d", len(all))
	}
	up := all[0].Up
	required := []string{"CREATE TABLE tenant (", "CREATE TABLE agent_app (", "CREATE TABLE agent_app_revision (", "CREATE TABLE config_snapshot (", "CREATE TABLE channel_binding (", "CREATE TABLE backend_binding (", "FOREIGN KEY (tenant_id, default_agent_app_id)", "CREATE OR REPLACE FUNCTION publish_agent_app_revision(", "CREATE OR REPLACE FUNCTION rollback_agent_app_revision(", "CREATE OR REPLACE FUNCTION publish_config_snapshot(", "CREATE OR REPLACE FUNCTION transition_tenant_status(", "CREATE TRIGGER config_snapshot_immutable", "REVOKE ALL ON FUNCTION"}
	for _, clause := range required {
		if !strings.Contains(up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
	if !strings.Contains(all[0].Down, "DROP TABLE IF EXISTS tenant;") {
		t.Fatal("down migration does not remove tenant")
	}
}

func TestTenantOwnedConstraintsAreTenantLeading(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	up := all[0].Up
	for _, clause := range []string{"PRIMARY KEY (tenant_id, agent_app_id)", "PRIMARY KEY (tenant_id, config_version)", "UNIQUE (tenant_id, config_version, channel, external_account_id)", "FOREIGN KEY (tenant_id, agent_app_id)", "FOREIGN KEY (tenant_id, config_version)"} {
		if !strings.Contains(up, clause) {
			t.Errorf("missing tenant-leading constraint %q", clause)
		}
	}
}

func TestMigrationFunctionBodiesArePaired(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	up := all[0].Up
	if strings.Count(up, "AS $$") != strings.Count(up, "$$;") {
		t.Fatalf("unpaired function bodies: opens=%d closes=%d", strings.Count(up, "AS $$"), strings.Count(up, "$$;"))
	}
	if !strings.HasPrefix(strings.TrimSpace(up), "BEGIN;") || !strings.HasSuffix(strings.TrimSpace(up), "COMMIT;") {
		t.Fatal("migration must be transaction wrapped")
	}
}
