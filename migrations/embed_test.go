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
	if len(all) != 7 {
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

func TestProviderProfileMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[5]
	if migration.Version != "000006" || migration.Name != "provider_profiles" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.model_profile (", "CREATE TABLE public.model_profile_revision (", "CREATE TABLE public.backend_profile_revision (", "backend_binding_profile_version_fk", "guard_profile_revision_immutable"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
	if strings.Contains(migration.Up, "legacy direct backend bindings must be republished") || !strings.Contains(migration.Up, "Migrated ") {
		t.Fatal("legacy backend bindings are not deterministically migrated")
	}
}

func TestDurableSessionHistoryMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[6]
	if migration.Version != "000007" || migration.Name != "durable_session_history" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"ADD COLUMN event_payload jsonb", "CREATE TABLE public.result_payload (", "agent_revision_model_profile_version_fk", "agent_model_profile_publish_guard"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestAgentDefinitionV1MigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[4]
	if migration.Version != "000005" || migration.Name != "agent_definition_v1" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{
		"ADD COLUMN agent_spec jsonb", "agent_kind IN ('llm', 'graph', 'chain', 'parallel', 'cycle')",
		"CREATE TABLE agent_app_revision_child (", "CREATE TABLE agent_app_revision_skill (",
		"guard_agent_app_revision_child", "guard_revision_child_write",
	} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestInboundPayloadMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[3]
	if migration.Version != "000004" || migration.Name != "inbound_payload" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE inbound_payload (", "payload_ciphertext bytea NOT NULL", "payload_nonce bytea NOT NULL", "key_version bigint NOT NULL", "PRIMARY KEY (tenant_id, request_id)", "UNIQUE (tenant_id, payload_ref)", "FOREIGN KEY (tenant_id)"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestControlPlaneTriggerSearchPathMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[2]
	if migration.Version != "000003" || migration.Name != "control_plane_trigger_search_path" {
		t.Fatalf("migration=%#v", migration)
	}
	if strings.Count(migration.Up, "FROM public.agent_app_revision") != 3 {
		t.Fatal("trigger functions must schema-qualify every agent_app_revision lookup")
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
	for _, migration := range all {
		up := migration.Up
		if strings.Count(up, "AS $$") != strings.Count(up, "$$;") {
			t.Fatalf("%s has unpaired function bodies: opens=%d closes=%d", migration.Name, strings.Count(up, "AS $$"), strings.Count(up, "$$;"))
		}
		if !strings.HasPrefix(strings.TrimSpace(up), "BEGIN;") || !strings.HasSuffix(strings.TrimSpace(up), "COMMIT;") {
			t.Fatalf("%s must be transaction wrapped", migration.Name)
		}
	}
}

func TestRuntimeConsistencyMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	runtimeMigration := all[1]
	if runtimeMigration.Version != "000002" || runtimeMigration.Name != "runtime_consistency" {
		t.Fatalf("migration=%#v", runtimeMigration)
	}
	for _, clause := range []string{
		"CREATE TABLE session_head (", "CREATE TABLE session_event (", "CREATE TABLE session_commit (",
		"CREATE UNIQUE INDEX session_commit_terminal_input_idx", "CREATE TABLE inbox (",
		"CREATE TABLE execution_record (", "CREATE TABLE delivery_ledger (",
		"CREATE OR REPLACE FUNCTION claim_inbox(", "CREATE OR REPLACE FUNCTION prepare_dispatch(",
		"CREATE OR REPLACE FUNCTION commit_turn(", "FOR UPDATE", "ON CONFLICT", "REVOKE ALL ON FUNCTION",
		"CREATE OR REPLACE FUNCTION request_cancel_execution(", "CREATE OR REPLACE FUNCTION park_execution(",
	} {
		if !strings.Contains(runtimeMigration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
	for _, clause := range []string{
		"PRIMARY KEY (tenant_id, agent_app_id, session_id)",
		"UNIQUE (tenant_id, request_id, event_seq)",
		"UNIQUE (tenant_id, kind, idempotency_key)",
		"FOREIGN KEY (tenant_id, request_id)",
	} {
		combined := all[0].Up + runtimeMigration.Up
		if !strings.Contains(combined, clause) {
			t.Errorf("missing tenant-leading runtime constraint %q", clause)
		}
	}
	if !strings.Contains(runtimeMigration.Down, "DROP TABLE IF EXISTS session_head;") ||
		!strings.Contains(runtimeMigration.Down, "DROP FUNCTION IF EXISTS commit_turn(") {
		t.Fatal("runtime down migration is incomplete")
	}
	for _, clause := range []string{
		"v_execution public.execution_record%ROWTYPE", "execution scope mismatch",
		"UPDATE public.execution_record SET outcome = p_outcome", "already_terminal boolean",
	} {
		if !strings.Contains(runtimeMigration.Up, clause) {
			t.Errorf("missing commit authority clause %q", clause)
		}
	}
}
