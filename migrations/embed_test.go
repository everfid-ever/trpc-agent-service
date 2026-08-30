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
	if len(all) != 21 {
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

func TestChannelSendCredentialsMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[20]
	if migration.Version != "000021" || migration.Name != "channel_send_credentials" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"send_secret_ref text", "send_secret_version bigint", "populate_channel_send_secret", "BEFORE INSERT", "channel IN ('feishu', 'wecom')", "REVOKE ALL ON FUNCTION"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestArtifactRetentionMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[19]
	if migration.Version != "000020" || migration.Name != "artifact_retention" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"artifact_retention_seconds", "retention_managed boolean NOT NULL DEFAULT false", "CREATE TABLE public.artifact_reference", "retain_until", "delete_claimed", "quarantined", "media_artifact_lifecycle_shape_check", "media_artifact_retention_claim_idx"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestArtifactObjectLifecycleMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[18]
	if migration.Version != "000019" || migration.Name != "artifact_object_lifecycle" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.artifact_object_upload", "protect_until", "cleanup_claimed", "quarantined", "cleanup_attempt", "last_error_class", "artifact_object_upload_cleanup_idx"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestArtifactObjectStoreMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[17]
	if migration.Version != "000018" || migration.Name != "artifact_object_store" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"storage_kind", "object_key", "content_size", "media_artifact_storage_shape_check", "media_artifact_object_lifecycle_idx", "while object-backed rows exist"} {
		if !strings.Contains(migration.Up+migration.Down, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestPreparedPayloadMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[15]
	if migration.Version != "000016" || migration.Name != "prepared_payload" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"prepared_payload_ref", "CREATE TABLE public.prepared_payload", "source_payload_ref", "preprocess_job_prepared_ref_complete"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestMediaArtifactMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[16]
	if migration.Version != "000017" || migration.Name != "media_artifact" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.media_artifact", "source_digest", "malware_scan_version", "media_artifact_request_idx"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestChannelPreprocessPipelineMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[14]
	if migration.Version != "000015" || migration.Name != "channel_preprocess_pipeline" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"identity_secret_ref", "session_secret_ref", "CREATE TABLE public.preprocess_job", "preprocess_job_claim_idx", "execution_requires_preprocess", "reject_unpreprocessed_execution", "ERRCODE = 'P0904'"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestChannelIngressFoundationMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[13]
	if migration.Version != "000014" || migration.Name != "channel_ingress_foundation" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"external_chat_id text", "claim_channel_inbox(", "channel_public_route", "channel_binding_locator", "channel_ingress_candidate", "verifier_acquired", "receipt_token_digest", "channel_ingress_candidate_expiry_idx"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestDeliveryRetryPolicyMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[12]
	if migration.Version != "000013" || migration.Name != "delivery_retry_policy" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"reconcile_attempt integer", "delivery_ledger_reconcile_attempt_check"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestDeliveryAttemptRecoveryMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[11]
	if migration.Version != "000012" || migration.Name != "delivery_attempt_recovery" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"client_request_id text", "claim_owner text", "claim_until timestamptz", "delivery_ledger_claim_check", "delivery_ledger_claim_expiry_idx", "state='ambiguous'"} {
		if !strings.Contains(migration.Up+migration.Down, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestExecutionCancelIntentMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[10]
	if migration.Version != "000011" || migration.Name != "execution_cancel_intent" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"cancel_requested_at timestamptz", "cancel_version bigint", "execution_record_cancel_intent_check", "CREATE TABLE public.execution_cancel_intent", "actor_id text", "reason_code text", "guard_execution_cancel_intent", "t.status='disabled'", "cancelled outcome requires a durable cancellation intent", "execution_cancel_intent_guard", "ERRCODE = 'P0902'", "cancel-intent-audit", "'execution-control'", "RETURNS TABLE(accepted boolean, execution_version bigint, cancel_version bigint)"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestInputParkRecoveryHardeningMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[9]
	if migration.Version != "000010" || migration.Name != "input_park_recovery_hardening" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"p_max_attempts > 64", "LEAST(v_deadline", "ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestInputParkWakeupMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[8]
	if migration.Version != "000009" || migration.Name != "input_park_wakeup" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"park_deadline timestamptz", "'blocked'", "execution_record_park_state_check", "p_max_attempts integer", "CREATE FUNCTION public.park_execution(", "enqueue_next_parked_wakeup", "session_head_wakeup_next_parked", "inspect_execution_wakeup"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestDeliveryRelayMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[7]
	if migration.Version != "000008" || migration.Name != "delivery_relay" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"renderer_version text", "format_version text", "content_digest text", "segment_count integer", "'ambiguous'", "delivery_ledger_retry_idx"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
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
