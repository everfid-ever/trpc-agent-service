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
	if len(all) != 36 {
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

func TestGraphPublishModelGuardMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[35]
	if migration.Version != "000036" || migration.Name != "graph_publish_model_guard" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{
		"CREATE OR REPLACE FUNCTION public.guard_agent_model_profile_publish()",
		"NEW.agent_kind = 'llm'",
		"published LLM agent requires a fixed model profile",
		"model profile is missing, inactive, or invalid",
	} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
	if !strings.Contains(migration.Down, "published agent requires a fixed model profile") {
		t.Fatal("down does not restore the original guard")
	}
}

func TestKnowledgeIngestionMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[31]
	if migration.Version != "000032" || migration.Name != "knowledge_ingestion" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.knowledge_manifest (", "CREATE TABLE public.knowledge_chunk (",
		"CREATE TABLE public.knowledge_probe (", "REFERENCES public.tenant(tenant_id)", "knowledge_manifest_guard",
		"knowledge manifest identity is immutable",
		"illegal knowledge manifest state transition", "begin_knowledge_manifest", "stage_knowledge_chunk",
		"begin_knowledge_indexing", "mark_knowledge_chunk_indexed", "begin_knowledge_verifying",
		"record_knowledge_probe", "publish_knowledge_version", "fail_knowledge_version",
		"record_knowledge_migration_mutation", "'upsert'", "v_source_config",
		"knowledge sample verification is incomplete", "knowledge indexing is incomplete",
		"knowledge verification digest does not match chunk set", "knowledge chunk does not match manifest",
		"agent_app_revision_knowledge_published_guard", "agent app revision references unpublished knowledge",
		"state IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe')", "FOR UPDATE"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
	for _, clause := range []string{"DROP TABLE IF EXISTS public.knowledge_probe", "DROP TABLE IF EXISTS public.knowledge_chunk",
		"DROP TABLE IF EXISTS public.knowledge_manifest"} {
		if !strings.Contains(migration.Down, clause) {
			t.Errorf("down missing %q", clause)
		}
	}
}

func TestBusinessAuditRetentionMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[34]
	if migration.Version != "000035" || migration.Name != "business_audit_retention" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.business_audit_purge_batch (",
		"CREATE TABLE public.business_audit_purge_certificate (", "audit_retention_purger",
		"business_audit_watermark", "plan_business_audit_purge", "execute_business_audit_purge", "quarantine_business_audit_purge",
		"state <> 'published'", "purge_authorized", "pg_has_role",
		"session_user", "audit event is immutable", "watermark_drift", "divergence",
		"business audit purge batch identity is immutable", "illegal business audit purge batch transition",
		"business audit purge certificate is immutable", "p_chunk bigint", "LIMIT p_chunk", "not_before > clock_timestamp()",
		"BEFORE UPDATE OR DELETE", "FOR UPDATE", "REVOKE ALL ON FUNCTION"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
	for _, clause := range []string{"DROP TABLE IF EXISTS public.business_audit_purge_certificate",
		"DROP TABLE IF EXISTS public.business_audit_purge_batch", "DROP ROLE IF EXISTS audit_retention_purger"} {
		if !strings.Contains(migration.Down, clause) {
			t.Errorf("down missing %q", clause)
		}
	}
}

func TestKnowledgeMigrationCutoverContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[30]
	if migration.Version != "000031" || migration.Name != "knowledge_migration_cutover" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"ADD COLUMN direction", "knowledge_migration_mutation_direction_idx",
		"record_knowledge_migration_mutation", "p_config_version", "v_direction :=",
		"cutover_knowledge_backend_migration", "begin_knowledge_backend_observation",
		"rollback_knowledge_backend_migration", "cleanup_knowledge_backend_migration",
		"knowledge_backend_migration_drain_status", "direction='reverse'", "knowledge cleanup drain is incomplete",
		"FOR UPDATE"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestKnowledgeMigrationDriverContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[29]
	if migration.Version != "000030" || migration.Name != "knowledge_migration_driver" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.knowledge_migration_mutation (",
		"knowledge_migration_mutation_repair_idx", "record_knowledge_migration_mutation",
		"knowledge migration mutation identity is immutable", "OLD.state='applying' AND NEW.state='applying'",
		"knowledge migration mutation claim is invalid", "illegal knowledge migration mutation transition",
		"knowledge migration mutation result is invalid", "backend_migration_knowledge_repair_gate",
		"knowledge migration repair backlog is not drained"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestSessionMigrationCutoverContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[28]
	if migration.Version != "000029" || migration.Name != "session_migration_cutover" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"ADD COLUMN direction", "backend_migration_config_switch",
		"cutover_session_backend_migration", "begin_session_backend_observation",
		"rollback_session_backend_migration", "cleanup_session_backend_migration",
		"session_backend_migration_drain_status", "direction='reverse'", "migration cleanup drain is incomplete",
		"'config-invalidation'", "FOR UPDATE"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestSessionMigrationDriverContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[27]
	if migration.Version != "000028" || migration.Name != "session_migration_driver" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.session_migration_mutation (",
		"session_migration_mutation_repair_idx", "capture_session_migration_mutation",
		"AFTER INSERT ON public.session_commit", "state IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe')",
		"source_config_version=v_config_version", "session migration mutation identity is immutable",
		"OLD.state='applying' AND NEW.state='applying'", "session migration mutation claim is invalid",
		"illegal session migration mutation transition", "session migration mutation result is invalid",
		"backend_migration_session_repair_gate", "session migration repair backlog is not drained"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestBackendMigrationAuthorityContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[26]
	if migration.Version != "000027" || migration.Name != "backend_migration" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.backend_migration (", "CREATE TABLE public.backend_migration_batch (",
		"backend_binding_migration_coordinate_key", "source_backend_profile_id,source_backend_version",
		"target_backend_profile_id,target_backend_version", "backend_migration_active_domain_idx",
		"guard_backend_migration_update", "backend_migration_batch_immutable",
		"verify_source_count IS NULL OR verify_source_count >= 0", "verify_source_digest ~ '^[0-9a-f]{64}$'",
		"backfill checkpoint update is invalid", "state transition cannot mutate backfill progress",
		"backfill must complete before verification", "verification evidence is incomplete", "rollback sync is incomplete"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestAuditRelayMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[25]
	if migration.Version != "000026" || migration.Name != "audit_relay" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.audit_event", "event_digest", "resource_refs jsonb", "audit_event_tenant_time_idx", "reject_audit_event_change", "audit_event_immutable"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestGovernanceFoundationMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[23]
	if migration.Version != "000024" || migration.Name != "governance_foundation" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.policy_snapshot", "CREATE TABLE public.pricing_snapshot", "CREATE TABLE public.budget_reservation", "CREATE TABLE public.usage_ledger", "CREATE TABLE public.governance_decision", "deny-by-default", "config_snapshot_policy_fk", "reject_governance_snapshot_change"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestDurableConfirmationMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[24]
	if migration.Version != "000025" || migration.Name != "durable_confirmation" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.confirmation", "request_digest", "CREATE TABLE public.confirmation_grant",
		"CREATE TABLE public.tool_attempt", "effect_unknown", "CREATE TABLE public.tool_result_payload", "result_ciphertext bytea",
		"CREATE TABLE public.interaction_payload", "content_ciphertext bytea",
		"CREATE FUNCTION public.suspend_turn", "public.commit_turn", "CREATE FUNCTION public.decide_confirmation", "CREATE FUNCTION public.expire_confirmations", "FOR UPDATE", "REVOKE ALL ON FUNCTION public.suspend_turn"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
	for _, clause := range []string{"DROP TABLE IF EXISTS public.tool_result_payload", "DROP TABLE IF EXISTS public.confirmation"} {
		if !strings.Contains(migration.Down, clause) {
			t.Errorf("down missing %q", clause)
		}
	}
}

func TestPreprocessBindingScopeMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[22]
	if migration.Version != "000023" || migration.Name != "preprocess_binding_scope" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"ADD COLUMN channel_binding_id", "ADD COLUMN config_version", "preprocess_job_channel_binding_fk", "REFERENCES public.channel_binding"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
	}
}

func TestWebUIChannelMigrationContract(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	migration := all[21]
	if migration.Version != "000022" || migration.Name != "webui_channel" {
		t.Fatalf("migration=%#v", migration)
	}
	for _, clause := range []string{"CREATE TABLE public.webui_message", "client_request_id", "provider_message_id", "content_digest", "webui_message_mailbox_idx", "REFERENCES public.execution_record", "CREATE OR REPLACE FUNCTION public.populate_channel_send_secret", "v_version >= 1"} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("missing %q", clause)
		}
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
