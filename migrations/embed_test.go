package migrations

import (
	"strings"
	"testing"
)

func serviceSchemaBaseline(t *testing.T) Migration {
	t.Helper()
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("migrations=%d, want one acceptance baseline", len(all))
	}
	migration := all[0]
	if migration.Version != "000001" || migration.Name != "service_schema" {
		t.Fatalf("migration=%#v", migration)
	}
	return migration
}

func TestServiceSchemaBaselineContainsFinalPlatformContract(t *testing.T) {
	migration := serviceSchemaBaseline(t)
	for _, clause := range []string{
		"CREATE TABLE public.tenant", "CREATE TABLE public.agent_app", "CREATE TABLE public.agent_app_revision",
		"CREATE TABLE public.config_snapshot", "CREATE TABLE public.channel_binding", "CREATE TABLE public.backend_profile",
		"CREATE TABLE public.backend_binding", "CREATE TABLE public.session_head", "CREATE TABLE public.session_event",
		"CREATE TABLE public.session_summary", "CREATE TABLE public.inbox", "CREATE TABLE public.execution_record",
		"CREATE TABLE public.outbox", "CREATE TABLE public.delivery_ledger", "CREATE TABLE public.preprocess_job",
		"CREATE TABLE public.media_artifact", "CREATE TABLE public.artifact_reference", "CREATE TABLE public.knowledge_manifest",
		"CREATE TABLE public.knowledge_chunk", "CREATE TABLE public.policy_snapshot", "CREATE TABLE public.confirmation",
		"CREATE TABLE public.audit_event", "CREATE TABLE public.backend_migration", "CREATE TABLE public.session_migration_mutation",
		"CREATE TABLE public.knowledge_migration_mutation", "CREATE FUNCTION public.claim_channel_inbox", "CREATE FUNCTION public.prepare_dispatch",
		"CREATE FUNCTION public.commit_turn", "CREATE FUNCTION public.guard_outbox_idempotency", "CREATE FUNCTION public.begin_session_backend_observation",
		"CREATE FUNCTION public.begin_knowledge_backend_observation", "CREATE ROLE audit_retention_purger",
		"UNIQUE (tenant_id, kind, idempotency_key)", "GRANT ALL ON FUNCTION public.execute_business_audit_purge",
	} {
		if !strings.Contains(migration.Up, clause) {
			t.Errorf("baseline lacks %q", clause)
		}
	}
}

func TestServiceSchemaBaselineIsTransactionWrappedAndResettable(t *testing.T) {
	migration := serviceSchemaBaseline(t)
	if _, err := transactionBody(migration.Up); err != nil {
		t.Fatalf("baseline up: %v", err)
	}
	if _, err := transactionBody(migration.Down); err != nil {
		t.Fatalf("baseline down: %v", err)
	}
	for _, clause := range []string{"DROP SCHEMA public CASCADE", "CREATE SCHEMA public", "GRANT USAGE ON SCHEMA public TO PUBLIC", "CREATE TABLE public.schema_migrations"} {
		if !strings.Contains(migration.Down, clause) {
			t.Errorf("baseline down lacks %q", clause)
		}
	}
}
