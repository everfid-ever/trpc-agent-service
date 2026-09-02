BEGIN;

-- Refuse to roll back after any purge has executed: destruction is one-way.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM compliance.audit_purge_batch WHERE state = 'completed') THEN
    RAISE EXCEPTION 'cannot roll back compliance retention after purge execution' USING ERRCODE = '55000';
  END IF;
END;
$$;

DROP TRIGGER IF EXISTS audit_query_record_immutable ON compliance.audit_query_record;
DROP TRIGGER IF EXISTS audit_purge_certificate_immutable ON compliance.audit_purge_certificate;
DROP TRIGGER IF EXISTS audit_purge_batch_guard ON compliance.audit_purge_batch;
DROP TRIGGER IF EXISTS audit_quarantine_resolution_immutable ON compliance.audit_quarantine_resolution;
DROP TRIGGER IF EXISTS audit_legal_hold_immutable ON compliance.audit_legal_hold;
DROP TRIGGER IF EXISTS audit_retention_policy_guard ON compliance.audit_retention_policy;
DROP TRIGGER IF EXISTS audit_retention_floor_guard ON compliance.audit_retention_floor;

DROP INDEX IF EXISTS compliance.compliance_audit_event_global_time_idx;

DROP FUNCTION IF EXISTS compliance.quarantine_audit_purge_batch(text,text,text,text);
DROP FUNCTION IF EXISTS compliance.execute_audit_purge_batch(text,text,text);
DROP FUNCTION IF EXISTS compliance.approve_audit_purge_batch(text,text,text,text);
DROP FUNCTION IF EXISTS compliance.plan_audit_purge_batch(text,text,timestamptz,text,text,interval,bigint);
DROP FUNCTION IF EXISTS compliance.audit_effective_retention(text,text);
DROP FUNCTION IF EXISTS compliance.audit_event_on_hold(text,timestamptz);
DROP FUNCTION IF EXISTS compliance.audit_retention_class(text);
DROP FUNCTION IF EXISTS compliance.guard_audit_purge_batch_update();
DROP FUNCTION IF EXISTS compliance.guard_audit_retention_policy();
DROP FUNCTION IF EXISTS compliance.guard_audit_retention_floor();
DROP FUNCTION IF EXISTS compliance.reject_any_change();

DROP TABLE IF EXISTS compliance.audit_query_record;
DROP TABLE IF EXISTS compliance.audit_purge_certificate;
DROP TABLE IF EXISTS compliance.audit_purge_batch;
DROP TABLE IF EXISTS compliance.audit_quarantine_resolution;
DROP TABLE IF EXISTS compliance.audit_legal_hold;
DROP TABLE IF EXISTS compliance.audit_retention_policy;
DROP TABLE IF EXISTS compliance.audit_retention_class_rule;
DROP TABLE IF EXISTS compliance.audit_retention_floor;

-- restore the strict immutable guard from 000001
CREATE OR REPLACE FUNCTION compliance.reject_immutable_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'compliance fact is immutable' USING ERRCODE = '55000';
END;
$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'compliance_purger') THEN
    DROP ROLE compliance_purger;
  END IF;
END;
$$;

COMMIT;
