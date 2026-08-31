BEGIN;

CREATE SCHEMA compliance;

CREATE TABLE compliance.audit_event (
  tenant_id text NOT NULL,
  audit_id text NOT NULL,
  schema_version integer NOT NULL CHECK (schema_version = 1),
  event_json jsonb NOT NULL CHECK (jsonb_typeof(event_json) = 'object'),
  event_digest text NOT NULL CHECK (event_digest ~ '^[0-9a-f]{64}$'),
  occurred_at timestamptz NOT NULL,
  received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, audit_id),
  CHECK (length(btrim(tenant_id)) BETWEEN 1 AND 128),
  CHECK (length(btrim(audit_id)) BETWEEN 1 AND 256),
  CHECK (event_json->>'tenant_id' = tenant_id),
  CHECK (event_json->>'audit_id' = audit_id),
  CHECK ((event_json->>'schema_version')::integer = schema_version)
);
CREATE INDEX compliance_audit_event_tenant_time_idx
  ON compliance.audit_event(tenant_id, occurred_at DESC, audit_id);

CREATE TABLE compliance.quarantine_alert (
  tenant_id text NOT NULL,
  audit_id text NOT NULL,
  resource_kind text NOT NULL CHECK (resource_kind IN ('upload', 'retention')),
  artifact_id text NOT NULL,
  resource_version bigint NOT NULL CHECK (resource_version >= 1),
  request_id text NOT NULL DEFAULT '',
  error_type text NOT NULL,
  resource_ref text NOT NULL,
  event_digest text NOT NULL CHECK (event_digest ~ '^[0-9a-f]{64}$'),
  occurred_at timestamptz NOT NULL,
  received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  state text NOT NULL DEFAULT 'open' CHECK (state = 'open'),
  PRIMARY KEY (tenant_id, audit_id),
  FOREIGN KEY (tenant_id, audit_id) REFERENCES compliance.audit_event(tenant_id, audit_id),
  CHECK (length(btrim(artifact_id)) BETWEEN 1 AND 256),
  CHECK (length(btrim(error_type)) BETWEEN 1 AND 128),
  CHECK (length(btrim(resource_ref)) BETWEEN 1 AND 1024)
);
CREATE INDEX compliance_quarantine_alert_open_idx
  ON compliance.quarantine_alert(received_at, tenant_id, audit_id) WHERE state = 'open';

CREATE FUNCTION compliance.reject_immutable_change() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'compliance fact is immutable' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER compliance_audit_event_immutable BEFORE UPDATE OR DELETE ON compliance.audit_event
FOR EACH ROW EXECUTE FUNCTION compliance.reject_immutable_change();
CREATE TRIGGER compliance_quarantine_alert_immutable BEFORE UPDATE OR DELETE ON compliance.quarantine_alert
FOR EACH ROW EXECUTE FUNCTION compliance.reject_immutable_change();

COMMIT;
