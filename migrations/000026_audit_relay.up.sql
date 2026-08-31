BEGIN;

CREATE TABLE public.audit_event (
  tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
  audit_id text NOT NULL,
  schema_version integer NOT NULL CHECK (schema_version = 1),
  channel text NOT NULL DEFAULT '',
  user_id text NOT NULL DEFAULT '',
  session_id text NOT NULL DEFAULT '',
  request_id text NOT NULL DEFAULT '',
  agent_app_id text NOT NULL DEFAULT '',
  agent_app_revision bigint NOT NULL DEFAULT 0 CHECK (agent_app_revision >= 0),
  agent_name text NOT NULL DEFAULT '',
  tool_name text NOT NULL DEFAULT '',
  action text NOT NULL,
  decision text NOT NULL,
  reason_code text NOT NULL DEFAULT '',
  latency_ms bigint NOT NULL DEFAULT 0 CHECK (latency_ms >= 0),
  error_type text NOT NULL DEFAULT '',
  cost_micros bigint NOT NULL DEFAULT 0 CHECK (cost_micros >= 0),
  currency text NOT NULL DEFAULT '',
  input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
  output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
  config_version bigint NOT NULL DEFAULT 0 CHECK (config_version >= 0),
  policy_version bigint NOT NULL DEFAULT 0 CHECK (policy_version >= 0),
  content_digest text NOT NULL DEFAULT '',
  trace_id text NOT NULL DEFAULT '',
  resource_refs jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(resource_refs)='array'),
  occurred_at timestamptz NOT NULL,
  event_digest text NOT NULL CHECK (event_digest ~ '^[0-9a-f]{64}$'),
  exported_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,audit_id),
  CHECK (length(btrim(audit_id)) BETWEEN 1 AND 256),
  CHECK (length(btrim(action)) BETWEEN 1 AND 128),
  CHECK (length(btrim(decision)) BETWEEN 1 AND 128),
  CHECK (currency='' OR currency ~ '^[A-Z]{3}$'),
  CHECK (content_digest='' OR content_digest ~ '^[0-9a-f]{64}$'),
  CHECK (trace_id='' OR trace_id ~ '^[0-9a-f]{32}$')
);
CREATE INDEX audit_event_tenant_time_idx ON public.audit_event(tenant_id,occurred_at DESC,audit_id);
CREATE INDEX audit_event_request_idx ON public.audit_event(tenant_id,request_id,occurred_at) WHERE request_id<>'';

CREATE FUNCTION public.reject_audit_event_change() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'audit event is immutable' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER audit_event_immutable BEFORE UPDATE OR DELETE ON public.audit_event
FOR EACH ROW EXECUTE FUNCTION public.reject_audit_event_change();

COMMIT;
