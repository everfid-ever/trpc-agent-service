BEGIN;

ALTER TABLE public.backend_profile RENAME COLUMN version TO row_version;
ALTER TABLE public.backend_profile
  ADD COLUMN profile_key text,
  ADD COLUMN current_version bigint;
UPDATE public.backend_profile SET profile_key = backend_profile_id;
ALTER TABLE public.backend_profile
  ALTER COLUMN profile_key SET NOT NULL,
  ADD CONSTRAINT backend_profile_key_unique UNIQUE (tenant_id, profile_key),
  ADD CONSTRAINT backend_profile_key_check CHECK (length(btrim(profile_key)) BETWEEN 1 AND 128);

CREATE TABLE public.backend_profile_revision (
  tenant_id text NOT NULL,
  backend_profile_id text NOT NULL,
  profile_version bigint NOT NULL CHECK (profile_version >= 1),
  schema_version integer NOT NULL CHECK (schema_version >= 1),
  provider text NOT NULL CHECK (length(btrim(provider)) BETWEEN 1 AND 128),
  configuration jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(configuration) = 'object'),
  credential_ref text,
  credential_version bigint CHECK (credential_version >= 1),
  capabilities text[] NOT NULL DEFAULT '{}',
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, backend_profile_id, profile_version),
  FOREIGN KEY (tenant_id, backend_profile_id)
    REFERENCES public.backend_profile(tenant_id, backend_profile_id),
  CHECK ((credential_ref IS NULL) = (credential_version IS NULL))
);

-- Convert every legacy direct binding into a dedicated, suspended immutable
-- profile. The upgrade is deterministic and lossless, while fail-closed status
-- forces an operator to validate the imported provider configuration before it
-- can be selected for new execution.
INSERT INTO public.backend_profile(
  tenant_id, backend_profile_id, display_name, status, row_version,
  profile_key, current_version
)
SELECT b.tenant_id,
  'legacy_' || md5(b.config_version::text || ':' || b.domain),
  'Migrated ' || b.domain || ' backend', 'suspended', 1,
  'legacy-' || b.config_version::text || '-' || substr(md5(b.domain), 1, 16), 1
FROM public.backend_binding b
ON CONFLICT (tenant_id, backend_profile_id) DO NOTHING;

INSERT INTO public.backend_profile_revision(
  tenant_id, backend_profile_id, profile_version, schema_version, provider,
  configuration, credential_ref, credential_version, capabilities, content_digest
)
SELECT b.tenant_id,
  'legacy_' || md5(b.config_version::text || ':' || b.domain),
  1, 1, b.backend_type,
  jsonb_build_object('backend_ref', b.backend_ref),
  b.credential_ref, b.credential_version, b.capabilities,
  md5(b.backend_type || ':' || b.backend_ref || ':' || b.credential_ref || ':' || b.credential_version::text || ':' || b.capabilities::text)
    || md5('profile:' || b.backend_type || ':' || b.backend_ref || ':' || b.credential_ref || ':' || b.credential_version::text || ':' || b.capabilities::text)
FROM public.backend_binding b
ON CONFLICT (tenant_id, backend_profile_id, profile_version) DO NOTHING;

ALTER TABLE public.backend_profile
  ADD CONSTRAINT backend_profile_current_version_fk
  FOREIGN KEY (tenant_id, backend_profile_id, current_version)
  REFERENCES public.backend_profile_revision(tenant_id, backend_profile_id, profile_version)
  DEFERRABLE INITIALLY IMMEDIATE;

CREATE TABLE public.model_profile (
  tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
  model_profile_id text NOT NULL,
  profile_key text NOT NULL,
  display_name text NOT NULL,
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'disabled')),
  current_version bigint,
  row_version bigint NOT NULL DEFAULT 1 CHECK (row_version >= 1),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, model_profile_id),
  UNIQUE (tenant_id, profile_key),
  CHECK (length(btrim(profile_key)) BETWEEN 1 AND 128)
);

CREATE TABLE public.model_profile_revision (
  tenant_id text NOT NULL,
  model_profile_id text NOT NULL,
  profile_version bigint NOT NULL CHECK (profile_version >= 1),
  schema_version integer NOT NULL CHECK (schema_version >= 1),
  provider text NOT NULL CHECK (length(btrim(provider)) BETWEEN 1 AND 128),
  model_name text NOT NULL CHECK (length(btrim(model_name)) BETWEEN 1 AND 256),
  endpoint text NOT NULL DEFAULT '',
  options jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(options) = 'object'),
  secret_ref text,
  secret_version bigint CHECK (secret_version >= 1),
  generation jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(generation) = 'object'),
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, model_profile_id, profile_version),
  FOREIGN KEY (tenant_id, model_profile_id)
    REFERENCES public.model_profile(tenant_id, model_profile_id),
  CHECK ((secret_ref IS NULL) = (secret_version IS NULL))
);

ALTER TABLE public.model_profile
  ADD CONSTRAINT model_profile_current_version_fk
  FOREIGN KEY (tenant_id, model_profile_id, current_version)
  REFERENCES public.model_profile_revision(tenant_id, model_profile_id, profile_version)
  DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE public.backend_binding
	ADD COLUMN backend_version bigint,
	ADD COLUMN required text[] NOT NULL DEFAULT '{}';
UPDATE public.backend_binding SET
	backend_profile_id = 'legacy_' || md5(config_version::text || ':' || domain),
	backend_version = 1,
	required = capabilities;
ALTER TABLE public.backend_binding
  DROP COLUMN backend_type,
  DROP COLUMN backend_ref,
  DROP COLUMN credential_ref,
  DROP COLUMN credential_version,
  DROP COLUMN capabilities,
  ALTER COLUMN backend_profile_id SET NOT NULL,
	ALTER COLUMN backend_version SET NOT NULL,
	ADD CONSTRAINT backend_binding_backend_version_check CHECK (backend_version >= 1),
  ADD CONSTRAINT backend_binding_profile_version_fk
    FOREIGN KEY (tenant_id, backend_profile_id, backend_version)
    REFERENCES public.backend_profile_revision(tenant_id, backend_profile_id, profile_version);

CREATE OR REPLACE FUNCTION public.guard_profile_identity()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id OR NEW.profile_key <> OLD.profile_key
     OR (TG_TABLE_NAME = 'model_profile'
       AND to_jsonb(NEW)->>'model_profile_id' <> to_jsonb(OLD)->>'model_profile_id')
     OR (TG_TABLE_NAME = 'backend_profile'
       AND to_jsonb(NEW)->>'backend_profile_id' <> to_jsonb(OLD)->>'backend_profile_id') THEN
    RAISE EXCEPTION 'profile identity is immutable' USING ERRCODE = '55000';
  END IF;
  IF OLD.status = 'disabled' AND NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'disabled profile is immutable' USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER model_profile_identity_guard BEFORE UPDATE ON public.model_profile
FOR EACH ROW EXECUTE FUNCTION public.guard_profile_identity();
CREATE TRIGGER backend_profile_identity_guard BEFORE UPDATE ON public.backend_profile
FOR EACH ROW EXECUTE FUNCTION public.guard_profile_identity();

CREATE OR REPLACE FUNCTION public.guard_profile_revision_immutable()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'profile revisions are immutable' USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER model_profile_revision_immutable BEFORE UPDATE OR DELETE ON public.model_profile_revision
FOR EACH ROW EXECUTE FUNCTION public.guard_profile_revision_immutable();
CREATE TRIGGER backend_profile_revision_immutable BEFORE UPDATE OR DELETE ON public.backend_profile_revision
FOR EACH ROW EXECUTE FUNCTION public.guard_profile_revision_immutable();

CREATE OR REPLACE FUNCTION public.publish_config_snapshot(
  p_tenant_id text, p_expected_tenant_version bigint, p_schema_version integer,
  p_payload jsonb, p_content_digest text, p_default_agent_app_id text,
  p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text,
  p_traceparent text
) RETURNS TABLE(config_version bigint, tenant_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  v_status text;
  v_current_version bigint;
  v_config_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'config publish metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  SELECT status, version INTO v_status, v_current_version
    FROM public.tenant WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_status = 'disabled' THEN RAISE EXCEPTION 'disabled tenant is immutable' USING ERRCODE = '55000'; END IF;
  IF v_current_version <> p_expected_tenant_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE = '40001'; END IF;
  IF p_schema_version <> 1 OR jsonb_typeof(p_payload) <> 'object'
     OR p_content_digest !~ '^[0-9a-f]{64}$'
     OR p_payload->>'default_agent_app_id' IS DISTINCT FROM p_default_agent_app_id
     OR COALESCE((p_payload->>'policy_version')::bigint, 0) < 1 THEN
    RAISE EXCEPTION 'invalid config snapshot' USING ERRCODE = '22023';
  END IF;
  PERFORM 1 FROM public.agent_app
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_default_agent_app_id
      AND status = 'active' AND current_revision IS NOT NULL;
  IF NOT FOUND THEN RAISE EXCEPTION 'default app is not active' USING ERRCODE = '23503'; END IF;
  SELECT COALESCE(max(cs.config_version), 0) + 1 INTO v_config_version
    FROM public.config_snapshot cs WHERE cs.tenant_id = p_tenant_id;
  INSERT INTO public.config_snapshot(
    tenant_id, config_version, schema_version, payload, content_digest, state,
    actor_id, reason_code, correlation_id, trace_id
  ) VALUES (
    p_tenant_id, v_config_version, p_schema_version, p_payload, p_content_digest,
    'published', p_actor_id, p_reason_code, p_correlation_id, p_trace_id
  );
  INSERT INTO public.channel_binding(
    tenant_id, config_version, binding_id, channel, external_account_id,
    agent_app_id, secret_ref, secret_version
  )
  SELECT p_tenant_id, v_config_version, item.binding_id, item.channel,
    item.external_account_id, item.agent_app_id,
    item.secret_ref->>'ref', (item.secret_ref->>'version')::bigint
  FROM jsonb_to_recordset(COALESCE(p_payload->'channel_bindings', '[]'::jsonb))
    AS item(binding_id text, channel text, external_account_id text, agent_app_id text, secret_ref jsonb);
  INSERT INTO public.backend_binding(
    tenant_id, config_version, domain, backend_profile_id, backend_version, required
  )
  SELECT p_tenant_id, v_config_version, item.domain, item.backend_profile_id,
    item.backend_version,
    ARRAY(SELECT jsonb_array_elements_text(COALESCE(item.required, '[]'::jsonb)))
  FROM jsonb_to_recordset(COALESCE(p_payload->'backend_bindings', '[]'::jsonb))
    AS item(domain text, backend_profile_id text, backend_version bigint, required jsonb);
  UPDATE public.tenant SET
    default_agent_app_id = p_default_agent_app_id,
    active_config_version = v_config_version,
    version = v_current_version + 1
  WHERE tenant_id = p_tenant_id;
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('config-audit:%s:%s', p_tenant_id, v_config_version), 'audit', p_tenant_id, v_config_version, format('config:%s:%s:audit', p_tenant_id, v_config_version), format('config://%s/%s', p_tenant_id, v_config_version), p_traceparent),
    (p_tenant_id, format('config-invalidation:%s:%s', p_tenant_id, v_config_version), 'config-invalidation', p_tenant_id, v_config_version, format('config:%s:%s:invalidate', p_tenant_id, v_config_version), format('config://%s/%s', p_tenant_id, v_config_version), p_traceparent);
  RETURN QUERY SELECT v_config_version, v_current_version + 1;
END;
$$;

COMMIT;
