BEGIN;

ALTER TABLE public.session_event ADD COLUMN event_payload jsonb;

CREATE TABLE public.result_payload (
  tenant_id text NOT NULL,
  request_id text NOT NULL,
  result_ref text NOT NULL,
  result_ciphertext bytea NOT NULL,
  result_nonce bytea NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  key_version bigint NOT NULL CHECK (key_version >= 1),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, request_id),
  UNIQUE (tenant_id, result_ref),
  FOREIGN KEY (tenant_id, request_id) REFERENCES public.execution_record(tenant_id, request_id),
  FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

-- Legacy agent revisions are preserved but imported profiles are suspended so
-- they cannot pass the new publish gate until an operator validates them.
INSERT INTO public.model_profile(
  tenant_id, model_profile_id, profile_key, display_name, status, current_version
)
SELECT DISTINCT r.tenant_id, r.model_profile_id,
  'legacy-' || substr(md5(r.model_profile_id), 1, 24),
  'Migrated ' || r.model_profile_id || ' model', 'suspended', NULL::bigint
FROM public.agent_app_revision r
WHERE r.model_profile_id IS NOT NULL AND r.model_profile_version IS NOT NULL
ON CONFLICT (tenant_id, model_profile_id) DO NOTHING;

INSERT INTO public.model_profile_revision(
  tenant_id, model_profile_id, profile_version, schema_version, provider,
  model_name, content_digest
)
SELECT DISTINCT r.tenant_id, r.model_profile_id, r.model_profile_version, 1,
  'unresolved', r.model_profile_id,
  md5(r.model_profile_id || ':' || r.model_profile_version::text)
    || md5('model:' || r.model_profile_id || ':' || r.model_profile_version::text)
FROM public.agent_app_revision r
WHERE r.model_profile_id IS NOT NULL AND r.model_profile_version IS NOT NULL
ON CONFLICT (tenant_id, model_profile_id, profile_version) DO NOTHING;

UPDATE public.model_profile p SET current_version = versions.profile_version
FROM (
  SELECT tenant_id, model_profile_id, max(profile_version) AS profile_version
  FROM public.model_profile_revision GROUP BY tenant_id, model_profile_id
) versions
WHERE p.tenant_id = versions.tenant_id
  AND p.model_profile_id = versions.model_profile_id
  AND p.current_version IS NULL;

ALTER TABLE public.agent_app_revision
  ADD CONSTRAINT agent_revision_model_profile_version_fk
  FOREIGN KEY (tenant_id, model_profile_id, model_profile_version)
  REFERENCES public.model_profile_revision(tenant_id, model_profile_id, profile_version);

CREATE OR REPLACE FUNCTION public.guard_agent_model_profile_publish()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF NEW.state = 'published' AND OLD.state = 'draft' THEN
    IF NEW.model_profile_id IS NULL OR NEW.model_profile_version IS NULL THEN
      RAISE EXCEPTION 'published agent requires a fixed model profile' USING ERRCODE = '23503';
    END IF;
    PERFORM 1 FROM public.model_profile p
      JOIN public.model_profile_revision v USING (tenant_id, model_profile_id)
      WHERE p.tenant_id = NEW.tenant_id
        AND p.model_profile_id = NEW.model_profile_id
        AND v.profile_version = NEW.model_profile_version
        AND p.status = 'active'
        AND v.content_digest ~ '^[0-9a-f]{64}$';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'model profile is missing, inactive, or invalid' USING ERRCODE = '23503';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER agent_model_profile_publish_guard
BEFORE UPDATE OF state ON public.agent_app_revision
FOR EACH ROW EXECUTE FUNCTION public.guard_agent_model_profile_publish();

CREATE OR REPLACE FUNCTION public.unpack_session_event_payload()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE v_wrapper jsonb;
BEGIN
  BEGIN
    v_wrapper := NEW.payload_ref::jsonb;
  EXCEPTION WHEN others THEN
    RAISE EXCEPTION 'session event payload wrapper missing' USING ERRCODE = '23514';
  END;
  IF jsonb_typeof(v_wrapper) <> 'object'
     OR COALESCE(v_wrapper->>'ref', '') = ''
     OR v_wrapper->'payload' IS NULL
     OR jsonb_typeof(v_wrapper->'payload') <> 'object' THEN
    RAISE EXCEPTION 'invalid session event payload wrapper' USING ERRCODE = '23514';
  END IF;
  NEW.payload_ref := v_wrapper->>'ref';
  NEW.event_payload := v_wrapper->'payload';
  RETURN NEW;
END;
$$;

CREATE TRIGGER session_event_unpack_payload
BEFORE INSERT ON public.session_event
FOR EACH ROW EXECUTE FUNCTION public.unpack_session_event_payload();

COMMIT;
