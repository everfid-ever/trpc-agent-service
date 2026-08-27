BEGIN;

ALTER TABLE public.channel_binding_locator
  ADD COLUMN identity_secret_ref text,
  ADD COLUMN identity_secret_version bigint CHECK (identity_secret_version >= 1),
  ADD COLUMN session_secret_ref text,
  ADD COLUMN session_secret_version bigint CHECK (session_secret_version >= 1),
  ADD CONSTRAINT channel_binding_locator_identity_secret_complete CHECK (
    (identity_secret_ref IS NULL AND identity_secret_version IS NULL) OR
    (length(btrim(identity_secret_ref)) > 0 AND identity_secret_version IS NOT NULL)
  ),
  ADD CONSTRAINT channel_binding_locator_session_secret_complete CHECK (
    (session_secret_ref IS NULL AND session_secret_version IS NULL) OR
    (length(btrim(session_secret_ref)) > 0 AND session_secret_version IS NOT NULL)
  );

CREATE TABLE public.preprocess_job (
  tenant_id text NOT NULL,
  request_id text NOT NULL,
  job_id text NOT NULL,
  tenant_version bigint NOT NULL CHECK (tenant_version >= 1),
  agent_app_id text NOT NULL,
  session_id text NOT NULL,
  user_id text NOT NULL,
  channel text NOT NULL,
  payload_ref text NOT NULL,
  traceparent text NOT NULL DEFAULT '',
  state text NOT NULL DEFAULT 'pending',
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  lease_owner text,
  lease_until timestamptz,
  not_before timestamptz NOT NULL DEFAULT now(),
  reject_reason text NOT NULL DEFAULT '',
  dispatched_at timestamptz,
  version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, job_id),
  UNIQUE (tenant_id, request_id),
  FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id),
  CHECK (state IN ('pending','running','ready','rejected','retry_wait')),
  CHECK ((lease_owner IS NULL AND lease_until IS NULL AND state <> 'running') OR
         (lease_owner IS NOT NULL AND lease_until IS NOT NULL AND state IN ('running','ready'))),
  CHECK (dispatched_at IS NULL OR state = 'ready')
);

CREATE INDEX preprocess_job_claim_idx
  ON public.preprocess_job(state, not_before, lease_until, created_at);
CREATE INDEX preprocess_job_ready_idx
  ON public.preprocess_job(created_at) WHERE state = 'ready' AND dispatched_at IS NULL;

CREATE OR REPLACE FUNCTION public.reject_unpreprocessed_execution()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE v_state text;
BEGIN
  SELECT state INTO v_state FROM public.inbox
    WHERE tenant_id = NEW.tenant_id AND request_id = NEW.request_id;
  IF v_state = 'preprocess_pending' THEN
    RAISE EXCEPTION 'execution requires completed preprocess' USING ERRCODE = 'P0904';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER execution_requires_preprocess
BEFORE INSERT ON public.execution_record
FOR EACH ROW EXECUTE FUNCTION public.reject_unpreprocessed_execution();

COMMIT;
