BEGIN;

CREATE TABLE public.artifact_object_upload (
  tenant_id text NOT NULL,
  object_key text NOT NULL,
  artifact_id text NOT NULL,
  request_id text NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  content_size bigint NOT NULL CHECK (content_size > 0),
  state text NOT NULL DEFAULT 'uploading' CHECK (state IN ('uploading','cleanup_claimed','quarantined')),
  protect_until timestamptz NOT NULL,
  claim_owner text,
  claim_until timestamptz,
  cleanup_attempt integer NOT NULL DEFAULT 0 CHECK (cleanup_attempt >= 0),
  last_error_class text,
  quarantined_at timestamptz,
  version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, object_key),
  UNIQUE (tenant_id, artifact_id),
  FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id),
  FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id),
  CHECK (object_key ~ '^artifacts/v1/[A-Za-z0-9_-]{43}/a1_[A-Za-z0-9_-]{43}$'),
  CHECK (artifact_id ~ '^a1_[A-Za-z0-9_-]{43}$'),
  CHECK (
    (state = 'uploading' AND claim_owner IS NULL AND claim_until IS NULL AND quarantined_at IS NULL)
    OR
    (state = 'cleanup_claimed' AND claim_owner IS NOT NULL AND claim_until IS NOT NULL AND quarantined_at IS NULL)
    OR
    (state = 'quarantined' AND claim_owner IS NULL AND claim_until IS NULL
      AND quarantined_at IS NOT NULL AND last_error_class IS NOT NULL)
  )
);

CREATE INDEX artifact_object_upload_cleanup_idx
  ON public.artifact_object_upload(state, protect_until, claim_until, created_at);

COMMIT;
