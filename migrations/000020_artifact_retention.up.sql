BEGIN;

ALTER TABLE public.prepared_payload
  ADD COLUMN artifact_retention_seconds bigint NOT NULL DEFAULT 0
    CHECK (artifact_retention_seconds >= 0);

ALTER TABLE public.media_artifact
  ADD COLUMN retention_managed boolean NOT NULL DEFAULT false,
  ADD COLUMN lifecycle_state text NOT NULL DEFAULT 'active'
    CHECK (lifecycle_state IN ('active','delete_claimed','quarantined')),
  ADD COLUMN claim_owner text,
  ADD COLUMN claim_until timestamptz,
  ADD COLUMN delete_attempt integer NOT NULL DEFAULT 0 CHECK (delete_attempt >= 0),
  ADD COLUMN last_error_class text,
  ADD COLUMN quarantined_at timestamptz,
  ADD COLUMN lifecycle_version bigint NOT NULL DEFAULT 1 CHECK (lifecycle_version >= 1),
  ADD CONSTRAINT media_artifact_lifecycle_shape_check CHECK (
    (lifecycle_state = 'active' AND claim_owner IS NULL AND claim_until IS NULL AND quarantined_at IS NULL)
    OR
    (lifecycle_state = 'delete_claimed' AND claim_owner IS NOT NULL AND claim_until IS NOT NULL AND quarantined_at IS NULL)
    OR
    (lifecycle_state = 'quarantined' AND claim_owner IS NULL AND claim_until IS NULL
      AND quarantined_at IS NOT NULL AND last_error_class IS NOT NULL)
  );

CREATE TABLE public.artifact_reference (
  tenant_id text NOT NULL,
  artifact_id text NOT NULL,
  reference_kind text NOT NULL CHECK (reference_kind IN ('prepared_payload')),
  reference_id text NOT NULL,
  retain_until timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, artifact_id, reference_kind, reference_id),
  FOREIGN KEY (tenant_id, artifact_id)
    REFERENCES public.media_artifact(tenant_id, artifact_id) ON DELETE CASCADE,
  CHECK (length(btrim(reference_id)) > 0)
);

CREATE INDEX artifact_reference_retention_idx
  ON public.artifact_reference(retain_until, tenant_id, artifact_id);

CREATE INDEX media_artifact_retention_claim_idx
  ON public.media_artifact(lifecycle_state, claim_until, created_at, tenant_id, artifact_id);

COMMIT;
