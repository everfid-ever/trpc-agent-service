BEGIN;

DROP INDEX IF EXISTS public.media_artifact_retention_claim_idx;
DROP TABLE IF EXISTS public.artifact_reference;

ALTER TABLE public.media_artifact
  DROP CONSTRAINT IF EXISTS media_artifact_lifecycle_shape_check,
  DROP COLUMN IF EXISTS lifecycle_version,
  DROP COLUMN IF EXISTS quarantined_at,
  DROP COLUMN IF EXISTS last_error_class,
  DROP COLUMN IF EXISTS delete_attempt,
  DROP COLUMN IF EXISTS claim_until,
  DROP COLUMN IF EXISTS claim_owner,
  DROP COLUMN IF EXISTS lifecycle_state,
  DROP COLUMN IF EXISTS retention_managed;

ALTER TABLE public.prepared_payload
  DROP COLUMN IF EXISTS artifact_retention_seconds;

COMMIT;
