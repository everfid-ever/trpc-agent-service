BEGIN;

ALTER TABLE public.media_artifact
  ALTER COLUMN content DROP NOT NULL,
  ADD COLUMN storage_kind text NOT NULL DEFAULT 'postgres_bytea',
  ADD COLUMN object_key text,
  ADD COLUMN content_size bigint;

UPDATE public.media_artifact
SET content_size = octet_length(content)
WHERE content_size IS NULL;

ALTER TABLE public.media_artifact
  ALTER COLUMN content_size SET NOT NULL,
  ADD CONSTRAINT media_artifact_content_size_check CHECK (content_size > 0),
  ADD CONSTRAINT media_artifact_storage_kind_check CHECK (storage_kind IN ('postgres_bytea','object')),
  ADD CONSTRAINT media_artifact_storage_shape_check CHECK (
    (storage_kind = 'postgres_bytea' AND content IS NOT NULL AND object_key IS NULL AND content_size = octet_length(content))
    OR
    (storage_kind = 'object' AND content IS NULL AND object_key ~ '^artifacts/v1/[A-Za-z0-9_-]{43}/a1_[A-Za-z0-9_-]{43}$')
  ),
  ADD CONSTRAINT media_artifact_object_key_unique UNIQUE (tenant_id, object_key);

CREATE INDEX media_artifact_object_lifecycle_idx
  ON public.media_artifact(tenant_id, created_at, object_key)
  WHERE storage_kind = 'object';

COMMIT;
