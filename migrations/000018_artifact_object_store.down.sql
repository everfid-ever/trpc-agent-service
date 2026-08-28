BEGIN;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.media_artifact WHERE storage_kind = 'object') THEN
    RAISE EXCEPTION 'cannot remove artifact object-store metadata while object-backed rows exist'
      USING ERRCODE = '55000';
  END IF;
END
$$;

DROP INDEX IF EXISTS public.media_artifact_object_lifecycle_idx;

ALTER TABLE public.media_artifact
  DROP CONSTRAINT IF EXISTS media_artifact_object_key_unique,
  DROP CONSTRAINT IF EXISTS media_artifact_storage_shape_check,
  DROP CONSTRAINT IF EXISTS media_artifact_storage_kind_check,
  DROP CONSTRAINT IF EXISTS media_artifact_content_size_check,
  ALTER COLUMN content SET NOT NULL,
  DROP COLUMN IF EXISTS content_size,
  DROP COLUMN IF EXISTS object_key,
  DROP COLUMN IF EXISTS storage_kind;

COMMIT;
