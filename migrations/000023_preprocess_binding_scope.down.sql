BEGIN;

ALTER TABLE public.preprocess_job
  DROP CONSTRAINT IF EXISTS preprocess_job_channel_binding_fk,
  DROP CONSTRAINT IF EXISTS preprocess_job_config_version_check,
  DROP COLUMN IF EXISTS config_version,
  DROP COLUMN IF EXISTS channel_binding_id;

COMMIT;
