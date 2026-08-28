BEGIN;

ALTER TABLE public.preprocess_job
  DROP CONSTRAINT IF EXISTS preprocess_job_prepared_ref_complete,
  DROP COLUMN IF EXISTS prepared_payload_ref;
DROP TABLE IF EXISTS public.prepared_payload;

COMMIT;
