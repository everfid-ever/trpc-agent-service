BEGIN;
DROP TRIGGER IF EXISTS execution_requires_preprocess ON public.execution_record;
DROP FUNCTION IF EXISTS public.reject_unpreprocessed_execution();
DROP TABLE IF EXISTS public.preprocess_job;
ALTER TABLE public.channel_binding_locator
  DROP CONSTRAINT IF EXISTS channel_binding_locator_session_secret_complete,
  DROP CONSTRAINT IF EXISTS channel_binding_locator_identity_secret_complete,
  DROP COLUMN IF EXISTS session_secret_version,
  DROP COLUMN IF EXISTS session_secret_ref,
  DROP COLUMN IF EXISTS identity_secret_version,
  DROP COLUMN IF EXISTS identity_secret_ref;
COMMIT;
