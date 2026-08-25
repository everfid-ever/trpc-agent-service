BEGIN;

DROP TRIGGER IF EXISTS backend_profile_revision_immutable ON public.backend_profile_revision;
DROP TRIGGER IF EXISTS model_profile_revision_immutable ON public.model_profile_revision;
DROP TRIGGER IF EXISTS backend_profile_identity_guard ON public.backend_profile;
DROP TRIGGER IF EXISTS model_profile_identity_guard ON public.model_profile;
DROP FUNCTION IF EXISTS public.guard_profile_revision_immutable();
DROP FUNCTION IF EXISTS public.guard_profile_identity();

ALTER TABLE public.backend_binding
  DROP CONSTRAINT IF EXISTS backend_binding_profile_version_fk,
  DROP COLUMN required,
  DROP COLUMN backend_version,
  ADD COLUMN backend_type text NOT NULL DEFAULT 'unconfigured',
  ADD COLUMN backend_ref text NOT NULL DEFAULT 'unconfigured',
  ADD COLUMN credential_ref text NOT NULL DEFAULT 'unconfigured',
  ADD COLUMN credential_version bigint NOT NULL DEFAULT 1 CHECK (credential_version >= 1),
  ADD COLUMN capabilities text[] NOT NULL DEFAULT '{}',
  ALTER COLUMN backend_profile_id DROP NOT NULL;

ALTER TABLE public.backend_profile DROP CONSTRAINT IF EXISTS backend_profile_current_version_fk;
DROP TABLE IF EXISTS public.backend_profile_revision;
ALTER TABLE public.backend_profile
  DROP CONSTRAINT IF EXISTS backend_profile_key_check,
  DROP CONSTRAINT IF EXISTS backend_profile_key_unique,
  DROP COLUMN current_version,
  DROP COLUMN profile_key;
ALTER TABLE public.backend_profile RENAME COLUMN row_version TO version;

ALTER TABLE public.model_profile DROP CONSTRAINT IF EXISTS model_profile_current_version_fk;
DROP TABLE IF EXISTS public.model_profile_revision;
DROP TABLE IF EXISTS public.model_profile;

COMMIT;
