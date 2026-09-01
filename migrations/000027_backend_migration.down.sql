BEGIN;
DROP TABLE IF EXISTS public.backend_migration_batch;
DROP TRIGGER IF EXISTS backend_migration_guard ON public.backend_migration;
DROP FUNCTION IF EXISTS public.reject_backend_migration_batch_change();
DROP FUNCTION IF EXISTS public.guard_backend_migration_update();
DROP TABLE IF EXISTS public.backend_migration;
ALTER TABLE public.backend_binding
  DROP CONSTRAINT IF EXISTS backend_binding_migration_coordinate_key;
COMMIT;
