BEGIN;
DROP TRIGGER IF EXISTS session_commit_capture_migration ON public.session_commit;
DROP FUNCTION IF EXISTS public.capture_session_migration_mutation();
DROP TRIGGER IF EXISTS backend_migration_session_repair_gate ON public.backend_migration;
DROP FUNCTION IF EXISTS public.guard_session_migration_authority_update();
DROP TABLE IF EXISTS public.session_migration_mutation;
DROP FUNCTION IF EXISTS public.guard_session_migration_mutation_update();
COMMIT;
