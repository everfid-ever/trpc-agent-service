BEGIN;

DROP TABLE IF EXISTS public.backend_migration_phase_intent;
DROP FUNCTION IF EXISTS public.guard_backend_migration_phase_intent_update();

COMMIT;
