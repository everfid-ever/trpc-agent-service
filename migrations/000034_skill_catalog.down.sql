BEGIN;

DROP TRIGGER IF EXISTS skill_catalog_guard ON skill_catalog;
DROP FUNCTION IF EXISTS public.guard_skill_catalog();
DROP TABLE IF EXISTS skill_catalog;

COMMIT;
