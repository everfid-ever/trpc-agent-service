BEGIN;
DROP TRIGGER IF EXISTS backend_migration_knowledge_repair_gate ON public.backend_migration;
DROP FUNCTION IF EXISTS public.guard_knowledge_migration_authority_update();
DROP TRIGGER IF EXISTS knowledge_migration_mutation_guard ON public.knowledge_migration_mutation;
DROP FUNCTION IF EXISTS public.guard_knowledge_migration_mutation_update();
DROP FUNCTION IF EXISTS public.record_knowledge_migration_mutation(text,text,text,bigint,text,bigint,text,text,bigint,text,timestamptz);
DROP TABLE IF EXISTS public.knowledge_migration_mutation;
COMMIT;

