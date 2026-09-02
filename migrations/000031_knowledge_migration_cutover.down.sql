BEGIN;
DROP FUNCTION IF EXISTS public.cleanup_knowledge_backend_migration(text,text,bigint,bigint,text,timestamptz);
DROP FUNCTION IF EXISTS public.knowledge_backend_migration_drain_status(text,text);
DROP FUNCTION IF EXISTS public.rollback_knowledge_backend_migration(text,text,bigint,bigint,text,timestamptz,text,text,text,text,text,text);
DROP FUNCTION IF EXISTS public.begin_knowledge_backend_observation(text,text,bigint,bigint,timestamptz,timestamptz);
DROP FUNCTION IF EXISTS public.cutover_knowledge_backend_migration(text,text,bigint,bigint,bigint,bigint,text,text,text,text,text,timestamptz,text,text,text,text,text,text);
DROP FUNCTION IF EXISTS public.record_knowledge_migration_mutation(text,text,text,bigint,text,bigint,text,text,bigint,text,bigint,timestamptz);
DROP TRIGGER IF EXISTS knowledge_migration_mutation_direction_guard ON public.knowledge_migration_mutation;
DROP FUNCTION IF EXISTS public.guard_knowledge_migration_direction_update();
DROP INDEX IF EXISTS public.knowledge_migration_mutation_direction_idx;
ALTER TABLE public.knowledge_migration_mutation DROP COLUMN direction;
CREATE FUNCTION public.record_knowledge_migration_mutation(
  p_tenant_id text,p_migration_id text,p_mutation_id text,p_epoch bigint,
  p_knowledge_id text,p_knowledge_version bigint,p_chunk_id text,p_operation text,
  p_source_revision bigint,p_mutation_digest text,p_created_at timestamptz
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_migration public.backend_migration%ROWTYPE; v_existing public.knowledge_migration_mutation%ROWTYPE;
BEGIN
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_migration.domain<>'knowledge' OR v_migration.epoch<>p_epoch OR v_migration.state NOT IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe') OR p_created_at IS NULL OR p_created_at<v_migration.created_at THEN
    RAISE EXCEPTION 'knowledge migration authority conflict' USING ERRCODE='23514';
  END IF;
  INSERT INTO public.knowledge_migration_mutation(tenant_id,migration_id,mutation_id,epoch,knowledge_id,knowledge_version,chunk_id,operation,source_revision,mutation_digest,not_before,created_at,updated_at)
  VALUES(p_tenant_id,p_migration_id,p_mutation_id,p_epoch,p_knowledge_id,p_knowledge_version,p_chunk_id,p_operation,p_source_revision,p_mutation_digest,p_created_at,p_created_at,p_created_at)
  ON CONFLICT (tenant_id,migration_id,knowledge_id,knowledge_version,chunk_id,mutation_id) DO NOTHING;
  SELECT * INTO v_existing FROM public.knowledge_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND mutation_id=p_mutation_id;
  IF (v_existing.epoch,v_existing.operation,v_existing.source_revision,v_existing.mutation_digest,v_existing.created_at)
     IS DISTINCT FROM (p_epoch,p_operation,p_source_revision,p_mutation_digest,p_created_at) THEN RAISE EXCEPTION 'knowledge mutation id collision' USING ERRCODE='23505'; END IF;
END;
$$;
COMMIT;

