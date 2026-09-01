BEGIN;
DROP FUNCTION IF EXISTS public.cleanup_session_backend_migration(text,text,bigint,bigint,text,timestamptz);
DROP FUNCTION IF EXISTS public.session_backend_migration_drain_status(text,text);
DROP FUNCTION IF EXISTS public.rollback_session_backend_migration(text,text,bigint,bigint,text,timestamptz,text,text,text,text,text,text);
DROP FUNCTION IF EXISTS public.begin_session_backend_observation(text,text,bigint,bigint,timestamptz,timestamptz);
DROP FUNCTION IF EXISTS public.cutover_session_backend_migration(text,text,bigint,bigint,bigint,bigint,text,text,text,text,text,timestamptz,text,text,text,text,text,text);
DROP TRIGGER IF EXISTS backend_migration_config_switch_immutable ON public.backend_migration_config_switch;
DROP FUNCTION IF EXISTS public.reject_backend_migration_config_switch_change();
DROP TABLE IF EXISTS public.backend_migration_config_switch;
DROP TRIGGER IF EXISTS session_migration_mutation_direction_guard ON public.session_migration_mutation;
DROP FUNCTION IF EXISTS public.guard_session_migration_direction_update();
DROP INDEX IF EXISTS public.session_migration_mutation_direction_idx;
ALTER TABLE public.session_migration_mutation DROP COLUMN direction;
CREATE OR REPLACE FUNCTION public.capture_session_migration_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_migration public.backend_migration%ROWTYPE; v_config_version bigint;
BEGIN
  SELECT config_version INTO v_config_version FROM public.execution_record
   WHERE tenant_id=NEW.tenant_id AND request_id=NEW.request_id;
  SELECT * INTO v_migration FROM public.backend_migration
   WHERE tenant_id=NEW.tenant_id AND domain='session'
     AND state IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe')
     AND source_config_version=v_config_version ORDER BY epoch DESC LIMIT 1 FOR SHARE;
  IF NOT FOUND THEN RETURN NEW; END IF;
  INSERT INTO public.session_migration_mutation(tenant_id,migration_id,mutation_id,epoch,agent_app_id,session_id,
    source_version,mutation_digest,not_before,created_at,updated_at)
  VALUES(NEW.tenant_id,v_migration.migration_id,NEW.commit_id,v_migration.epoch,NEW.agent_app_id,NEW.session_id,
    NEW.session_version,NEW.request_digest,NEW.created_at,NEW.created_at,NEW.created_at)
  ON CONFLICT (tenant_id,migration_id,agent_app_id,session_id,mutation_id) DO NOTHING;
  RETURN NEW;
END;
$$;
COMMIT;
