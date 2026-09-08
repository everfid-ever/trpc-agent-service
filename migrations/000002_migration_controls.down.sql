BEGIN;

DROP TRIGGER backend_migration_control_immutable ON public.backend_migration_control;
DROP FUNCTION public.reject_backend_migration_control_change();
DROP TABLE public.backend_migration_control;

-- Down is supported only for disposable local databases. A paused migration
-- can safely resume its recorded phase; an aborted one is pre-dual-write by
-- construction and has no data-plane state worth preserving.
UPDATE public.backend_migration
  SET state=paused_from_state, paused_from_state='', version=version+1,
      updated_at=GREATEST(updated_at, clock_timestamp())
  WHERE state='paused';
DELETE FROM public.backend_migration WHERE state='aborted';

DROP INDEX public.backend_migration_active_domain_idx;
CREATE UNIQUE INDEX backend_migration_active_domain_idx ON public.backend_migration USING btree (tenant_id, domain)
  WHERE state <> 'cleanup'::text;

ALTER TABLE public.backend_migration
  DROP CONSTRAINT backend_migration_paused_from_state_check,
  DROP CONSTRAINT backend_migration_state_check,
  ADD CONSTRAINT backend_migration_state_check CHECK (state = ANY (ARRAY[
    'planned'::text, 'snapshot'::text, 'dual_write'::text, 'backfill'::text,
    'verify'::text, 'cutover'::text, 'observe'::text, 'cleanup'::text
  ])),
  DROP COLUMN paused_from_state;

CREATE OR REPLACE FUNCTION public.guard_backend_migration_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
AS $$
DECLARE
  expected_state text;
BEGIN
  IF (NEW.tenant_id,NEW.migration_id,NEW.domain,NEW.epoch,
      NEW.source_config_version,NEW.source_backend_profile_id,NEW.source_backend_version,
      NEW.target_config_version,NEW.target_backend_profile_id,NEW.target_backend_version,NEW.created_at)
     IS DISTINCT FROM
     (OLD.tenant_id,OLD.migration_id,OLD.domain,OLD.epoch,
      OLD.source_config_version,OLD.source_backend_profile_id,OLD.source_backend_version,
      OLD.target_config_version,OLD.target_backend_profile_id,OLD.target_backend_version,OLD.created_at) THEN
    RAISE EXCEPTION 'backend migration identity is immutable' USING ERRCODE='23000';
  END IF;
  IF NEW.version <> OLD.version + 1 OR NEW.updated_at < OLD.updated_at THEN
    RAISE EXCEPTION 'backend migration version must advance exactly once' USING ERRCODE='40001';
  END IF;
  expected_state := CASE OLD.state
    WHEN 'planned' THEN 'snapshot' WHEN 'snapshot' THEN 'dual_write'
    WHEN 'dual_write' THEN 'backfill' WHEN 'backfill' THEN 'verify'
    WHEN 'verify' THEN 'cutover' WHEN 'cutover' THEN 'observe'
    WHEN 'observe' THEN 'cleanup' ELSE NULL END;
  IF NEW.state <> OLD.state AND NEW.state IS DISTINCT FROM expected_state THEN
    RAISE EXCEPTION 'illegal backend migration transition' USING ERRCODE='23514';
  END IF;
  IF NEW.state = OLD.state AND (
    OLD.state <> 'backfill' OR OLD.backfill_complete OR
    NEW.next_batch_seq <> OLD.next_batch_seq + 1 OR
    NEW.backfill_count < OLD.backfill_count OR
    NEW.backfill_checkpoint = OLD.backfill_checkpoint OR
    (NEW.snapshot_watermark,NEW.dual_write_ref,
     NEW.verify_source_count,NEW.verify_target_count,
     NEW.verify_source_digest,NEW.verify_target_digest,
     NEW.verify_source_watermark,NEW.verify_target_watermark,NEW.verify_sample_digest,
     NEW.cutover_config_version,NEW.cutover_at,NEW.observe_until,NEW.rollback_sync_watermark)
      IS DISTINCT FROM
    (OLD.snapshot_watermark,OLD.dual_write_ref,
     OLD.verify_source_count,OLD.verify_target_count,
     OLD.verify_source_digest,OLD.verify_target_digest,
     OLD.verify_source_watermark,OLD.verify_target_watermark,OLD.verify_sample_digest,
     OLD.cutover_config_version,OLD.cutover_at,OLD.observe_until,OLD.rollback_sync_watermark)
  ) THEN
    RAISE EXCEPTION 'backfill checkpoint update is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state <> OLD.state AND
     (NEW.backfill_checkpoint,NEW.next_batch_seq,NEW.backfill_count,NEW.backfill_complete)
       IS DISTINCT FROM
     (OLD.backfill_checkpoint,OLD.next_batch_seq,OLD.backfill_count,OLD.backfill_complete) THEN
    RAISE EXCEPTION 'state transition cannot mutate backfill progress' USING ERRCODE='23514';
  END IF;
  IF NEW.state='snapshot' AND length(NEW.snapshot_watermark)=0 THEN
    RAISE EXCEPTION 'snapshot watermark is required' USING ERRCODE='23514';
  END IF;
  IF NEW.state='dual_write' AND length(NEW.dual_write_ref)=0 THEN
    RAISE EXCEPTION 'dual write authority is required' USING ERRCODE='23514';
  END IF;
  IF NEW.state='verify' AND NOT NEW.backfill_complete THEN
    RAISE EXCEPTION 'backfill must complete before verification' USING ERRCODE='23514';
  END IF;
  IF NEW.state IN ('cutover','observe','cleanup') AND (
    NEW.verify_source_count IS NULL OR NEW.verify_target_count IS NULL OR
    NEW.verify_source_count<>NEW.verify_target_count OR
    NEW.verify_source_digest !~ '^[0-9a-f]{64}$' OR NEW.verify_target_digest<>NEW.verify_source_digest OR
    length(NEW.verify_source_watermark)=0 OR NEW.verify_target_watermark<>NEW.verify_source_watermark OR
    NEW.verify_sample_digest !~ '^[0-9a-f]{64}$' OR
    NEW.cutover_config_version IS NULL OR NEW.cutover_config_version<>NEW.target_config_version OR
    NEW.cutover_at IS NULL) THEN
    RAISE EXCEPTION 'verification evidence is incomplete' USING ERRCODE='23514';
  END IF;
  IF NEW.state IN ('observe','cleanup') AND (NEW.observe_until IS NULL OR NEW.observe_until <= NEW.cutover_at) THEN
    RAISE EXCEPTION 'observation window is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='cleanup' AND (NEW.updated_at < NEW.observe_until OR NEW.rollback_sync_watermark <> NEW.verify_target_watermark) THEN
    RAISE EXCEPTION 'rollback sync is incomplete' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

COMMIT;
