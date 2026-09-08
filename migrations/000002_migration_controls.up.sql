BEGIN;

ALTER TABLE public.backend_migration
  ADD COLUMN paused_from_state text NOT NULL DEFAULT '';

ALTER TABLE public.backend_migration
  DROP CONSTRAINT backend_migration_state_check,
  ADD CONSTRAINT backend_migration_state_check CHECK (state = ANY (ARRAY[
    'planned'::text, 'snapshot'::text, 'dual_write'::text, 'backfill'::text,
    'verify'::text, 'cutover'::text, 'observe'::text, 'cleanup'::text,
    'paused'::text, 'aborted'::text
  ])),
  ADD CONSTRAINT backend_migration_paused_from_state_check CHECK (
    (state='paused' AND paused_from_state = ANY (ARRAY['planned'::text, 'snapshot'::text, 'dual_write'::text, 'backfill'::text, 'verify'::text])) OR
    (state<>'paused' AND paused_from_state='')
  );

DROP INDEX public.backend_migration_active_domain_idx;
CREATE UNIQUE INDEX backend_migration_active_domain_idx ON public.backend_migration USING btree (tenant_id, domain)
  WHERE state NOT IN ('cleanup'::text, 'aborted'::text);

CREATE TABLE public.backend_migration_control (
  tenant_id text NOT NULL,
  migration_id text NOT NULL,
  control_version bigint NOT NULL,
  action text NOT NULL,
  from_state text NOT NULL,
  to_state text NOT NULL,
  actor_id text NOT NULL,
  reason_code text NOT NULL,
  correlation_id text NOT NULL,
  trace_id text NOT NULL,
  traceparent text NOT NULL DEFAULT '',
  occurred_at timestamp with time zone NOT NULL,
  PRIMARY KEY (tenant_id, migration_id, control_version),
  CONSTRAINT backend_migration_control_action_check CHECK (action = ANY (ARRAY['pause'::text, 'resume'::text, 'abort'::text])),
  CONSTRAINT backend_migration_control_actor_id_check CHECK (length(btrim(actor_id)) BETWEEN 1 AND 256),
  CONSTRAINT backend_migration_control_reason_code_check CHECK (length(btrim(reason_code)) BETWEEN 1 AND 128),
  CONSTRAINT backend_migration_control_correlation_id_check CHECK (length(btrim(correlation_id)) BETWEEN 1 AND 256),
  CONSTRAINT backend_migration_control_trace_id_check CHECK (length(btrim(trace_id)) BETWEEN 1 AND 128),
  CONSTRAINT backend_migration_control_traceparent_check CHECK (length(traceparent) <= 512),
  CONSTRAINT backend_migration_control_version_check CHECK (control_version >= 2),
  FOREIGN KEY (tenant_id, migration_id) REFERENCES public.backend_migration(tenant_id, migration_id)
);

CREATE FUNCTION public.reject_backend_migration_control_change() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path TO 'pg_catalog'
AS $$
BEGIN
  RAISE EXCEPTION 'backend migration controls are immutable' USING ERRCODE='23000';
END;
$$;

CREATE TRIGGER backend_migration_control_immutable
  BEFORE DELETE OR UPDATE ON public.backend_migration_control
  FOR EACH ROW EXECUTE FUNCTION public.reject_backend_migration_control_change();

CREATE OR REPLACE FUNCTION public.guard_backend_migration_update() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path TO 'pg_catalog', 'public'
AS $$
DECLARE
  expected_state text;
  pausable boolean;
  abortable boolean;
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
  pausable := OLD.state IN ('planned','snapshot','dual_write','backfill','verify');
  abortable := OLD.state IN ('planned','snapshot') OR
    (OLD.state='paused' AND OLD.paused_from_state IN ('planned','snapshot'));

  IF NEW.state = OLD.state THEN
    IF OLD.state='backfill' AND NOT OLD.backfill_complete AND
       NEW.next_batch_seq=OLD.next_batch_seq+1 AND NEW.backfill_count>=OLD.backfill_count AND
       NEW.backfill_checkpoint<>OLD.backfill_checkpoint AND NEW.paused_from_state=OLD.paused_from_state AND
       (NEW.snapshot_watermark,NEW.dual_write_ref,
        NEW.verify_source_count,NEW.verify_target_count,
        NEW.verify_source_digest,NEW.verify_target_digest,
        NEW.verify_source_watermark,NEW.verify_target_watermark,NEW.verify_sample_digest,
        NEW.cutover_config_version,NEW.cutover_at,NEW.observe_until,NEW.rollback_sync_watermark)
         IS NOT DISTINCT FROM
       (OLD.snapshot_watermark,OLD.dual_write_ref,
        OLD.verify_source_count,OLD.verify_target_count,
        OLD.verify_source_digest,OLD.verify_target_digest,
        OLD.verify_source_watermark,OLD.verify_target_watermark,OLD.verify_sample_digest,
        OLD.cutover_config_version,OLD.cutover_at,OLD.observe_until,OLD.rollback_sync_watermark) THEN
      RETURN NEW;
    ELSIF OLD.state='verify' AND NEW.paused_from_state='' AND
       (NEW.snapshot_watermark,NEW.dual_write_ref,NEW.backfill_checkpoint,NEW.next_batch_seq,NEW.backfill_count,NEW.backfill_complete,
        NEW.cutover_config_version,NEW.cutover_at,NEW.observe_until,NEW.rollback_sync_watermark)
         IS NOT DISTINCT FROM
       (OLD.snapshot_watermark,OLD.dual_write_ref,OLD.backfill_checkpoint,OLD.next_batch_seq,OLD.backfill_count,OLD.backfill_complete,
        OLD.cutover_config_version,OLD.cutover_at,OLD.observe_until,OLD.rollback_sync_watermark) THEN
      RETURN NEW;
    END IF;
    RAISE EXCEPTION 'backend migration update is invalid' USING ERRCODE='23514';
  END IF;

  IF (NEW.backfill_checkpoint,NEW.next_batch_seq,NEW.backfill_count,NEW.backfill_complete)
       IS DISTINCT FROM
     (OLD.backfill_checkpoint,OLD.next_batch_seq,OLD.backfill_count,OLD.backfill_complete) THEN
    RAISE EXCEPTION 'state transition cannot mutate backfill progress' USING ERRCODE='23514';
  END IF;

  IF NEW.state='paused' AND pausable AND NEW.paused_from_state=OLD.state THEN
    NULL;
  ELSIF OLD.state='paused' AND NEW.state=OLD.paused_from_state AND NEW.paused_from_state='' THEN
    NULL;
  ELSIF NEW.state='aborted' AND abortable AND NEW.paused_from_state='' THEN
    NULL;
  ELSIF NEW.state=expected_state AND OLD.paused_from_state='' AND NEW.paused_from_state='' THEN
    NULL;
  ELSE
    RAISE EXCEPTION 'illegal backend migration transition' USING ERRCODE='23514';
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
