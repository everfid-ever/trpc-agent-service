BEGIN;

ALTER TABLE public.backend_binding
  ADD CONSTRAINT backend_binding_migration_coordinate_key
  UNIQUE (tenant_id,config_version,domain,backend_profile_id,backend_version);

CREATE TABLE public.backend_migration (
  tenant_id text NOT NULL,
  migration_id text NOT NULL,
  domain text NOT NULL,
  epoch bigint NOT NULL CHECK (epoch >= 1),
  source_config_version bigint NOT NULL CHECK (source_config_version >= 1),
  source_backend_profile_id text NOT NULL,
  source_backend_version bigint NOT NULL CHECK (source_backend_version >= 1),
  target_config_version bigint NOT NULL CHECK (target_config_version >= 1),
  target_backend_profile_id text NOT NULL,
  target_backend_version bigint NOT NULL CHECK (target_backend_version >= 1),
  state text NOT NULL CHECK (state IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe','cleanup')),
  snapshot_watermark text NOT NULL DEFAULT '',
  dual_write_ref text NOT NULL DEFAULT '',
  backfill_checkpoint text NOT NULL DEFAULT '',
  next_batch_seq bigint NOT NULL DEFAULT 1 CHECK (next_batch_seq >= 1),
  backfill_count bigint NOT NULL DEFAULT 0 CHECK (backfill_count >= 0),
  backfill_complete boolean NOT NULL DEFAULT false,
  verify_source_count bigint,
  verify_target_count bigint,
  verify_source_digest text NOT NULL DEFAULT '',
  verify_target_digest text NOT NULL DEFAULT '',
  verify_source_watermark text NOT NULL DEFAULT '',
  verify_target_watermark text NOT NULL DEFAULT '',
  verify_sample_digest text NOT NULL DEFAULT '',
  cutover_config_version bigint,
  cutover_at timestamptz,
  observe_until timestamptz,
  rollback_sync_watermark text NOT NULL DEFAULT '',
  version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id,migration_id),
  UNIQUE (tenant_id,domain,epoch),
  FOREIGN KEY (tenant_id,source_config_version,domain,source_backend_profile_id,source_backend_version)
    REFERENCES public.backend_binding(tenant_id,config_version,domain,backend_profile_id,backend_version),
  FOREIGN KEY (tenant_id,target_config_version,domain,target_backend_profile_id,target_backend_version)
    REFERENCES public.backend_binding(tenant_id,config_version,domain,backend_profile_id,backend_version),
  FOREIGN KEY (tenant_id,cutover_config_version)
    REFERENCES public.config_snapshot(tenant_id,config_version),
  CHECK (length(btrim(migration_id)) > 0 AND length(migration_id) <= 128),
  CHECK (length(btrim(domain)) > 0 AND length(domain) <= 32),
  CHECK ((source_config_version,source_backend_profile_id,source_backend_version) <>
         (target_config_version,target_backend_profile_id,target_backend_version)),
  CHECK (length(snapshot_watermark) <= 512 AND length(dual_write_ref) <= 512 AND
         length(backfill_checkpoint) <= 512 AND length(verify_source_watermark) <= 512 AND
         length(verify_target_watermark) <= 512 AND length(rollback_sync_watermark) <= 512),
  CHECK ((verify_source_count IS NULL OR verify_source_count >= 0) AND
         (verify_target_count IS NULL OR verify_target_count >= 0)),
  CHECK ((verify_source_digest = '' OR verify_source_digest ~ '^[0-9a-f]{64}$') AND
         (verify_target_digest = '' OR verify_target_digest ~ '^[0-9a-f]{64}$') AND
         (verify_sample_digest = '' OR verify_sample_digest ~ '^[0-9a-f]{64}$')),
  CHECK (updated_at >= created_at)
);

CREATE UNIQUE INDEX backend_migration_active_domain_idx
  ON public.backend_migration(tenant_id,domain)
  WHERE state <> 'cleanup';

CREATE TABLE public.backend_migration_batch (
  tenant_id text NOT NULL,
  migration_id text NOT NULL,
  batch_seq bigint NOT NULL CHECK (batch_seq >= 1),
  batch_id text NOT NULL,
  epoch bigint NOT NULL CHECK (epoch >= 1),
  from_checkpoint text NOT NULL,
  to_checkpoint text NOT NULL,
  record_count bigint NOT NULL CHECK (record_count >= 0),
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  complete boolean NOT NULL DEFAULT false,
  result_version bigint NOT NULL CHECK (result_version >= 2),
  committed_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id,migration_id,batch_seq),
  UNIQUE (tenant_id,migration_id,batch_id),
  FOREIGN KEY (tenant_id,migration_id)
    REFERENCES public.backend_migration(tenant_id,migration_id),
  CHECK (length(btrim(batch_id)) > 0 AND length(batch_id) <= 128),
  CHECK (length(to_checkpoint) > 0 AND length(to_checkpoint) <= 512),
  CHECK (to_checkpoint <> from_checkpoint),
  CHECK (record_count > 0 OR complete)
);

CREATE OR REPLACE FUNCTION public.guard_backend_migration_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
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

CREATE TRIGGER backend_migration_guard
BEFORE UPDATE ON public.backend_migration
FOR EACH ROW EXECUTE FUNCTION public.guard_backend_migration_update();

CREATE OR REPLACE FUNCTION public.reject_backend_migration_batch_change()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
  RAISE EXCEPTION 'backend migration batches are immutable' USING ERRCODE='23000';
END;
$$;

CREATE TRIGGER backend_migration_batch_immutable
BEFORE UPDATE OR DELETE ON public.backend_migration_batch
FOR EACH ROW EXECUTE FUNCTION public.reject_backend_migration_batch_change();

REVOKE ALL ON FUNCTION public.guard_backend_migration_update() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.reject_backend_migration_batch_change() FROM PUBLIC;

COMMIT;
