BEGIN;

ALTER TABLE public.session_migration_mutation
  ADD COLUMN direction text NOT NULL DEFAULT 'forward'
    CHECK (direction IN ('forward','reverse'));

CREATE INDEX session_migration_mutation_direction_idx
  ON public.session_migration_mutation(tenant_id,migration_id,direction,state);

CREATE OR REPLACE FUNCTION public.guard_session_migration_direction_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF NEW.direction<>OLD.direction THEN
    RAISE EXCEPTION 'session migration mutation direction is immutable' USING ERRCODE='23000';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER session_migration_mutation_direction_guard
BEFORE UPDATE ON public.session_migration_mutation
FOR EACH ROW EXECUTE FUNCTION public.guard_session_migration_direction_update();

CREATE TABLE public.backend_migration_config_switch (
  tenant_id text NOT NULL,
  migration_id text NOT NULL,
  switch_id text NOT NULL,
  direction text NOT NULL CHECK (direction IN ('cutover','rollback')),
  previous_config_version bigint NOT NULL CHECK (previous_config_version >= 1),
  active_config_version bigint NOT NULL CHECK (active_config_version >= 1),
  migration_result_version bigint NOT NULL CHECK (migration_result_version >= 1),
  previous_tenant_version bigint NOT NULL CHECK (previous_tenant_version >= 1),
  tenant_result_version bigint NOT NULL CHECK (tenant_result_version = previous_tenant_version + 1),
  rollback_sync_watermark text NOT NULL DEFAULT '',
  actor_id text NOT NULL,
  reason_code text NOT NULL,
  correlation_id text NOT NULL,
  trace_id text NOT NULL,
  occurred_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id,migration_id,switch_id),
  UNIQUE (tenant_id,migration_id,direction),
  FOREIGN KEY (tenant_id,migration_id) REFERENCES public.backend_migration(tenant_id,migration_id),
  FOREIGN KEY (tenant_id,active_config_version) REFERENCES public.config_snapshot(tenant_id,config_version),
  CHECK (length(btrim(switch_id)) BETWEEN 1 AND 128),
  CHECK (length(btrim(actor_id)) BETWEEN 1 AND 256),
  CHECK (length(btrim(reason_code)) BETWEEN 1 AND 128),
  CHECK (length(btrim(correlation_id)) > 0 AND length(btrim(trace_id)) > 0),
  CHECK (length(rollback_sync_watermark) <= 512),
  CHECK ((direction='cutover' AND rollback_sync_watermark='') OR
         (direction='rollback' AND length(rollback_sync_watermark)>0))
);

CREATE OR REPLACE FUNCTION public.reject_backend_migration_config_switch_change()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'backend migration config switches are immutable' USING ERRCODE='23000';
END;
$$;

CREATE TRIGGER backend_migration_config_switch_immutable
BEFORE UPDATE OR DELETE ON public.backend_migration_config_switch
FOR EACH ROW EXECUTE FUNCTION public.reject_backend_migration_config_switch_change();

CREATE OR REPLACE FUNCTION public.capture_session_migration_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE
  v_migration public.backend_migration%ROWTYPE;
  v_config_version bigint;
  v_direction text;
BEGIN
  SELECT config_version INTO v_config_version
  FROM public.execution_record
  WHERE tenant_id=NEW.tenant_id AND request_id=NEW.request_id;
  SELECT * INTO v_migration
  FROM public.backend_migration
  WHERE tenant_id=NEW.tenant_id AND domain='session'
    AND state IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe')
    AND v_config_version IN (source_config_version,target_config_version)
  ORDER BY epoch DESC LIMIT 1 FOR SHARE;
  IF NOT FOUND THEN RETURN NEW; END IF;
  v_direction := CASE WHEN v_config_version=v_migration.target_config_version THEN 'reverse' ELSE 'forward' END;
  INSERT INTO public.session_migration_mutation(
    tenant_id,migration_id,mutation_id,epoch,direction,agent_app_id,session_id,
    source_version,mutation_digest,not_before,created_at,updated_at)
  VALUES(NEW.tenant_id,v_migration.migration_id,NEW.commit_id,v_migration.epoch,v_direction,
    NEW.agent_app_id,NEW.session_id,NEW.session_version,NEW.request_digest,
    NEW.created_at,NEW.created_at,NEW.created_at)
  ON CONFLICT (tenant_id,migration_id,agent_app_id,session_id,mutation_id) DO NOTHING;
  RETURN NEW;
END;
$$;

CREATE FUNCTION public.cutover_session_backend_migration(
  p_tenant_id text,p_migration_id text,p_expected_tenant_version bigint,p_expected_migration_version bigint,
  p_source_count bigint,p_target_count bigint,p_source_digest text,p_target_digest text,
  p_source_watermark text,p_target_watermark text,p_sample_digest text,p_at timestamptz,
  p_switch_id text,p_actor_id text,p_reason_code text,p_correlation_id text,p_trace_id text,p_traceparent text
) RETURNS TABLE(tenant_version bigint,active_config_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_existing public.backend_migration_config_switch%ROWTYPE;
BEGIN
  IF p_at IS NULL OR COALESCE(length(btrim(p_switch_id)),0)=0 OR length(p_switch_id)>128 OR
     COALESCE(length(btrim(p_actor_id)),0)=0 OR COALESCE(length(btrim(p_reason_code)),0)=0 OR
     COALESCE(length(btrim(p_correlation_id)),0)=0 OR COALESCE(length(btrim(p_trace_id)),0)=0 THEN
    RAISE EXCEPTION 'cutover metadata is incomplete' USING ERRCODE='22023';
  END IF;
  IF p_source_count IS NULL OR p_target_count IS NULL OR p_source_count<0 OR p_source_count<>p_target_count OR
     p_source_digest!~'^[0-9a-f]{64}$' OR p_source_digest<>p_target_digest OR
     COALESCE(length(p_source_watermark),0)=0 OR p_source_watermark<>p_target_watermark OR
     p_sample_digest!~'^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'verification evidence is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_existing FROM public.backend_migration_config_switch
   WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND switch_id=p_switch_id;
  IF FOUND THEN
    SELECT * INTO v_migration FROM public.backend_migration
      WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
    IF v_existing.direction<>'cutover' OR v_existing.previous_tenant_version<>p_expected_tenant_version OR
       v_existing.migration_result_version<>p_expected_migration_version+1 OR v_existing.actor_id<>p_actor_id OR
       v_existing.reason_code<>p_reason_code OR v_existing.correlation_id<>p_correlation_id OR
       v_existing.trace_id<>p_trace_id OR v_existing.occurred_at<>p_at OR
       v_migration.verify_source_count<>p_source_count OR v_migration.verify_target_count<>p_target_count OR
       v_migration.verify_source_digest<>p_source_digest OR v_migration.verify_target_digest<>p_target_digest OR
       v_migration.verify_source_watermark<>p_source_watermark OR v_migration.verify_target_watermark<>p_target_watermark OR
       v_migration.verify_sample_digest<>p_sample_digest THEN
      RAISE EXCEPTION 'switch id collision' USING ERRCODE='23505';
    END IF;
    RETURN QUERY SELECT v_existing.tenant_result_version,v_existing.active_config_version; RETURN;
  END IF;
  IF v_tenant.status='disabled' THEN RAISE EXCEPTION 'disabled tenant is immutable' USING ERRCODE='55000'; END IF;
  IF v_tenant.version<>p_expected_tenant_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE='40001'; END IF;
  SELECT * INTO v_migration FROM public.backend_migration
   WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_migration.domain<>'session' OR v_migration.state<>'verify' OR
     v_migration.version<>p_expected_migration_version OR
     v_tenant.active_config_version<>v_migration.source_config_version THEN
    RAISE EXCEPTION 'cutover authority conflict' USING ERRCODE='40001';
  END IF;
  IF EXISTS (SELECT 1 FROM public.session_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND state<>'applied') THEN
    RAISE EXCEPTION 'session migration repair backlog is not drained' USING ERRCODE='23514';
  END IF;
  UPDATE public.backend_migration SET state='cutover',verify_source_count=p_source_count,
    verify_target_count=p_target_count,verify_source_digest=p_source_digest,verify_target_digest=p_target_digest,
    verify_source_watermark=p_source_watermark,verify_target_watermark=p_target_watermark,
    verify_sample_digest=p_sample_digest,cutover_config_version=target_config_version,cutover_at=p_at,
    updated_at=p_at,version=version+1
  WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  UPDATE public.tenant SET active_config_version=v_migration.target_config_version,version=version+1
   WHERE tenant_id=p_tenant_id;
  INSERT INTO public.backend_migration_config_switch(tenant_id,migration_id,switch_id,direction,
    previous_config_version,active_config_version,migration_result_version,previous_tenant_version,tenant_result_version,
    actor_id,reason_code,correlation_id,trace_id,occurred_at)
  VALUES(p_tenant_id,p_migration_id,p_switch_id,'cutover',v_migration.source_config_version,v_migration.target_config_version,
    v_migration.version+1,v_tenant.version,v_tenant.version+1,p_actor_id,p_reason_code,p_correlation_id,p_trace_id,p_at);
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent) VALUES
    (p_tenant_id,format('migration-cutover-audit:%s:%s',p_migration_id,p_switch_id),'audit',p_migration_id,v_tenant.version+1,
      format('migration:%s:%s:cutover-audit',p_migration_id,p_switch_id),format('backend-migration-switch://%s/%s/%s',p_tenant_id,p_migration_id,p_switch_id),p_traceparent),
    (p_tenant_id,format('migration-cutover-invalidation:%s:%s',p_migration_id,p_switch_id),'config-invalidation',p_tenant_id,v_tenant.version+1,
      format('migration:%s:%s:cutover-invalidate',p_migration_id,p_switch_id),format('config://%s/%s',p_tenant_id,v_migration.target_config_version),p_traceparent);
  RETURN QUERY SELECT v_tenant.version+1,v_migration.target_config_version;
END;
$$;

CREATE FUNCTION public.begin_session_backend_observation(
  p_tenant_id text,p_migration_id text,p_expected_tenant_version bigint,p_expected_migration_version bigint,
  p_at timestamptz,p_observe_until timestamptz
) RETURNS TABLE(tenant_version bigint,active_config_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE;
BEGIN
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_tenant.version<>p_expected_tenant_version OR v_migration.version<>p_expected_migration_version OR
     v_migration.domain<>'session' OR v_migration.state<>'cutover' OR v_tenant.active_config_version<>v_migration.target_config_version THEN
    RAISE EXCEPTION 'observation authority conflict' USING ERRCODE='40001';
  END IF;
  IF p_at IS NULL OR p_observe_until IS NULL OR p_observe_until<=p_at THEN
    RAISE EXCEPTION 'observation window is invalid' USING ERRCODE='22023';
  END IF;
  UPDATE public.backend_migration SET state='observe',observe_until=p_observe_until,updated_at=p_at,version=version+1
   WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  RETURN QUERY SELECT v_tenant.version,v_tenant.active_config_version;
END;
$$;

CREATE FUNCTION public.rollback_session_backend_migration(
  p_tenant_id text,p_migration_id text,p_expected_tenant_version bigint,p_expected_migration_version bigint,
  p_rollback_sync_watermark text,p_at timestamptz,p_switch_id text,p_actor_id text,p_reason_code text,
  p_correlation_id text,p_trace_id text,p_traceparent text
) RETURNS TABLE(tenant_version bigint,active_config_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_existing public.backend_migration_config_switch%ROWTYPE;
BEGIN
  IF p_at IS NULL OR COALESCE(length(btrim(p_switch_id)),0)=0 OR length(p_switch_id)>128 OR
     COALESCE(length(btrim(p_actor_id)),0)=0 OR COALESCE(length(btrim(p_reason_code)),0)=0 OR
     COALESCE(length(btrim(p_correlation_id)),0)=0 OR COALESCE(length(btrim(p_trace_id)),0)=0 OR
     COALESCE(length(p_rollback_sync_watermark),0)=0 THEN
    RAISE EXCEPTION 'rollback metadata is incomplete' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_existing FROM public.backend_migration_config_switch
   WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND switch_id=p_switch_id;
  IF FOUND THEN
    IF v_existing.direction<>'rollback' OR v_existing.previous_tenant_version<>p_expected_tenant_version OR
       v_existing.migration_result_version<>p_expected_migration_version OR
       v_existing.rollback_sync_watermark<>p_rollback_sync_watermark OR v_existing.actor_id<>p_actor_id OR
       v_existing.reason_code<>p_reason_code OR v_existing.correlation_id<>p_correlation_id OR
       v_existing.trace_id<>p_trace_id OR v_existing.occurred_at<>p_at THEN
      RAISE EXCEPTION 'switch id collision' USING ERRCODE='23505';
    END IF;
    RETURN QUERY SELECT v_existing.tenant_result_version,v_existing.active_config_version; RETURN;
  END IF;
  IF v_tenant.status='disabled' THEN RAISE EXCEPTION 'disabled tenant is immutable' USING ERRCODE='55000'; END IF;
  IF v_tenant.version<>p_expected_tenant_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE='40001'; END IF;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_migration.domain<>'session' OR v_migration.state<>'observe' OR v_migration.version<>p_expected_migration_version OR
     p_at<v_migration.updated_at OR
     v_tenant.active_config_version<>v_migration.target_config_version THEN
    RAISE EXCEPTION 'rollback authority conflict' USING ERRCODE='40001';
  END IF;
  IF p_rollback_sync_watermark<>v_migration.verify_target_watermark OR EXISTS (
    SELECT 1 FROM public.session_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND direction='reverse' AND state<>'applied') THEN
    RAISE EXCEPTION 'reverse synchronization is incomplete' USING ERRCODE='23514';
  END IF;
  UPDATE public.tenant SET active_config_version=v_migration.source_config_version,version=version+1 WHERE tenant_id=p_tenant_id;
  INSERT INTO public.backend_migration_config_switch(tenant_id,migration_id,switch_id,direction,
    previous_config_version,active_config_version,migration_result_version,previous_tenant_version,tenant_result_version,
    rollback_sync_watermark,actor_id,reason_code,correlation_id,trace_id,occurred_at)
  VALUES(p_tenant_id,p_migration_id,p_switch_id,'rollback',v_migration.target_config_version,v_migration.source_config_version,
    v_migration.version,v_tenant.version,v_tenant.version+1,p_rollback_sync_watermark,p_actor_id,p_reason_code,p_correlation_id,p_trace_id,p_at);
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent) VALUES
    (p_tenant_id,format('migration-rollback-audit:%s:%s',p_migration_id,p_switch_id),'audit',p_migration_id,v_tenant.version+1,
      format('migration:%s:%s:rollback-audit',p_migration_id,p_switch_id),format('backend-migration-switch://%s/%s/%s',p_tenant_id,p_migration_id,p_switch_id),p_traceparent),
    (p_tenant_id,format('migration-rollback-invalidation:%s:%s',p_migration_id,p_switch_id),'config-invalidation',p_tenant_id,v_tenant.version+1,
      format('migration:%s:%s:rollback-invalidate',p_migration_id,p_switch_id),format('config://%s/%s',p_tenant_id,v_migration.source_config_version),p_traceparent);
  RETURN QUERY SELECT v_tenant.version+1,v_migration.source_config_version;
END;
$$;

CREATE FUNCTION public.session_backend_migration_drain_status(p_tenant_id text,p_migration_id text)
RETURNS TABLE(source_in_flight bigint,target_in_flight bigint,forward_outstanding bigint,
  reverse_outstanding bigint,active_config_version bigint,rolled_back boolean)
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
  SELECT
    (SELECT count(*) FROM public.execution_record e WHERE e.tenant_id=m.tenant_id AND e.config_version=m.source_config_version
      AND e.outcome IN ('queued','running','pending','blocked','waiting_confirmation')),
    (SELECT count(*) FROM public.execution_record e WHERE e.tenant_id=m.tenant_id AND e.config_version=m.target_config_version
      AND e.outcome IN ('queued','running','pending','blocked','waiting_confirmation')),
    (SELECT count(*) FROM public.session_migration_mutation x WHERE x.tenant_id=m.tenant_id AND x.migration_id=m.migration_id
      AND x.direction='forward' AND x.state<>'applied'),
    (SELECT count(*) FROM public.session_migration_mutation x WHERE x.tenant_id=m.tenant_id AND x.migration_id=m.migration_id
      AND x.direction='reverse' AND x.state<>'applied'),
    t.active_config_version,
    EXISTS(SELECT 1 FROM public.backend_migration_config_switch s WHERE s.tenant_id=m.tenant_id
      AND s.migration_id=m.migration_id AND s.direction='rollback')
  FROM public.backend_migration m JOIN public.tenant t ON t.tenant_id=m.tenant_id
  WHERE m.tenant_id=p_tenant_id AND m.migration_id=p_migration_id;
$$;

CREATE FUNCTION public.cleanup_session_backend_migration(
  p_tenant_id text,p_migration_id text,p_expected_tenant_version bigint,p_expected_migration_version bigint,
  p_rollback_sync_watermark text,p_at timestamptz
) RETURNS TABLE(tenant_version bigint,active_config_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_rolled_back boolean; v_old_config bigint;
BEGIN
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  SELECT EXISTS(SELECT 1 FROM public.backend_migration_config_switch WHERE tenant_id=p_tenant_id
    AND migration_id=p_migration_id AND direction='rollback') INTO v_rolled_back;
  IF v_tenant.version<>p_expected_tenant_version OR v_migration.version<>p_expected_migration_version OR
     v_migration.domain<>'session' OR v_migration.state<>'observe' OR p_at IS NULL OR p_at<v_migration.observe_until OR
     p_rollback_sync_watermark<>v_migration.verify_target_watermark THEN
    RAISE EXCEPTION 'cleanup authority conflict' USING ERRCODE='40001';
  END IF;
  IF (v_rolled_back AND v_tenant.active_config_version<>v_migration.source_config_version) OR
     (NOT v_rolled_back AND v_tenant.active_config_version<>v_migration.target_config_version) THEN
    RAISE EXCEPTION 'active config does not match migration direction' USING ERRCODE='23514';
  END IF;
  v_old_config := CASE WHEN v_rolled_back THEN v_migration.target_config_version ELSE v_migration.source_config_version END;
  IF EXISTS (SELECT 1 FROM public.session_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND state<>'applied') OR
     EXISTS (SELECT 1 FROM public.execution_record WHERE tenant_id=p_tenant_id AND config_version=v_old_config
       AND outcome IN ('queued','running','pending','blocked','waiting_confirmation')) THEN
    RAISE EXCEPTION 'migration cleanup drain is incomplete' USING ERRCODE='23514';
  END IF;
  UPDATE public.backend_migration SET state='cleanup',rollback_sync_watermark=p_rollback_sync_watermark,
    updated_at=p_at,version=version+1 WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  RETURN QUERY SELECT v_tenant.version,v_tenant.active_config_version;
END;
$$;

REVOKE ALL ON FUNCTION public.reject_backend_migration_config_switch_change() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.guard_session_migration_direction_update() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.cutover_session_backend_migration(text,text,bigint,bigint,bigint,bigint,text,text,text,text,text,timestamptz,text,text,text,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.begin_session_backend_observation(text,text,bigint,bigint,timestamptz,timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.rollback_session_backend_migration(text,text,bigint,bigint,text,timestamptz,text,text,text,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.cleanup_session_backend_migration(text,text,bigint,bigint,text,timestamptz) FROM PUBLIC;

COMMIT;
