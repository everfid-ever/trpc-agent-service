BEGIN;

-- 000030 and 000031 are an atomic, unreleased Knowledge migration pair.  The
-- old recorder signature has no ConfigVersion fence and would remain a write
-- bypass after cutover, so it must be replaced rather than retained as an
-- overload.  Do not ship 000030 independently.

ALTER TABLE public.knowledge_migration_mutation
  ADD COLUMN direction text NOT NULL DEFAULT 'forward'
    CHECK (direction IN ('forward','reverse'));

CREATE INDEX knowledge_migration_mutation_direction_idx
  ON public.knowledge_migration_mutation(tenant_id,migration_id,direction,state);

CREATE FUNCTION public.guard_knowledge_migration_direction_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF NEW.direction<>OLD.direction THEN
    RAISE EXCEPTION 'knowledge migration mutation direction is immutable' USING ERRCODE='23000';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_migration_mutation_direction_guard
BEFORE UPDATE ON public.knowledge_migration_mutation
FOR EACH ROW EXECUTE FUNCTION public.guard_knowledge_migration_direction_update();

DROP FUNCTION public.record_knowledge_migration_mutation(text,text,text,bigint,text,bigint,text,text,bigint,text,timestamptz);
CREATE FUNCTION public.record_knowledge_migration_mutation(
  p_tenant_id text,p_migration_id text,p_mutation_id text,p_epoch bigint,
  p_knowledge_id text,p_knowledge_version bigint,p_chunk_id text,p_operation text,
  p_source_revision bigint,p_mutation_digest text,p_config_version bigint,p_created_at timestamptz
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_migration public.backend_migration%ROWTYPE; v_existing public.knowledge_migration_mutation%ROWTYPE; v_direction text;
BEGIN
  SELECT * INTO v_migration FROM public.backend_migration
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_migration.domain<>'knowledge' OR v_migration.epoch<>p_epoch OR
     (v_migration.state IN ('planned','snapshot','dual_write','backfill','verify') AND p_config_version<>v_migration.source_config_version) OR
     (v_migration.state IN ('cutover','observe') AND p_config_version NOT IN (v_migration.source_config_version,v_migration.target_config_version)) OR
     v_migration.state NOT IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe') OR
     p_created_at IS NULL OR p_created_at<v_migration.created_at THEN
    RAISE EXCEPTION 'knowledge migration authority conflict' USING ERRCODE='23514';
  END IF;
  v_direction := CASE WHEN p_config_version=v_migration.target_config_version THEN 'reverse' ELSE 'forward' END;
  INSERT INTO public.knowledge_migration_mutation(tenant_id,migration_id,mutation_id,epoch,direction,
    knowledge_id,knowledge_version,chunk_id,operation,source_revision,mutation_digest,
    not_before,created_at,updated_at)
  VALUES(p_tenant_id,p_migration_id,p_mutation_id,p_epoch,v_direction,p_knowledge_id,p_knowledge_version,
    p_chunk_id,p_operation,p_source_revision,p_mutation_digest,p_created_at,p_created_at,p_created_at)
  ON CONFLICT (tenant_id,migration_id,knowledge_id,knowledge_version,chunk_id,mutation_id) DO NOTHING;
  SELECT * INTO v_existing FROM public.knowledge_migration_mutation
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND mutation_id=p_mutation_id;
  IF (v_existing.epoch,v_existing.direction,v_existing.operation,v_existing.source_revision,
      v_existing.mutation_digest,v_existing.created_at)
     IS DISTINCT FROM (p_epoch,v_direction,p_operation,p_source_revision,p_mutation_digest,p_created_at) THEN
    RAISE EXCEPTION 'knowledge mutation id collision' USING ERRCODE='23505';
  END IF;
END;
$$;

CREATE FUNCTION public.cutover_knowledge_backend_migration(
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
     COALESCE(length(btrim(p_correlation_id)),0)=0 OR COALESCE(length(btrim(p_trace_id)),0)=0 OR
     p_source_count IS NULL OR p_target_count IS NULL OR p_source_count<0 OR p_source_count<>p_target_count OR
     p_source_digest!~'^[0-9a-f]{64}$' OR p_source_digest<>p_target_digest OR
     COALESCE(length(p_source_watermark),0)=0 OR p_source_watermark<>p_target_watermark OR p_sample_digest!~'^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'knowledge cutover input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_existing FROM public.backend_migration_config_switch
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND switch_id=p_switch_id;
  IF FOUND THEN
    SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
    IF v_existing.direction<>'cutover' OR v_existing.previous_tenant_version<>p_expected_tenant_version OR
       v_existing.migration_result_version<>p_expected_migration_version+1 OR v_existing.actor_id<>p_actor_id OR
       v_existing.reason_code<>p_reason_code OR v_existing.correlation_id<>p_correlation_id OR v_existing.trace_id<>p_trace_id OR
       v_existing.occurred_at<>p_at OR v_migration.verify_source_count<>p_source_count OR v_migration.verify_target_count<>p_target_count OR
       v_migration.verify_source_digest<>p_source_digest OR v_migration.verify_target_digest<>p_target_digest OR
       v_migration.verify_source_watermark<>p_source_watermark OR v_migration.verify_target_watermark<>p_target_watermark OR
       v_migration.verify_sample_digest<>p_sample_digest THEN
      RAISE EXCEPTION 'switch id collision' USING ERRCODE='23505';
    END IF;
    RETURN QUERY SELECT v_existing.tenant_result_version,v_existing.active_config_version; RETURN;
  END IF;
  IF v_tenant.status='disabled' OR v_tenant.version<>p_expected_tenant_version THEN
    RAISE EXCEPTION 'knowledge tenant authority conflict' USING ERRCODE='40001';
  END IF;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_migration.domain<>'knowledge' OR v_migration.state<>'verify' OR v_migration.version<>p_expected_migration_version OR
     v_tenant.active_config_version<>v_migration.source_config_version OR EXISTS (
       SELECT 1 FROM public.knowledge_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND state<>'applied') THEN
    RAISE EXCEPTION 'knowledge cutover authority conflict' USING ERRCODE='23514';
  END IF;
  UPDATE public.backend_migration SET state='cutover',verify_source_count=p_source_count,verify_target_count=p_target_count,
    verify_source_digest=p_source_digest,verify_target_digest=p_target_digest,verify_source_watermark=p_source_watermark,
    verify_target_watermark=p_target_watermark,verify_sample_digest=p_sample_digest,cutover_config_version=target_config_version,
    cutover_at=p_at,updated_at=p_at,version=version+1 WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  UPDATE public.tenant SET active_config_version=v_migration.target_config_version,version=version+1 WHERE tenant_id=p_tenant_id;
  INSERT INTO public.backend_migration_config_switch(tenant_id,migration_id,switch_id,direction,previous_config_version,
    active_config_version,migration_result_version,previous_tenant_version,tenant_result_version,actor_id,reason_code,
    correlation_id,trace_id,occurred_at)
  VALUES(p_tenant_id,p_migration_id,p_switch_id,'cutover',v_migration.source_config_version,v_migration.target_config_version,
    v_migration.version+1,v_tenant.version,v_tenant.version+1,p_actor_id,p_reason_code,p_correlation_id,p_trace_id,p_at);
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent) VALUES
    (p_tenant_id,format('knowledge-migration-cutover-audit:%s:%s',p_migration_id,p_switch_id),'audit',p_migration_id,v_tenant.version+1,
      format('knowledge-migration:%s:%s:cutover-audit',p_migration_id,p_switch_id),format('backend-migration-switch://%s/%s/%s',p_tenant_id,p_migration_id,p_switch_id),p_traceparent),
    (p_tenant_id,format('knowledge-migration-cutover-invalidation:%s:%s',p_migration_id,p_switch_id),'config-invalidation',p_tenant_id,v_tenant.version+1,
      format('knowledge-migration:%s:%s:cutover-invalidate',p_migration_id,p_switch_id),format('config://%s/%s',p_tenant_id,v_migration.target_config_version),p_traceparent);
  RETURN QUERY SELECT v_tenant.version+1,v_migration.target_config_version;
END;
$$;

CREATE FUNCTION public.begin_knowledge_backend_observation(
  p_tenant_id text,p_migration_id text,p_expected_tenant_version bigint,p_expected_migration_version bigint,
  p_at timestamptz,p_observe_until timestamptz
) RETURNS TABLE(tenant_version bigint,active_config_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE;
BEGIN
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND OR v_tenant.version<>p_expected_tenant_version OR v_migration.version<>p_expected_migration_version OR
     v_migration.domain<>'knowledge' OR v_migration.state<>'cutover' OR v_tenant.active_config_version<>v_migration.target_config_version OR
     p_at IS NULL OR p_observe_until IS NULL OR p_observe_until<=p_at THEN
    RAISE EXCEPTION 'knowledge observation authority conflict' USING ERRCODE='40001';
  END IF;
  UPDATE public.backend_migration SET state='observe',observe_until=p_observe_until,updated_at=p_at,version=version+1
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  RETURN QUERY SELECT v_tenant.version,v_tenant.active_config_version;
END;
$$;

CREATE FUNCTION public.rollback_knowledge_backend_migration(
  p_tenant_id text,p_migration_id text,p_expected_tenant_version bigint,p_expected_migration_version bigint,
  p_rollback_sync_watermark text,p_at timestamptz,p_switch_id text,p_actor_id text,p_reason_code text,
  p_correlation_id text,p_trace_id text,p_traceparent text
) RETURNS TABLE(tenant_version bigint,active_config_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_existing public.backend_migration_config_switch%ROWTYPE;
BEGIN
  IF p_at IS NULL OR COALESCE(length(btrim(p_switch_id)),0)=0 OR COALESCE(length(btrim(p_actor_id)),0)=0 OR
     COALESCE(length(btrim(p_reason_code)),0)=0 OR COALESCE(length(btrim(p_correlation_id)),0)=0 OR
     COALESCE(length(btrim(p_trace_id)),0)=0 OR COALESCE(length(p_rollback_sync_watermark),0)=0 THEN
    RAISE EXCEPTION 'knowledge rollback input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  SELECT * INTO v_existing FROM public.backend_migration_config_switch WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND switch_id=p_switch_id;
  IF FOUND THEN
    IF v_existing.direction<>'rollback' OR v_existing.previous_tenant_version<>p_expected_tenant_version OR
       v_existing.migration_result_version<>p_expected_migration_version OR v_existing.rollback_sync_watermark<>p_rollback_sync_watermark OR
       v_existing.actor_id<>p_actor_id OR v_existing.reason_code<>p_reason_code OR v_existing.correlation_id<>p_correlation_id OR
       v_existing.trace_id<>p_trace_id OR v_existing.occurred_at<>p_at THEN RAISE EXCEPTION 'switch id collision' USING ERRCODE='23505'; END IF;
    RETURN QUERY SELECT v_existing.tenant_result_version,v_existing.active_config_version; RETURN;
  END IF;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND OR v_tenant.version<>p_expected_tenant_version OR v_migration.version<>p_expected_migration_version OR
     v_migration.domain<>'knowledge' OR v_migration.state<>'observe' OR p_at<v_migration.updated_at OR
     v_tenant.active_config_version<>v_migration.target_config_version OR p_rollback_sync_watermark<>v_migration.verify_target_watermark OR
     EXISTS (SELECT 1 FROM public.knowledge_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND direction='reverse' AND state<>'applied') THEN
    RAISE EXCEPTION 'knowledge rollback authority conflict' USING ERRCODE='23514';
  END IF;
  UPDATE public.tenant SET active_config_version=v_migration.source_config_version,version=version+1 WHERE tenant_id=p_tenant_id;
  INSERT INTO public.backend_migration_config_switch(tenant_id,migration_id,switch_id,direction,previous_config_version,
    active_config_version,migration_result_version,previous_tenant_version,tenant_result_version,rollback_sync_watermark,
    actor_id,reason_code,correlation_id,trace_id,occurred_at)
  VALUES(p_tenant_id,p_migration_id,p_switch_id,'rollback',v_migration.target_config_version,v_migration.source_config_version,
    v_migration.version,v_tenant.version,v_tenant.version+1,p_rollback_sync_watermark,p_actor_id,p_reason_code,p_correlation_id,p_trace_id,p_at);
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent) VALUES
    (p_tenant_id,format('knowledge-migration-rollback-audit:%s:%s',p_migration_id,p_switch_id),'audit',p_migration_id,v_tenant.version+1,
      format('knowledge-migration:%s:%s:rollback-audit',p_migration_id,p_switch_id),format('backend-migration-switch://%s/%s/%s',p_tenant_id,p_migration_id,p_switch_id),p_traceparent),
    (p_tenant_id,format('knowledge-migration-rollback-invalidation:%s:%s',p_migration_id,p_switch_id),'config-invalidation',p_tenant_id,v_tenant.version+1,
      format('knowledge-migration:%s:%s:rollback-invalidate',p_migration_id,p_switch_id),format('config://%s/%s',p_tenant_id,v_migration.source_config_version),p_traceparent);
  RETURN QUERY SELECT v_tenant.version+1,v_migration.source_config_version;
END;
$$;

CREATE FUNCTION public.knowledge_backend_migration_drain_status(p_tenant_id text,p_migration_id text)
RETURNS TABLE(source_in_flight bigint,target_in_flight bigint,forward_outstanding bigint,reverse_outstanding bigint,
  active_config_version bigint,rolled_back boolean)
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
  SELECT
    (SELECT count(*) FROM public.execution_record e WHERE e.tenant_id=m.tenant_id AND e.config_version=m.source_config_version
      AND e.outcome IN ('queued','running','pending','blocked','waiting_confirmation')),
    (SELECT count(*) FROM public.execution_record e WHERE e.tenant_id=m.tenant_id AND e.config_version=m.target_config_version
      AND e.outcome IN ('queued','running','pending','blocked','waiting_confirmation')),
    (SELECT count(*) FROM public.knowledge_migration_mutation x WHERE x.tenant_id=m.tenant_id AND x.migration_id=m.migration_id AND x.direction='forward' AND x.state<>'applied'),
    (SELECT count(*) FROM public.knowledge_migration_mutation x WHERE x.tenant_id=m.tenant_id AND x.migration_id=m.migration_id AND x.direction='reverse' AND x.state<>'applied'),
    t.active_config_version,
    EXISTS(SELECT 1 FROM public.backend_migration_config_switch s WHERE s.tenant_id=m.tenant_id AND s.migration_id=m.migration_id AND s.direction='rollback')
  FROM public.backend_migration m JOIN public.tenant t ON t.tenant_id=m.tenant_id
  WHERE m.tenant_id=p_tenant_id AND m.migration_id=p_migration_id;
$$;

CREATE FUNCTION public.cleanup_knowledge_backend_migration(
  p_tenant_id text,p_migration_id text,p_expected_tenant_version bigint,p_expected_migration_version bigint,
  p_rollback_sync_watermark text,p_at timestamptz
) RETURNS TABLE(tenant_version bigint,active_config_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_rolled_back boolean; v_old_config bigint;
BEGIN
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  SELECT EXISTS(SELECT 1 FROM public.backend_migration_config_switch WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND direction='rollback') INTO v_rolled_back;
  IF NOT FOUND OR v_tenant.version<>p_expected_tenant_version OR v_migration.version<>p_expected_migration_version OR
     v_migration.domain<>'knowledge' OR v_migration.state<>'observe' OR p_at IS NULL OR p_at<v_migration.observe_until OR
     p_rollback_sync_watermark<>v_migration.verify_target_watermark OR
     (v_rolled_back AND v_tenant.active_config_version<>v_migration.source_config_version) OR
     (NOT v_rolled_back AND v_tenant.active_config_version<>v_migration.target_config_version) THEN
    RAISE EXCEPTION 'knowledge cleanup authority conflict' USING ERRCODE='40001';
  END IF;
  v_old_config := CASE WHEN v_rolled_back THEN v_migration.target_config_version ELSE v_migration.source_config_version END;
  IF EXISTS (SELECT 1 FROM public.knowledge_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND state<>'applied') OR
     EXISTS (SELECT 1 FROM public.execution_record WHERE tenant_id=p_tenant_id AND config_version=v_old_config
       AND outcome IN ('queued','running','pending','blocked','waiting_confirmation')) THEN
    RAISE EXCEPTION 'knowledge cleanup drain is incomplete' USING ERRCODE='23514';
  END IF;
  UPDATE public.backend_migration SET state='cleanup',rollback_sync_watermark=p_rollback_sync_watermark,updated_at=p_at,version=version+1
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  RETURN QUERY SELECT v_tenant.version,v_tenant.active_config_version;
END;
$$;

REVOKE ALL ON FUNCTION public.guard_knowledge_migration_direction_update() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.cutover_knowledge_backend_migration(text,text,bigint,bigint,bigint,bigint,text,text,text,text,text,timestamptz,text,text,text,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.begin_knowledge_backend_observation(text,text,bigint,bigint,timestamptz,timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.rollback_knowledge_backend_migration(text,text,bigint,bigint,text,timestamptz,text,text,text,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.cleanup_knowledge_backend_migration(text,text,bigint,bigint,text,timestamptz) FROM PUBLIC;

COMMIT;
