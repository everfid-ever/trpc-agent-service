BEGIN;

ALTER TABLE public.execution_record
  ADD COLUMN park_deadline timestamptz,
  ADD COLUMN blocked_at timestamptz,
  ADD COLUMN blocked_reason text;

ALTER TABLE public.execution_record DROP CONSTRAINT execution_record_outcome_check;
ALTER TABLE public.execution_record ADD CONSTRAINT execution_record_outcome_check
  CHECK (outcome IN ('queued', 'running', 'pending', 'blocked', 'waiting_confirmation',
    'succeeded', 'denied', 'failed', 'cancelled', 'confirmation_denied', 'confirmation_timeout'));
ALTER TABLE public.execution_record ADD CONSTRAINT execution_record_park_state_check CHECK (
  park_attempt >= 0
  AND (park_deadline IS NULL OR not_before IS NULL OR not_before <= park_deadline)
  AND (outcome <> 'blocked' OR (blocked_at IS NOT NULL AND blocked_reason IN ('park_attempts_exhausted','park_deadline_exceeded')))
);

CREATE INDEX execution_park_ready_idx
  ON public.execution_record(tenant_id, agent_app_id, session_id, input_seq, not_before)
  WHERE outcome = 'pending';

DROP FUNCTION public.park_execution(text,text,bigint,integer);

CREATE FUNCTION public.park_execution(
  p_tenant_id text, p_request_id text, p_input_seq bigint,
  p_base_delay_seconds bigint, p_max_delay_seconds bigint,
  p_deadline_seconds bigint, p_max_attempts integer
) RETURNS TABLE(disposition text, attempt integer, execution_version bigint,
  not_before timestamptz, deadline timestamptz)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  v_probe public.execution_record%ROWTYPE;
  v_execution public.execution_record%ROWTYPE;
  v_next bigint;
  v_attempt integer;
  v_now timestamptz := clock_timestamp();
  v_deadline timestamptz;
  v_not_before timestamptz;
BEGIN
  IF p_base_delay_seconds < 1 OR p_max_delay_seconds < p_base_delay_seconds
     OR p_deadline_seconds < p_max_delay_seconds OR p_max_attempts < 1 THEN
    RAISE EXCEPTION 'invalid park policy' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_probe FROM public.execution_record e
    WHERE e.tenant_id=p_tenant_id AND e.request_id=p_request_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'execution not found' USING ERRCODE='P0002'; END IF;
  IF v_probe.input_seq <> p_input_seq THEN
    RAISE EXCEPTION 'execution park scope mismatch' USING ERRCODE='42501';
  END IF;

  SELECT h.next_input_seq INTO v_next FROM public.session_head h
    WHERE h.tenant_id=v_probe.tenant_id AND h.agent_app_id=v_probe.agent_app_id
      AND h.session_id=v_probe.session_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'session not found' USING ERRCODE='P0002'; END IF;

  SELECT * INTO v_execution FROM public.execution_record e
    WHERE e.tenant_id=p_tenant_id AND e.request_id=p_request_id FOR UPDATE;
  IF v_execution.input_seq <> p_input_seq OR v_execution.agent_app_id <> v_probe.agent_app_id
     OR v_execution.session_id <> v_probe.session_id THEN
    RAISE EXCEPTION 'execution changed during park' USING ERRCODE='40001';
  END IF;
  IF v_execution.outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout')
     OR p_input_seq < v_next THEN
    PERFORM 1 FROM public.session_commit c WHERE c.tenant_id=v_execution.tenant_id
      AND c.agent_app_id=v_execution.agent_app_id AND c.session_id=v_execution.session_id
      AND c.input_seq=v_execution.input_seq
      AND c.outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout');
    IF NOT FOUND OR v_execution.outcome NOT IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN
      RAISE EXCEPTION 'terminal invariant missing during park' USING ERRCODE='XX001';
    END IF;
    RETURN QUERY SELECT 'terminal'::text,v_execution.park_attempt,v_execution.version,
      v_execution.not_before,v_execution.park_deadline;
    RETURN;
  END IF;
  IF p_input_seq = v_next THEN
    RETURN QUERY SELECT 'ready'::text,v_execution.park_attempt,v_execution.version,
      v_execution.not_before,v_execution.park_deadline;
    RETURN;
  END IF;

  v_deadline := COALESCE(v_execution.park_deadline,
    v_now + make_interval(secs => p_deadline_seconds::double precision));
  IF v_execution.outcome = 'pending' THEN
    IF v_now >= v_deadline THEN
      UPDATE public.execution_record SET outcome='blocked',blocked_at=v_now,
        blocked_reason='park_deadline_exceeded',park_deadline=v_deadline,version=version+1
        WHERE tenant_id=p_tenant_id AND request_id=p_request_id
        RETURNING park_attempt,version INTO v_attempt,execution_version;
      INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
        VALUES(p_tenant_id,format('park-blocked:%s',p_request_id),'audit',p_request_id,1,
          format('park-blocked:%s',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id))
        ON CONFLICT (tenant_id,kind,idempotency_key) DO NOTHING;
      RETURN QUERY SELECT 'blocked'::text,v_attempt,execution_version,NULL::timestamptz,v_deadline;
      RETURN;
    END IF;
    RETURN QUERY SELECT 'parked'::text,v_execution.park_attempt,v_execution.version,
      v_execution.not_before,v_deadline;
    RETURN;
  END IF;

  v_attempt := v_execution.park_attempt + 1;
  IF v_attempt > p_max_attempts OR v_now >= v_deadline THEN
    UPDATE public.execution_record SET outcome='blocked',park_attempt=v_attempt,
      park_deadline=v_deadline,blocked_at=v_now,
      blocked_reason=CASE WHEN v_attempt > p_max_attempts THEN 'park_attempts_exhausted' ELSE 'park_deadline_exceeded' END,
      not_before=NULL,version=version+1
      WHERE tenant_id=p_tenant_id AND request_id=p_request_id
      RETURNING version INTO execution_version;
    INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
      VALUES(p_tenant_id,format('park-blocked:%s',p_request_id),'audit',p_request_id,1,
        format('park-blocked:%s',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id))
      ON CONFLICT (tenant_id,kind,idempotency_key) DO NOTHING;
    RETURN QUERY SELECT 'blocked'::text,v_attempt,execution_version,NULL::timestamptz,v_deadline;
    RETURN;
  END IF;

  v_not_before := v_now + make_interval(secs => LEAST(p_max_delay_seconds::double precision,
    p_base_delay_seconds::double precision * power(2::double precision,v_attempt-1)));
  UPDATE public.execution_record SET outcome='pending',park_attempt=v_attempt,
    not_before=v_not_before,park_deadline=v_deadline,blocked_at=NULL,blocked_reason=NULL,
    version=version+1 WHERE tenant_id=p_tenant_id AND request_id=p_request_id
    RETURNING version INTO execution_version;
  RETURN QUERY SELECT 'parked'::text,v_attempt,execution_version,v_not_before,v_deadline;
END;
$$;

CREATE FUNCTION public.enqueue_next_parked_wakeup()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_execution public.execution_record%ROWTYPE;
BEGIN
  IF NEW.next_input_seq <= OLD.next_input_seq THEN RETURN NEW; END IF;
  SELECT * INTO v_execution FROM public.execution_record e
    WHERE e.tenant_id=NEW.tenant_id AND e.agent_app_id=NEW.agent_app_id
      AND e.session_id=NEW.session_id AND e.input_seq=NEW.next_input_seq
      AND e.outcome='pending';
  IF NOT FOUND THEN RETURN NEW; END IF;
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,
    idempotency_key,payload_ref,traceparent,next_attempt_at)
  VALUES(v_execution.tenant_id,format('wakeup:%s:%s',v_execution.request_id,v_execution.park_attempt),
    'wakeup',v_execution.request_id,GREATEST(1,v_execution.park_attempt),
    format('wakeup:%s:%s',v_execution.request_id,v_execution.park_attempt),
    format('execution://%s/%s',v_execution.tenant_id,v_execution.request_id),
    v_execution.traceparent,GREATEST(now(),COALESCE(v_execution.not_before,now())))
  ON CONFLICT (tenant_id,kind,idempotency_key) DO NOTHING;
  RETURN NEW;
END;
$$;

CREATE TRIGGER session_head_wakeup_next_parked
AFTER UPDATE OF next_input_seq ON public.session_head
FOR EACH ROW EXECUTE FUNCTION public.enqueue_next_parked_wakeup();

CREATE FUNCTION public.inspect_execution_wakeup(p_tenant_id text,p_request_id text)
RETURNS TABLE(
  tenant_id text,tenant_version bigint,agent_app_id text,agent_app_version bigint,
  agent_app_revision bigint,agent_content_digest text,config_version bigint,policy_version bigint,
  request_id text,session_id text,user_id text,channel text,input_seq bigint,payload_ref text,
  traceparent text,outcome text,result_ref text,created_at timestamptz,ready boolean,blocked boolean,
  execution_version bigint
) LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_probe public.execution_record%ROWTYPE; v_execution public.execution_record%ROWTYPE; v_next bigint;
BEGIN
  SELECT * INTO v_probe FROM public.execution_record e
    WHERE e.tenant_id=p_tenant_id AND e.request_id=p_request_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'execution not found' USING ERRCODE='P0002'; END IF;
  SELECT h.next_input_seq INTO v_next FROM public.session_head h
    WHERE h.tenant_id=v_probe.tenant_id AND h.agent_app_id=v_probe.agent_app_id
      AND h.session_id=v_probe.session_id FOR UPDATE;
  SELECT * INTO v_execution FROM public.execution_record e
    WHERE e.tenant_id=p_tenant_id AND e.request_id=p_request_id FOR UPDATE;
  IF v_execution.outcome='pending' AND v_execution.park_deadline IS NOT NULL
     AND clock_timestamp() >= v_execution.park_deadline THEN
    UPDATE public.execution_record SET outcome='blocked',blocked_at=clock_timestamp(),
      blocked_reason='park_deadline_exceeded',version=version+1
      WHERE execution_record.tenant_id=p_tenant_id AND execution_record.request_id=p_request_id
      RETURNING * INTO v_execution;
    INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
      VALUES(p_tenant_id,format('park-blocked:%s',p_request_id),'audit',p_request_id,1,
        format('park-blocked:%s',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id))
      ON CONFLICT (tenant_id,kind,idempotency_key) DO NOTHING;
  END IF;
  RETURN QUERY SELECT v_execution.tenant_id,v_execution.tenant_version,v_execution.agent_app_id,
    v_execution.agent_app_version,v_execution.agent_app_revision,v_execution.agent_content_digest,
    v_execution.config_version,v_execution.policy_version,v_execution.request_id,v_execution.session_id,
    v_execution.user_id,v_execution.channel,v_execution.input_seq,v_execution.payload_ref,
    v_execution.traceparent,v_execution.outcome,v_execution.result_ref,v_execution.created_at,
    (v_execution.outcome='pending' AND v_execution.input_seq=v_next
      AND COALESCE(v_execution.not_before,'-infinity'::timestamptz)<=clock_timestamp()),
    (v_execution.outcome='blocked'),v_execution.version;
END;
$$;

REVOKE ALL ON FUNCTION public.park_execution(text,text,bigint,bigint,bigint,bigint,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.inspect_execution_wakeup(text,text) FROM PUBLIC;

COMMIT;
