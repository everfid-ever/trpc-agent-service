BEGIN;

-- A parked request can reach its deadline while an earlier request is waiting
-- for a human confirmation.  Once that earlier request commits terminally,
-- the blocked request is again the next ordered input and can be retried
-- safely.  Recover only that exact next input; out-of-order requests remain
-- blocked and cannot overtake the session fence.
CREATE OR REPLACE FUNCTION public.enqueue_next_parked_wakeup()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_execution public.execution_record%ROWTYPE;
BEGIN
  IF NEW.next_input_seq <= OLD.next_input_seq THEN
    RETURN NEW;
  END IF;

  SELECT * INTO v_execution FROM public.execution_record e
    WHERE e.tenant_id=NEW.tenant_id AND e.agent_app_id=NEW.agent_app_id
      AND e.session_id=NEW.session_id AND e.input_seq=NEW.next_input_seq
      AND e.outcome IN ('pending','blocked')
    FOR UPDATE;
  IF NOT FOUND THEN
    RETURN NEW;
  END IF;

  IF v_execution.outcome='blocked' THEN
    UPDATE public.execution_record SET outcome='pending',park_attempt=0,
      not_before=clock_timestamp(),park_deadline=NULL,
      blocked_at=NULL,blocked_reason=NULL,version=version+1
      WHERE tenant_id=v_execution.tenant_id AND request_id=v_execution.request_id
      RETURNING * INTO v_execution;
  END IF;

  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,
    idempotency_key,payload_ref,traceparent,next_attempt_at)
  VALUES(v_execution.tenant_id,format('wakeup:%s:recovered:%s',v_execution.request_id,v_execution.version),
    'wakeup',v_execution.request_id,GREATEST(1,v_execution.version),
    format('wakeup:%s:recovered:%s',v_execution.request_id,v_execution.version),
    format('execution://%s/%s',v_execution.tenant_id,v_execution.request_id),
    v_execution.traceparent,GREATEST(now(),COALESCE(v_execution.not_before,now())))
  ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING;
  RETURN NEW;
END;
$$;

-- Recover pre-existing local or interrupted deployments where the predecessor
-- committed before this migration was installed.  This is deliberately
-- restricted to the authoritative next input of each session.
WITH recovered AS (
  UPDATE public.execution_record e
     SET outcome='pending',park_attempt=0,not_before=clock_timestamp(),
         park_deadline=NULL,blocked_at=NULL,blocked_reason=NULL,version=e.version+1
    FROM public.session_head h
   WHERE e.tenant_id=h.tenant_id AND e.agent_app_id=h.agent_app_id
     AND e.session_id=h.session_id AND e.input_seq=h.next_input_seq
     AND e.outcome='blocked'
  RETURNING e.tenant_id,e.request_id,e.version,e.traceparent,e.not_before
)
INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,
  idempotency_key,payload_ref,traceparent,next_attempt_at)
SELECT tenant_id,format('wakeup:%s:recovered:%s',request_id,version),
  'wakeup',request_id,GREATEST(1,version),
  format('wakeup:%s:recovered:%s',request_id,version),
  format('execution://%s/%s',tenant_id,request_id),traceparent,
  GREATEST(now(),COALESCE(not_before,now()))
FROM recovered
ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING;

REVOKE ALL ON FUNCTION public.enqueue_next_parked_wakeup() FROM PUBLIC;

COMMIT;
