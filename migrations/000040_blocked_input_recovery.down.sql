BEGIN;

CREATE OR REPLACE FUNCTION public.enqueue_next_parked_wakeup()
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
  ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING;
  RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION public.enqueue_next_parked_wakeup() FROM PUBLIC;

COMMIT;
