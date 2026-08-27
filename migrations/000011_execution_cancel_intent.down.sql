BEGIN;

DROP FUNCTION public.request_cancel_execution(text,text,bigint,text,text,text);
DROP TRIGGER execution_cancel_intent_guard ON public.execution_record;
DROP FUNCTION public.guard_execution_cancel_intent();
DROP TABLE public.execution_cancel_intent;

-- Preserve cancellation facts when rolling back to the pre-09-02 outbox
-- contract; the acceleration-only kind is no longer understood there.
UPDATE public.outbox SET kind='audit' WHERE kind='execution-control';
ALTER TABLE public.outbox DROP CONSTRAINT outbox_kind_check;
ALTER TABLE public.outbox ADD CONSTRAINT outbox_kind_check CHECK
  (kind IN ('audit', 'tenant-control', 'config-invalidation', 'dispatch', 'reply', 'wakeup'));

ALTER TABLE public.execution_record DROP CONSTRAINT execution_record_cancel_intent_check;
ALTER TABLE public.execution_record
  DROP COLUMN cancel_version,
  DROP COLUMN cancel_requested_at;

CREATE FUNCTION public.request_cancel_execution(
  p_tenant_id text, p_request_id text, p_expected_version bigint, p_traceparent text
) RETURNS TABLE(accepted boolean, execution_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_outcome text; v_version bigint;
BEGIN
  PERFORM 1 FROM public.tenant WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant not found' USING ERRCODE = 'P0002'; END IF;
  SELECT outcome, version INTO v_outcome, v_version FROM public.execution_record
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'execution not found' USING ERRCODE = 'P0002'; END IF;
  IF v_outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN
    RETURN QUERY SELECT false, v_version;
    RETURN;
  END IF;
  IF v_version <> p_expected_version THEN RAISE EXCEPTION 'execution version conflict' USING ERRCODE = '40001'; END IF;
  UPDATE public.execution_record SET outcome = 'cancelled', version = version + 1
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id;
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent)
    VALUES
      (p_tenant_id,format('cancel-reply:%s',p_request_id),'reply',p_request_id,1,
       format('cancel:%s:reply',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id),p_traceparent),
      (p_tenant_id,format('cancel-audit:%s',p_request_id),'audit',p_request_id,1,
       format('cancel:%s:audit',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id),p_traceparent);
  RETURN QUERY SELECT true, v_version + 1;
END;
$$;

REVOKE ALL ON FUNCTION public.request_cancel_execution(text,text,bigint,text) FROM PUBLIC;

COMMIT;
