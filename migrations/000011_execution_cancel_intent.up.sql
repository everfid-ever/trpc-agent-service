BEGIN;

ALTER TABLE public.outbox DROP CONSTRAINT outbox_kind_check;
ALTER TABLE public.outbox ADD CONSTRAINT outbox_kind_check CHECK
  (kind IN ('audit', 'tenant-control', 'config-invalidation', 'dispatch', 'reply', 'wakeup', 'execution-control'));

ALTER TABLE public.execution_record
  ADD COLUMN cancel_requested_at timestamptz,
  ADD COLUMN cancel_version bigint NOT NULL DEFAULT 0;

ALTER TABLE public.execution_record ADD CONSTRAINT execution_record_cancel_intent_check CHECK (
  cancel_version >= 0
  AND ((cancel_requested_at IS NULL AND cancel_version = 0)
    OR (cancel_requested_at IS NOT NULL AND cancel_version >= 1))
);

CREATE TABLE public.execution_cancel_intent (
  tenant_id text NOT NULL,
  request_id text NOT NULL,
  cancel_version bigint NOT NULL CHECK (cancel_version >= 1),
  actor_id text NOT NULL CHECK (actor_id <> ''),
  reason_code text NOT NULL CHECK (reason_code <> ''),
  traceparent text,
  requested_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,request_id,cancel_version),
  FOREIGN KEY (tenant_id,request_id) REFERENCES public.execution_record(tenant_id,request_id)
);

CREATE FUNCTION public.guard_execution_cancel_intent()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
  IF NEW.outcome = 'cancelled'
     AND OLD.cancel_requested_at IS NULL
     AND NOT EXISTS (SELECT 1 FROM public.tenant t WHERE t.tenant_id=OLD.tenant_id AND t.status='disabled') THEN
    RAISE EXCEPTION 'cancelled outcome requires a durable cancellation intent' USING ERRCODE = 'P0902';
  END IF;
  IF (OLD.cancel_requested_at IS NOT NULL
       OR EXISTS (SELECT 1 FROM public.tenant t WHERE t.tenant_id=OLD.tenant_id AND t.status='disabled'))
     AND NEW.outcome IN ('succeeded','denied','failed','confirmation_denied','confirmation_timeout') THEN
    RAISE EXCEPTION 'execution cancellation requested' USING ERRCODE = 'P0902';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER execution_cancel_intent_guard
BEFORE UPDATE OF outcome ON public.execution_record
FOR EACH ROW EXECUTE FUNCTION public.guard_execution_cancel_intent();

DROP FUNCTION public.request_cancel_execution(text,text,bigint,text);

CREATE FUNCTION public.request_cancel_execution(
  p_tenant_id text, p_request_id text, p_expected_version bigint,
  p_actor_id text, p_reason_code text, p_traceparent text
) RETURNS TABLE(accepted boolean, execution_version bigint, cancel_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_execution public.execution_record%ROWTYPE;
BEGIN
  IF p_expected_version < 0 OR p_actor_id IS NULL OR p_actor_id='' OR p_reason_code IS NULL OR p_reason_code='' THEN
    RAISE EXCEPTION 'invalid cancel request' USING ERRCODE = '22023';
  END IF;
  PERFORM 1 FROM public.tenant t WHERE t.tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant not found' USING ERRCODE = 'P0002'; END IF;
  SELECT * INTO v_execution FROM public.execution_record e
    WHERE e.tenant_id = p_tenant_id AND e.request_id = p_request_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'execution not found' USING ERRCODE = 'P0002'; END IF;
  IF v_execution.outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN
    RETURN QUERY SELECT false, v_execution.version, v_execution.cancel_version;
    RETURN;
  END IF;
  IF v_execution.cancel_requested_at IS NOT NULL THEN
    RETURN QUERY SELECT true, v_execution.version, v_execution.cancel_version;
    RETURN;
  END IF;
  IF v_execution.version <> p_expected_version THEN
    RAISE EXCEPTION 'execution version conflict' USING ERRCODE = '40001';
  END IF;
  UPDATE public.execution_record e SET cancel_requested_at = clock_timestamp(),
    cancel_version = e.cancel_version + 1, version = e.version + 1
    WHERE e.tenant_id = p_tenant_id AND e.request_id = p_request_id
    RETURNING e.version,e.cancel_version INTO execution_version,cancel_version;
  INSERT INTO public.execution_cancel_intent(tenant_id,request_id,cancel_version,actor_id,reason_code,traceparent)
    VALUES(p_tenant_id,p_request_id,cancel_version,p_actor_id,p_reason_code,p_traceparent);
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent)
    VALUES
      (p_tenant_id,format('cancel-intent-audit:%s:%s',p_request_id,cancel_version),'audit',p_request_id,
       cancel_version,format('cancel-intent:%s:%s:audit',p_request_id,cancel_version),
       format('cancel-intent://%s/%s/%s',p_tenant_id,p_request_id,cancel_version),p_traceparent),
      (p_tenant_id,format('cancel-intent-control:%s:%s',p_request_id,cancel_version),'execution-control',p_request_id,
       cancel_version,format('cancel-intent:%s:%s:control',p_request_id,cancel_version),
       format('cancel-intent://%s/%s/%s',p_tenant_id,p_request_id,cancel_version),p_traceparent);
  RETURN QUERY SELECT true,execution_version,cancel_version;
END;
$$;

REVOKE ALL ON TABLE public.execution_cancel_intent FROM PUBLIC;
REVOKE ALL ON FUNCTION public.request_cancel_execution(text,text,bigint,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.guard_execution_cancel_intent() FROM PUBLIC;

COMMIT;
