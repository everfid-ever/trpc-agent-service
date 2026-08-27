BEGIN;

DROP FUNCTION IF EXISTS public.inspect_execution_wakeup(text,text);
DROP TRIGGER IF EXISTS session_head_wakeup_next_parked ON public.session_head;
DROP FUNCTION IF EXISTS public.enqueue_next_parked_wakeup();
DROP FUNCTION IF EXISTS public.park_execution(text,text,bigint,bigint,bigint,bigint,integer);
DROP INDEX IF EXISTS public.execution_park_ready_idx;

UPDATE public.execution_record SET outcome='pending' WHERE outcome='blocked';
ALTER TABLE public.execution_record DROP CONSTRAINT execution_record_outcome_check;
ALTER TABLE public.execution_record DROP CONSTRAINT execution_record_park_state_check;
ALTER TABLE public.execution_record ADD CONSTRAINT execution_record_outcome_check
  CHECK (outcome IN ('queued', 'running', 'pending', 'waiting_confirmation', 'succeeded', 'denied', 'failed',
    'cancelled', 'confirmation_denied', 'confirmation_timeout'));
ALTER TABLE public.execution_record
  DROP COLUMN blocked_reason,
  DROP COLUMN blocked_at,
  DROP COLUMN park_deadline;

CREATE FUNCTION public.park_execution(
  p_tenant_id text, p_request_id text, p_input_seq bigint, p_attempt integer
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_input_seq bigint; v_outcome text;
BEGIN
  IF p_attempt < 1 THEN RAISE EXCEPTION 'invalid park attempt' USING ERRCODE = '22023'; END IF;
  SELECT input_seq, outcome INTO v_input_seq, v_outcome FROM public.execution_record
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'execution not found' USING ERRCODE = 'P0002'; END IF;
  IF v_input_seq <> p_input_seq OR v_outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN
    RAISE EXCEPTION 'execution cannot be parked' USING ERRCODE = '55000';
  END IF;
  UPDATE public.execution_record SET outcome = 'pending', park_attempt = GREATEST(park_attempt,p_attempt),
    not_before = now() + make_interval(secs => LEAST(300, power(2,GREATEST(0,p_attempt-1))::integer)),
    version = version + 1
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id;
END;
$$;

REVOKE ALL ON FUNCTION public.park_execution(text,text,bigint,integer) FROM PUBLIC;

COMMIT;
