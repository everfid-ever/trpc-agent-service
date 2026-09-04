BEGIN;

-- 000002 is immutable and was already applied by local installations. Make
-- outbox writes idempotent with an additive guard instead of changing that
-- migration's checksum. The transaction-scoped advisory lock closes the
-- read/insert race before inspecting the unique key's existing row.
CREATE OR REPLACE FUNCTION public.guard_outbox_idempotency()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog AS $$
DECLARE
  v_existing public.outbox%ROWTYPE;
BEGIN
  PERFORM pg_advisory_xact_lock(
    hashtextextended(NEW.tenant_id || chr(0) || NEW.kind || chr(0) || NEW.idempotency_key, 0)
  );

  SELECT * INTO v_existing
    FROM public.outbox
    WHERE tenant_id = NEW.tenant_id
      AND kind = NEW.kind
      AND idempotency_key = NEW.idempotency_key;

  IF NOT FOUND THEN
    RETURN NEW;
  END IF;
  IF v_existing.outbox_id IS DISTINCT FROM NEW.outbox_id
     OR v_existing.aggregate_id IS DISTINCT FROM NEW.aggregate_id
     OR v_existing.event_seq IS DISTINCT FROM NEW.event_seq
     OR v_existing.payload_ref IS DISTINCT FROM NEW.payload_ref
     OR v_existing.traceparent IS DISTINCT FROM NEW.traceparent THEN
    RAISE EXCEPTION 'outbox idempotency collision' USING ERRCODE = '23505';
  END IF;
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS outbox_idempotency_guard ON public.outbox;
CREATE TRIGGER outbox_idempotency_guard
BEFORE INSERT ON public.outbox
FOR EACH ROW EXECUTE FUNCTION public.guard_outbox_idempotency();

COMMIT;
