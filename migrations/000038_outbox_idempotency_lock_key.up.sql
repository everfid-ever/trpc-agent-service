BEGIN;

-- 000037's function body is immutable once applied. PostgreSQL text rejects
-- chr(0), so replace only the advisory-lock key representation with a legal
-- unit-separator character. The lock may conservatively collide; it must not
-- permit a duplicate unique key to race through the guard.
CREATE OR REPLACE FUNCTION public.guard_outbox_idempotency()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog AS $$
DECLARE
  v_existing public.outbox%ROWTYPE;
BEGIN
  PERFORM pg_advisory_xact_lock(
    hashtextextended(NEW.tenant_id || chr(31) || NEW.kind || chr(31) || NEW.idempotency_key, 0)
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

COMMIT;
