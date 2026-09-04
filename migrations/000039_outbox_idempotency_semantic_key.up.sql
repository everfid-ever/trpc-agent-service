BEGIN;

-- The unique key is the durable idempotency contract. Governance records its
-- audit outbox before commit_turn records the terminal transition; those two
-- writers intentionally choose different physical outbox_id values while
-- sharing one (tenant, kind, idempotency_key). Treat that existing semantic
-- key as success, exactly as INSERT ... ON CONFLICT DO NOTHING would.
CREATE OR REPLACE FUNCTION public.guard_outbox_idempotency()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(
    hashtextextended(NEW.tenant_id || chr(31) || NEW.kind || chr(31) || NEW.idempotency_key, 0)
  );

  PERFORM 1
    FROM public.outbox
    WHERE tenant_id = NEW.tenant_id
      AND kind = NEW.kind
      AND idempotency_key = NEW.idempotency_key;
  IF FOUND THEN
    RETURN NULL;
  END IF;
  RETURN NEW;
END;
$$;

COMMENT ON FUNCTION public.guard_outbox_idempotency() IS
  'Enforces semantic idempotency for public.outbox (tenant_id, kind, idempotency_key).';

COMMIT;
