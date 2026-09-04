BEGIN;

DROP TRIGGER IF EXISTS outbox_idempotency_guard ON public.outbox;
DROP FUNCTION IF EXISTS public.guard_outbox_idempotency();

COMMIT;
