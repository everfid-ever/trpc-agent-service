BEGIN;

ALTER TABLE public.delivery_ledger
  ADD COLUMN reconcile_attempt integer NOT NULL DEFAULT 0,
  ADD CONSTRAINT delivery_ledger_reconcile_attempt_check CHECK (reconcile_attempt >= 0);

COMMIT;
