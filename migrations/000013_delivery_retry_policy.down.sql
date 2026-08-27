BEGIN;

ALTER TABLE public.delivery_ledger
  DROP CONSTRAINT delivery_ledger_reconcile_attempt_check,
  DROP COLUMN reconcile_attempt;

COMMIT;
