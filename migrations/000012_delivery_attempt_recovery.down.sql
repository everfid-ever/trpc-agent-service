BEGIN;

DROP INDEX IF EXISTS public.delivery_ledger_claim_expiry_idx;

UPDATE public.delivery_ledger
SET state='ambiguous', last_error_class='migration_down_sending', claim_owner=NULL, claim_until=NULL
WHERE state='sending';

ALTER TABLE public.delivery_ledger
  DROP CONSTRAINT delivery_ledger_claim_check,
  DROP CONSTRAINT delivery_ledger_client_request_id_check,
  DROP COLUMN claim_until,
  DROP COLUMN claim_owner,
  DROP COLUMN client_request_id;

COMMIT;
