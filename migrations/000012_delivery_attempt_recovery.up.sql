BEGIN;

ALTER TABLE public.delivery_ledger
  ADD COLUMN client_request_id text,
  ADD COLUMN claim_owner text,
  ADD COLUMN claim_until timestamptz;

UPDATE public.delivery_ledger
SET client_request_id = 'legacy:' || delivery_key || ':' || segment_no::text,
    claim_owner = CASE WHEN state='sending' THEN 'legacy-owner' ELSE NULL END,
    claim_until = CASE WHEN state='sending' THEN now() ELSE NULL END;

ALTER TABLE public.delivery_ledger
  ALTER COLUMN client_request_id SET NOT NULL,
  ADD CONSTRAINT delivery_ledger_client_request_id_check
    CHECK (length(btrim(client_request_id)) > 0),
  ADD CONSTRAINT delivery_ledger_claim_check CHECK (
    (state='sending' AND claim_owner IS NOT NULL AND length(btrim(claim_owner)) > 0 AND claim_until IS NOT NULL)
    OR (state<>'sending' AND claim_owner IS NULL AND claim_until IS NULL)
  );

CREATE INDEX delivery_ledger_claim_expiry_idx
  ON public.delivery_ledger(claim_until)
  WHERE state='sending';

COMMIT;
