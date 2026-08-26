BEGIN;

ALTER TABLE public.delivery_ledger
  DROP CONSTRAINT delivery_ledger_state_check;

UPDATE public.delivery_ledger SET state='sent' WHERE state='confirmed';

ALTER TABLE public.delivery_ledger
  ADD COLUMN renderer_version text NOT NULL DEFAULT 'legacy-v1',
  ADD COLUMN format_version text NOT NULL DEFAULT 'legacy-v1',
  ADD COLUMN content_digest text NOT NULL DEFAULT repeat('0',64),
  ADD COLUMN segment_count integer NOT NULL DEFAULT 1,
  ADD COLUMN attempt integer NOT NULL DEFAULT 0,
  ADD COLUMN not_before timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN last_error_class text,
  ADD CONSTRAINT delivery_ledger_state_check
    CHECK (state IN ('pending','sending','sent','ambiguous','retry_wait','failed')),
  ADD CONSTRAINT delivery_ledger_content_digest_check
    CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  ADD CONSTRAINT delivery_ledger_segment_count_check
    CHECK (segment_count >= 1 AND segment_no < segment_count),
  ADD CONSTRAINT delivery_ledger_attempt_check CHECK (attempt >= 0),
  ADD CONSTRAINT delivery_ledger_renderer_version_check
    CHECK (length(btrim(renderer_version)) > 0),
  ADD CONSTRAINT delivery_ledger_format_version_check
    CHECK (length(btrim(format_version)) > 0);

CREATE INDEX delivery_ledger_retry_idx
  ON public.delivery_ledger(state,not_before,updated_at)
  WHERE state IN ('pending','retry_wait','ambiguous');

COMMIT;
