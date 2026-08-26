BEGIN;

DROP INDEX IF EXISTS public.delivery_ledger_retry_idx;

UPDATE public.delivery_ledger
SET state = CASE WHEN state='sent' THEN 'sent' WHEN state='failed' THEN 'failed' ELSE 'pending' END;

ALTER TABLE public.delivery_ledger
  DROP CONSTRAINT delivery_ledger_state_check,
  DROP CONSTRAINT delivery_ledger_content_digest_check,
  DROP CONSTRAINT delivery_ledger_segment_count_check,
  DROP CONSTRAINT delivery_ledger_attempt_check,
  DROP CONSTRAINT delivery_ledger_renderer_version_check,
  DROP CONSTRAINT delivery_ledger_format_version_check,
  DROP COLUMN renderer_version,
  DROP COLUMN format_version,
  DROP COLUMN content_digest,
  DROP COLUMN segment_count,
  DROP COLUMN attempt,
  DROP COLUMN not_before,
  DROP COLUMN last_error_class,
  ADD CONSTRAINT delivery_ledger_state_check
    CHECK (state IN ('pending','sent','confirmed','failed'));

COMMIT;
