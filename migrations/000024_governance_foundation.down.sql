BEGIN;

ALTER TABLE public.config_snapshot DROP CONSTRAINT IF EXISTS config_snapshot_policy_fk;
DROP TRIGGER IF EXISTS config_snapshot_policy_default ON public.config_snapshot;
DROP FUNCTION IF EXISTS public.ensure_config_policy_snapshot();
ALTER TABLE public.config_snapshot DROP COLUMN IF EXISTS policy_version;
DROP TABLE IF EXISTS public.governance_decision;
DROP TABLE IF EXISTS public.usage_ledger;
DROP TABLE IF EXISTS public.budget_reservation;
DROP TRIGGER IF EXISTS policy_snapshot_immutable ON public.policy_snapshot;
DROP TRIGGER IF EXISTS pricing_snapshot_immutable ON public.pricing_snapshot;
DROP FUNCTION IF EXISTS public.reject_governance_snapshot_change();
DROP TABLE IF EXISTS public.policy_snapshot;
DROP TABLE IF EXISTS public.pricing_snapshot;

COMMIT;
