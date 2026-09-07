BEGIN;

ALTER TABLE public.agent_app_revision
    DROP CONSTRAINT agent_app_revision_fallback_model_refs_check,
    DROP COLUMN fallback_model_refs;

COMMIT;
