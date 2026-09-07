BEGIN;

ALTER TABLE public.agent_app_revision
    ADD COLUMN fallback_model_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD CONSTRAINT agent_app_revision_fallback_model_refs_check CHECK (
        jsonb_typeof(fallback_model_refs) = 'array'
        AND (agent_kind = 'llm'::text OR fallback_model_refs = '[]'::jsonb)
    );

COMMIT;
