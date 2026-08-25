BEGIN;

DROP TRIGGER IF EXISTS agent_app_revision_skill_draft_only ON agent_app_revision_skill;
DROP TRIGGER IF EXISTS agent_app_revision_child_target_guard ON agent_app_revision_child;
DROP TRIGGER IF EXISTS agent_app_revision_child_draft_only ON agent_app_revision_child;
DROP FUNCTION IF EXISTS public.guard_agent_app_revision_child();
DROP TABLE IF EXISTS agent_app_revision_skill;
DROP TABLE IF EXISTS agent_app_revision_child;

ALTER TABLE agent_app_revision
  DROP CONSTRAINT IF EXISTS agent_app_revision_model_v1_check,
  DROP CONSTRAINT IF EXISTS agent_app_revision_instruction_v1_check,
  DROP CONSTRAINT IF EXISTS agent_app_revision_agent_spec_v1_check,
  DROP CONSTRAINT IF EXISTS agent_app_revision_agent_kind_v1_check,
  DROP COLUMN IF EXISTS agent_spec;

ALTER TABLE agent_app_revision
  ALTER COLUMN instruction DROP DEFAULT,
  ALTER COLUMN model_profile_id SET NOT NULL,
  ALTER COLUMN model_profile_version SET NOT NULL,
  ADD CONSTRAINT agent_app_revision_agent_kind_check CHECK (agent_kind = 'llm'),
  ADD CONSTRAINT agent_app_revision_instruction_check
    CHECK (length(btrim(instruction)) BETWEEN 1 AND 65536),
  ADD CONSTRAINT agent_app_revision_model_profile_id_check
    CHECK (length(btrim(model_profile_id)) > 0),
  ADD CONSTRAINT agent_app_revision_model_profile_version_check
    CHECK (model_profile_version >= 1);

COMMIT;
