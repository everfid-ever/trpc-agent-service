BEGIN;

ALTER TABLE agent_app_revision
  ADD COLUMN agent_spec jsonb NOT NULL DEFAULT '{}'::jsonb,
  ALTER COLUMN instruction SET DEFAULT '',
  ALTER COLUMN model_profile_id DROP NOT NULL,
  ALTER COLUMN model_profile_version DROP NOT NULL,
  DROP CONSTRAINT agent_app_revision_agent_kind_check,
  DROP CONSTRAINT agent_app_revision_instruction_check,
  DROP CONSTRAINT agent_app_revision_model_profile_id_check,
  DROP CONSTRAINT agent_app_revision_model_profile_version_check;

ALTER TABLE agent_app_revision
  ADD CONSTRAINT agent_app_revision_agent_kind_v1_check
    CHECK (agent_kind IN ('llm', 'graph', 'chain', 'parallel', 'cycle')),
  ADD CONSTRAINT agent_app_revision_agent_spec_v1_check
    CHECK (schema_version = 1 AND jsonb_typeof(agent_spec) = 'object'),
  ADD CONSTRAINT agent_app_revision_instruction_v1_check
    CHECK (length(instruction) <= 65536),
  ADD CONSTRAINT agent_app_revision_model_v1_check CHECK (
    (agent_kind = 'llm'
      AND length(btrim(instruction)) >= 1
      AND model_profile_id IS NOT NULL
      AND length(btrim(model_profile_id)) > 0
      AND model_profile_version IS NOT NULL
      AND model_profile_version >= 1)
    OR
    (agent_kind <> 'llm'
      AND model_profile_id IS NULL
      AND model_profile_version IS NULL)
  );

CREATE TABLE agent_app_revision_child (
  tenant_id          text    NOT NULL,
  agent_app_id       text    NOT NULL,
  revision           bigint  NOT NULL,
  node_key           text    NOT NULL,
  ordinal            integer NOT NULL,
  child_agent_app_id text    NOT NULL,
  child_revision     bigint  NOT NULL,
  child_digest       text    NOT NULL,
  PRIMARY KEY (tenant_id, agent_app_id, revision, node_key),
  UNIQUE (tenant_id, agent_app_id, revision, ordinal),
  FOREIGN KEY (tenant_id, agent_app_id, revision)
    REFERENCES agent_app_revision(tenant_id, agent_app_id, revision) ON DELETE CASCADE,
  FOREIGN KEY (tenant_id, child_agent_app_id, child_revision)
    REFERENCES agent_app_revision(tenant_id, agent_app_id, revision),
  CHECK (node_key ~ '^[a-z][a-z0-9_-]{0,63}$'),
  CHECK (ordinal >= 0),
  CHECK (child_revision >= 1),
  CHECK (child_digest ~ '^[0-9a-f]{64}$'),
  CHECK (child_agent_app_id <> agent_app_id OR child_revision <> revision)
);

CREATE TABLE agent_app_revision_skill (
  tenant_id      text   NOT NULL,
  agent_app_id   text   NOT NULL,
  revision       bigint NOT NULL,
  skill_id       text   NOT NULL,
  skill_version  bigint NOT NULL,
  content_digest text   NOT NULL,
  PRIMARY KEY (tenant_id, agent_app_id, revision, skill_id),
  FOREIGN KEY (tenant_id, agent_app_id, revision)
    REFERENCES agent_app_revision(tenant_id, agent_app_id, revision) ON DELETE CASCADE,
  CHECK (skill_version >= 1),
  CHECK (content_digest ~ '^[0-9a-f]{64}$')
);

CREATE OR REPLACE FUNCTION public.guard_agent_app_revision_child()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  v_state text;
  v_digest text;
BEGIN
  SELECT state, content_digest INTO v_state, v_digest
  FROM public.agent_app_revision
  WHERE tenant_id = NEW.tenant_id
    AND agent_app_id = NEW.child_agent_app_id
    AND revision = NEW.child_revision;
  IF NOT FOUND OR v_state <> 'published' OR v_digest <> NEW.child_digest THEN
    RAISE EXCEPTION 'child revision must be published with matching digest'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_revision_child_draft_only
BEFORE INSERT OR UPDATE OR DELETE ON agent_app_revision_child
FOR EACH ROW EXECUTE FUNCTION public.guard_revision_child_write();

CREATE TRIGGER agent_app_revision_child_target_guard
BEFORE INSERT OR UPDATE ON agent_app_revision_child
FOR EACH ROW EXECUTE FUNCTION public.guard_agent_app_revision_child();

CREATE TRIGGER agent_app_revision_skill_draft_only
BEFORE INSERT OR UPDATE OR DELETE ON agent_app_revision_skill
FOR EACH ROW EXECUTE FUNCTION public.guard_revision_child_write();

COMMIT;
