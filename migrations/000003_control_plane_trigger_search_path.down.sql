BEGIN;

CREATE OR REPLACE FUNCTION public.guard_agent_app_current_revision()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_state text;
BEGIN
  IF NEW.current_revision IS NULL OR NEW.current_revision IS NOT DISTINCT FROM OLD.current_revision THEN
    RETURN NEW;
  END IF;
  SELECT state INTO v_state FROM agent_app_revision
    WHERE tenant_id = NEW.tenant_id AND agent_app_id = NEW.agent_app_id
      AND revision = NEW.current_revision;
  IF NOT FOUND OR v_state <> 'published' THEN
    RAISE EXCEPTION 'current revision must be published' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.guard_revision_child_write()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_state text;
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    SELECT state INTO v_state FROM agent_app_revision
      WHERE tenant_id = OLD.tenant_id AND agent_app_id = OLD.agent_app_id
        AND revision = OLD.revision FOR UPDATE;
    IF NOT FOUND OR v_state <> 'draft' THEN
      RAISE EXCEPTION 'source revision is not mutable' USING ERRCODE = '55000';
    END IF;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    SELECT state INTO v_state FROM agent_app_revision
      WHERE tenant_id = NEW.tenant_id AND agent_app_id = NEW.agent_app_id
        AND revision = NEW.revision FOR UPDATE;
    IF NOT FOUND OR v_state <> 'draft' THEN
      RAISE EXCEPTION 'target revision is not mutable' USING ERRCODE = '55000';
    END IF;
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

COMMIT;
