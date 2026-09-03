BEGIN;

-- Summary bodies live outside session_summary.  session_summary remains the
-- durable monotonic boundary committed by commit_turn; this table holds the
-- immutable body addressed by its content_ref.
CREATE TABLE session_summary_content (
  tenant_id text NOT NULL,
  agent_app_id text NOT NULL,
  session_id text NOT NULL,
  summary_id text NOT NULL,
  content_ref text NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  content bytea NOT NULL CHECK (octet_length(content) > 0 AND octet_length(content) <= 1048576),
  state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','superseded','delete_claimed')),
  superseded_by_summary_id text,
  frozen boolean NOT NULL DEFAULT false,
  claim_owner text,
  claim_until timestamptz,
  not_before timestamptz NOT NULL DEFAULT now(),
  delete_attempt integer NOT NULL DEFAULT 0 CHECK (delete_attempt >= 0),
  last_error_class text,
  record_version bigint NOT NULL DEFAULT 0 CHECK (record_version >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  superseded_at timestamptz,
  PRIMARY KEY (tenant_id,agent_app_id,session_id,summary_id),
  UNIQUE (tenant_id,agent_app_id,session_id,content_ref),
  FOREIGN KEY (tenant_id,agent_app_id,session_id,summary_id)
    REFERENCES session_summary(tenant_id,agent_app_id,session_id,summary_id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id,agent_app_id,session_id,superseded_by_summary_id)
    REFERENCES session_summary(tenant_id,agent_app_id,session_id,summary_id) ON DELETE RESTRICT,
  CHECK ((state='active' AND superseded_by_summary_id IS NULL AND superseded_at IS NULL AND claim_owner IS NULL AND claim_until IS NULL)
    OR (state='superseded' AND superseded_by_summary_id IS NOT NULL AND superseded_at IS NOT NULL AND claim_owner IS NULL AND claim_until IS NULL)
    OR (state='delete_claimed' AND superseded_by_summary_id IS NOT NULL AND superseded_at IS NOT NULL AND claim_owner IS NOT NULL AND claim_until IS NOT NULL))
);

CREATE INDEX session_summary_content_compact_idx
  ON session_summary_content(state,superseded_at,tenant_id,agent_app_id,session_id,summary_id)
  WHERE state IN ('superseded','delete_claimed') AND NOT frozen;

CREATE FUNCTION public.guard_session_summary_content()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
  old_base bigint;
  new_base bigint;
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD.state <> 'delete_claimed' THEN
      RAISE EXCEPTION 'summary content must be claimed before deletion' USING ERRCODE='55000';
    END IF;
    RETURN OLD;
  END IF;
  IF TG_OP = 'INSERT' THEN
    RETURN NEW;
  END IF;
  IF NEW.tenant_id <> OLD.tenant_id OR NEW.agent_app_id <> OLD.agent_app_id OR NEW.session_id <> OLD.session_id OR
     NEW.summary_id <> OLD.summary_id OR NEW.content_ref <> OLD.content_ref OR NEW.content_digest <> OLD.content_digest OR
     NEW.content <> OLD.content OR NEW.created_at <> OLD.created_at THEN
    RAISE EXCEPTION 'summary content identity is immutable' USING ERRCODE='23514';
  END IF;
  IF NEW.record_version <> OLD.record_version + 1 THEN
    RAISE EXCEPTION 'summary content version must advance by one' USING ERRCODE='40001';
  END IF;
  IF OLD.state = 'active' AND NEW.state = 'superseded' THEN
    SELECT base_session_seq INTO old_base FROM public.session_summary
      WHERE tenant_id=OLD.tenant_id AND agent_app_id=OLD.agent_app_id AND session_id=OLD.session_id AND summary_id=OLD.summary_id;
    SELECT base_session_seq INTO new_base FROM public.session_summary
      WHERE tenant_id=NEW.tenant_id AND agent_app_id=NEW.agent_app_id AND session_id=NEW.session_id AND summary_id=NEW.superseded_by_summary_id;
    IF new_base IS NULL OR new_base <= old_base THEN
      RAISE EXCEPTION 'summary replacement must advance watermark' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
  END IF;
  IF (OLD.state = 'superseded' AND NEW.state = 'delete_claimed') OR
     (OLD.state = 'delete_claimed' AND NEW.state IN ('delete_claimed','superseded')) THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'illegal summary content transition' USING ERRCODE='23514';
END;
$$;

CREATE TRIGGER session_summary_content_guard
BEFORE INSERT OR UPDATE OR DELETE ON session_summary_content
FOR EACH ROW EXECUTE FUNCTION public.guard_session_summary_content();

COMMIT;
