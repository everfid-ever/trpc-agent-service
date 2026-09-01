BEGIN;

CREATE TABLE public.session_migration_mutation (
  tenant_id text NOT NULL,
  migration_id text NOT NULL,
  mutation_id text NOT NULL,
  epoch bigint NOT NULL CHECK (epoch >= 1),
  agent_app_id text NOT NULL,
  session_id text NOT NULL,
  source_version bigint NOT NULL CHECK (source_version >= 1),
  mutation_digest text NOT NULL CHECK (mutation_digest ~ '^[0-9a-f]{64}$'),
  state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','applying','applied')),
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  lease_owner text NOT NULL DEFAULT '',
  lease_until timestamptz,
  not_before timestamptz NOT NULL,
  last_error_class text NOT NULL DEFAULT '',
  target_version bigint CHECK (target_version IS NULL OR target_version >= source_version),
  target_digest text NOT NULL DEFAULT '' CHECK (target_digest = '' OR target_digest ~ '^[0-9a-f]{64}$'),
  applied_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
  PRIMARY KEY (tenant_id,migration_id,agent_app_id,session_id,mutation_id),
  FOREIGN KEY (tenant_id,migration_id)
    REFERENCES public.backend_migration(tenant_id,migration_id),
  CHECK (length(btrim(mutation_id)) > 0),
  CHECK (length(last_error_class) <= 64),
  CHECK (updated_at >= created_at)
);

CREATE INDEX session_migration_mutation_repair_idx
  ON public.session_migration_mutation(tenant_id,migration_id,not_before,created_at)
  WHERE state <> 'applied';

CREATE OR REPLACE FUNCTION public.capture_session_migration_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE
  v_migration public.backend_migration%ROWTYPE;
  v_config_version bigint;
BEGIN
  SELECT config_version INTO v_config_version
  FROM public.execution_record
  WHERE tenant_id=NEW.tenant_id AND request_id=NEW.request_id;
  SELECT * INTO v_migration
  FROM public.backend_migration
  WHERE tenant_id=NEW.tenant_id AND domain='session'
    AND state IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe')
    AND source_config_version=v_config_version
  ORDER BY epoch DESC LIMIT 1 FOR SHARE;
  IF NOT FOUND THEN
    RETURN NEW;
  END IF;
  INSERT INTO public.session_migration_mutation(
    tenant_id,migration_id,mutation_id,epoch,agent_app_id,session_id,
    source_version,mutation_digest,not_before,created_at,updated_at)
  VALUES(NEW.tenant_id,v_migration.migration_id,NEW.commit_id,v_migration.epoch,
    NEW.agent_app_id,NEW.session_id,NEW.session_version,NEW.request_digest,
    NEW.created_at,NEW.created_at,NEW.created_at)
  ON CONFLICT (tenant_id,migration_id,agent_app_id,session_id,mutation_id) DO NOTHING;
  RETURN NEW;
END;
$$;

CREATE TRIGGER session_commit_capture_migration
AFTER INSERT ON public.session_commit
FOR EACH ROW EXECUTE FUNCTION public.capture_session_migration_mutation();

CREATE OR REPLACE FUNCTION public.guard_session_migration_mutation_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF (NEW.tenant_id,NEW.migration_id,NEW.mutation_id,NEW.epoch,NEW.agent_app_id,
      NEW.session_id,NEW.source_version,NEW.mutation_digest,NEW.created_at)
    IS DISTINCT FROM
     (OLD.tenant_id,OLD.migration_id,OLD.mutation_id,OLD.epoch,OLD.agent_app_id,
      OLD.session_id,OLD.source_version,OLD.mutation_digest,OLD.created_at) THEN
    RAISE EXCEPTION 'session migration mutation identity is immutable' USING ERRCODE='23000';
  END IF;
  IF NEW.version <> OLD.version + 1 OR NEW.updated_at < OLD.updated_at THEN
    RAISE EXCEPTION 'session migration mutation version must advance exactly once' USING ERRCODE='40001';
  END IF;
  IF NOT ((OLD.state='pending' AND NEW.state='applying') OR
          (OLD.state='applying' AND NEW.state='applying' AND OLD.lease_until<=NEW.updated_at) OR
          (OLD.state='applying' AND NEW.state IN ('pending','applied'))) THEN
    RAISE EXCEPTION 'illegal session migration mutation transition' USING ERRCODE='23514';
  END IF;
  IF NEW.state='applying' AND (length(NEW.lease_owner)=0 OR NEW.lease_until IS NULL OR NEW.lease_until<=NEW.updated_at) THEN
    RAISE EXCEPTION 'session migration mutation lease is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='applying' AND (NEW.attempt<>OLD.attempt+1 OR
      (NEW.not_before,NEW.last_error_class,NEW.target_version,NEW.target_digest,NEW.applied_at)
        IS DISTINCT FROM
      (OLD.not_before,OLD.last_error_class,OLD.target_version,OLD.target_digest,OLD.applied_at)) THEN
    RAISE EXCEPTION 'session migration mutation claim is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='pending' AND (length(NEW.lease_owner)>0 OR NEW.lease_until IS NOT NULL OR NEW.not_before<NEW.updated_at) THEN
    RAISE EXCEPTION 'session migration mutation retry is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='pending' AND (NEW.attempt<>OLD.attempt OR
      (NEW.target_version,NEW.target_digest,NEW.applied_at)
        IS DISTINCT FROM (OLD.target_version,OLD.target_digest,OLD.applied_at)) THEN
    RAISE EXCEPTION 'session migration mutation retry result is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='applied' AND (NEW.target_version IS NULL OR NEW.target_digest !~ '^[0-9a-f]{64}$' OR
      NEW.applied_at IS NULL OR length(NEW.lease_owner)>0 OR NEW.lease_until IS NOT NULL OR NEW.attempt<>OLD.attempt) THEN
    RAISE EXCEPTION 'session migration mutation result is invalid' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER session_migration_mutation_guard
BEFORE UPDATE ON public.session_migration_mutation
FOR EACH ROW EXECUTE FUNCTION public.guard_session_migration_mutation_update();

CREATE OR REPLACE FUNCTION public.guard_session_migration_authority_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
  IF NEW.domain='session' AND NEW.state IN ('verify','cutover','cleanup') AND EXISTS (
    SELECT 1 FROM public.session_migration_mutation
    WHERE tenant_id=NEW.tenant_id AND migration_id=NEW.migration_id AND state<>'applied'
  ) THEN
    RAISE EXCEPTION 'session migration repair backlog is not drained' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER backend_migration_session_repair_gate
BEFORE UPDATE ON public.backend_migration
FOR EACH ROW EXECUTE FUNCTION public.guard_session_migration_authority_update();

REVOKE ALL ON FUNCTION public.capture_session_migration_mutation() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.guard_session_migration_mutation_update() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.guard_session_migration_authority_update() FROM PUBLIC;

COMMIT;
