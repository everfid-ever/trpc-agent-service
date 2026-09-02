BEGIN;

CREATE TABLE public.knowledge_migration_mutation (
  tenant_id text NOT NULL,
  migration_id text NOT NULL,
  mutation_id text NOT NULL,
  epoch bigint NOT NULL CHECK (epoch >= 1),
  knowledge_id text NOT NULL,
  knowledge_version bigint NOT NULL CHECK (knowledge_version >= 1),
  chunk_id text NOT NULL,
  operation text NOT NULL CHECK (operation IN ('upsert','delete')),
  source_revision bigint NOT NULL CHECK (source_revision >= 1),
  mutation_digest text NOT NULL CHECK (mutation_digest ~ '^[0-9a-f]{64}$'),
  state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','applying','applied')),
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  lease_owner text NOT NULL DEFAULT '',
  lease_until timestamptz,
  not_before timestamptz NOT NULL,
  last_error_class text NOT NULL DEFAULT '',
  target_revision bigint CHECK (target_revision IS NULL OR target_revision >= source_revision),
  target_digest text NOT NULL DEFAULT '' CHECK (target_digest = '' OR target_digest ~ '^[0-9a-f]{64}$'),
  applied_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
  PRIMARY KEY (tenant_id,migration_id,knowledge_id,knowledge_version,chunk_id,mutation_id),
  UNIQUE (tenant_id,migration_id,mutation_id),
  FOREIGN KEY (tenant_id,migration_id) REFERENCES public.backend_migration(tenant_id,migration_id),
  CHECK (length(btrim(mutation_id)) BETWEEN 1 AND 128),
  CHECK (length(btrim(knowledge_id)) BETWEEN 1 AND 256),
  CHECK (length(btrim(chunk_id)) BETWEEN 1 AND 512),
  CHECK (length(last_error_class) <= 64),
  CHECK (updated_at >= created_at)
);

CREATE INDEX knowledge_migration_mutation_repair_idx
  ON public.knowledge_migration_mutation(tenant_id,migration_id,not_before,created_at)
  WHERE state <> 'applied';

CREATE FUNCTION public.record_knowledge_migration_mutation(
  p_tenant_id text,p_migration_id text,p_mutation_id text,p_epoch bigint,
  p_knowledge_id text,p_knowledge_version bigint,p_chunk_id text,p_operation text,
  p_source_revision bigint,p_mutation_digest text,p_created_at timestamptz
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_migration public.backend_migration%ROWTYPE; v_existing public.knowledge_migration_mutation%ROWTYPE;
BEGIN
  SELECT * INTO v_migration FROM public.backend_migration
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_migration.domain<>'knowledge' OR v_migration.epoch<>p_epoch OR
     v_migration.state NOT IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe') OR
     p_created_at IS NULL OR p_created_at<v_migration.created_at THEN
    RAISE EXCEPTION 'knowledge migration authority conflict' USING ERRCODE='23514';
  END IF;
  INSERT INTO public.knowledge_migration_mutation(tenant_id,migration_id,mutation_id,epoch,
    knowledge_id,knowledge_version,chunk_id,operation,source_revision,mutation_digest,
    not_before,created_at,updated_at)
  VALUES(p_tenant_id,p_migration_id,p_mutation_id,p_epoch,p_knowledge_id,p_knowledge_version,
    p_chunk_id,p_operation,p_source_revision,p_mutation_digest,p_created_at,p_created_at,p_created_at)
  ON CONFLICT (tenant_id,migration_id,knowledge_id,knowledge_version,chunk_id,mutation_id) DO NOTHING;
  SELECT * INTO v_existing FROM public.knowledge_migration_mutation
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND knowledge_id=p_knowledge_id
      AND knowledge_version=p_knowledge_version AND chunk_id=p_chunk_id AND mutation_id=p_mutation_id;
  IF (v_existing.epoch,v_existing.operation,v_existing.source_revision,v_existing.mutation_digest,v_existing.created_at)
     IS DISTINCT FROM (p_epoch,p_operation,p_source_revision,p_mutation_digest,p_created_at) THEN
    RAISE EXCEPTION 'knowledge mutation id collision' USING ERRCODE='23505';
  END IF;
END;
$$;

CREATE FUNCTION public.guard_knowledge_migration_mutation_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF (NEW.tenant_id,NEW.migration_id,NEW.mutation_id,NEW.epoch,NEW.knowledge_id,
      NEW.knowledge_version,NEW.chunk_id,NEW.operation,NEW.source_revision,NEW.mutation_digest,NEW.created_at)
    IS DISTINCT FROM
     (OLD.tenant_id,OLD.migration_id,OLD.mutation_id,OLD.epoch,OLD.knowledge_id,
      OLD.knowledge_version,OLD.chunk_id,OLD.operation,OLD.source_revision,OLD.mutation_digest,OLD.created_at) THEN
    RAISE EXCEPTION 'knowledge migration mutation identity is immutable' USING ERRCODE='23000';
  END IF;
  IF NEW.version<>OLD.version+1 OR NEW.updated_at<OLD.updated_at THEN
    RAISE EXCEPTION 'knowledge migration mutation version must advance exactly once' USING ERRCODE='40001';
  END IF;
  IF NOT ((OLD.state='pending' AND NEW.state='applying') OR
          (OLD.state='applying' AND NEW.state='applying' AND OLD.lease_until<=NEW.updated_at) OR
          (OLD.state='applying' AND NEW.state IN ('pending','applied'))) THEN
    RAISE EXCEPTION 'illegal knowledge migration mutation transition' USING ERRCODE='23514';
  END IF;
  IF NEW.state='applying' AND (length(NEW.lease_owner)=0 OR NEW.lease_until IS NULL OR
      NEW.lease_until<=NEW.updated_at OR NEW.attempt<>OLD.attempt+1 OR
      (NEW.not_before,NEW.last_error_class,NEW.target_revision,NEW.target_digest,NEW.applied_at)
        IS DISTINCT FROM (OLD.not_before,OLD.last_error_class,OLD.target_revision,OLD.target_digest,OLD.applied_at)) THEN
    RAISE EXCEPTION 'knowledge migration mutation claim is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='pending' AND (length(NEW.lease_owner)>0 OR NEW.lease_until IS NOT NULL OR
      NEW.not_before<NEW.updated_at OR NEW.attempt<>OLD.attempt OR
      (NEW.target_revision,NEW.target_digest,NEW.applied_at)
        IS DISTINCT FROM (OLD.target_revision,OLD.target_digest,OLD.applied_at)) THEN
    RAISE EXCEPTION 'knowledge migration mutation retry is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='applied' AND (NEW.target_revision IS NULL OR NEW.target_digest!~'^[0-9a-f]{64}$' OR
      NEW.applied_at IS NULL OR length(NEW.lease_owner)>0 OR NEW.lease_until IS NOT NULL OR NEW.attempt<>OLD.attempt) THEN
    RAISE EXCEPTION 'knowledge migration mutation result is invalid' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_migration_mutation_guard
BEFORE UPDATE ON public.knowledge_migration_mutation
FOR EACH ROW EXECUTE FUNCTION public.guard_knowledge_migration_mutation_update();

CREATE FUNCTION public.guard_knowledge_migration_authority_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
  IF NEW.domain='knowledge' AND NEW.state IN ('verify','cutover','cleanup') AND EXISTS (
    SELECT 1 FROM public.knowledge_migration_mutation
      WHERE tenant_id=NEW.tenant_id AND migration_id=NEW.migration_id AND state<>'applied'
  ) THEN
    RAISE EXCEPTION 'knowledge migration repair backlog is not drained' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER backend_migration_knowledge_repair_gate
BEFORE UPDATE ON public.backend_migration
FOR EACH ROW EXECUTE FUNCTION public.guard_knowledge_migration_authority_update();

REVOKE ALL ON FUNCTION public.guard_knowledge_migration_mutation_update() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.guard_knowledge_migration_authority_update() FROM PUBLIC;

COMMIT;
