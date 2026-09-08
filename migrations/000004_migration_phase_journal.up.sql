BEGIN;

CREATE TABLE public.backend_migration_phase_intent (
  tenant_id text NOT NULL,
  migration_id text NOT NULL,
  intent_id text NOT NULL,
  request jsonb NOT NULL,
  request_digest text NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  result_version bigint NOT NULL DEFAULT 0,
  created_at timestamp with time zone NOT NULL,
  completed_at timestamp with time zone,
  PRIMARY KEY (tenant_id, migration_id, intent_id),
  CONSTRAINT backend_migration_phase_intent_id_check CHECK (length(btrim(intent_id)) BETWEEN 1 AND 128),
  CONSTRAINT backend_migration_phase_intent_request_check CHECK (jsonb_typeof(request) = 'object'),
  CONSTRAINT backend_migration_phase_intent_digest_check CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  CONSTRAINT backend_migration_phase_intent_status_check CHECK (status = ANY (ARRAY['pending'::text, 'completed'::text])),
  CONSTRAINT backend_migration_phase_intent_completion_check CHECK (
    (status = 'pending' AND result_version = 0 AND completed_at IS NULL) OR
    (status = 'completed' AND result_version >= 2 AND completed_at IS NOT NULL)
  ),
  FOREIGN KEY (tenant_id, migration_id) REFERENCES public.backend_migration(tenant_id, migration_id)
);

CREATE INDEX backend_migration_phase_intent_pending_idx
  ON public.backend_migration_phase_intent (tenant_id, migration_id, created_at, intent_id)
  WHERE status = 'pending';

CREATE FUNCTION public.guard_backend_migration_phase_intent_update() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path TO 'pg_catalog'
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'backend migration phase intents are append-only' USING ERRCODE='23000';
  END IF;
  IF (NEW.tenant_id, NEW.migration_id, NEW.intent_id, NEW.request, NEW.request_digest, NEW.created_at)
     IS DISTINCT FROM
     (OLD.tenant_id, OLD.migration_id, OLD.intent_id, OLD.request, OLD.request_digest, OLD.created_at) THEN
    RAISE EXCEPTION 'backend migration phase intent identity is immutable' USING ERRCODE='23000';
  END IF;
  IF OLD.status <> 'pending' OR NEW.status <> 'completed' OR NEW.result_version < 2 OR NEW.completed_at IS NULL THEN
    RAISE EXCEPTION 'backend migration phase intent completion is invalid' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER backend_migration_phase_intent_guard
  BEFORE UPDATE OR DELETE ON public.backend_migration_phase_intent
  FOR EACH ROW EXECUTE FUNCTION public.guard_backend_migration_phase_intent_update();

COMMIT;
