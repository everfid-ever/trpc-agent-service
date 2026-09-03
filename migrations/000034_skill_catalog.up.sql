BEGIN;

-- The catalog is an authority over already security-scanned, tenant-local
-- staged bytes.  It deliberately stores no arbitrary source URL or code.
CREATE TABLE skill_catalog (
  tenant_id text NOT NULL REFERENCES tenant(tenant_id),
  skill_id text NOT NULL CHECK (skill_id !~ '[\\/]' AND skill_id NOT IN ('.','..')),
  skill_version bigint NOT NULL CHECK (skill_version >= 1),
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  relative_path text NOT NULL CHECK (relative_path !~ '^/' AND relative_path !~ '(^|/)\\.\\.?(/|$)'),
  state text NOT NULL CHECK (state IN ('staged','published','failed')),
  record_version bigint NOT NULL DEFAULT 0 CHECK (record_version >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  PRIMARY KEY (tenant_id,skill_id,skill_version),
  UNIQUE (tenant_id,relative_path),
  CHECK ((state='published' AND published_at IS NOT NULL) OR (state IN ('staged','failed') AND published_at IS NULL))
);

CREATE FUNCTION public.guard_skill_catalog()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'skill catalog is append-only' USING ERRCODE='23514';
  END IF;
  IF TG_OP = 'INSERT' THEN
    RETURN NEW;
  END IF;
  IF NEW.tenant_id <> OLD.tenant_id OR NEW.skill_id <> OLD.skill_id OR NEW.skill_version <> OLD.skill_version OR
     NEW.content_digest <> OLD.content_digest OR NEW.relative_path <> OLD.relative_path OR NEW.created_at <> OLD.created_at THEN
    RAISE EXCEPTION 'skill catalog identity is immutable' USING ERRCODE='23514';
  END IF;
  IF NEW.record_version <> OLD.record_version + 1 THEN
    RAISE EXCEPTION 'skill catalog version must advance by one' USING ERRCODE='40001';
  END IF;
  IF OLD.state='staged' AND NEW.state IN ('published','failed') THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'illegal skill catalog transition' USING ERRCODE='23514';
END;
$$;

CREATE TRIGGER skill_catalog_guard
BEFORE INSERT OR UPDATE OR DELETE ON skill_catalog
FOR EACH ROW EXECUTE FUNCTION public.guard_skill_catalog();

COMMIT;
