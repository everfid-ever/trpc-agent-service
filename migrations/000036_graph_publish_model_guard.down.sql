BEGIN;

CREATE OR REPLACE FUNCTION public.guard_agent_model_profile_publish()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF NEW.state = 'published' AND OLD.state = 'draft' THEN
    IF NEW.model_profile_id IS NULL OR NEW.model_profile_version IS NULL THEN
      RAISE EXCEPTION 'published agent requires a fixed model profile' USING ERRCODE = '23503';
    END IF;
    PERFORM 1 FROM public.model_profile p
      JOIN public.model_profile_revision v USING (tenant_id, model_profile_id)
      WHERE p.tenant_id = NEW.tenant_id
        AND p.model_profile_id = NEW.model_profile_id
        AND v.profile_version = NEW.model_profile_version
        AND p.status = 'active'
        AND v.content_digest ~ '^[0-9a-f]{64}$';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'model profile is missing, inactive, or invalid' USING ERRCODE = '23503';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

COMMIT;
