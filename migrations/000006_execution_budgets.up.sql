BEGIN;

ALTER TABLE public.agent_app_revision
    ADD COLUMN max_llm_calls integer NOT NULL DEFAULT 0,
    ADD COLUMN max_tool_calls integer NOT NULL DEFAULT 0,
    ADD COLUMN max_parallel_tools integer NOT NULL DEFAULT 0,
    ADD COLUMN execution_timeout_seconds integer NOT NULL DEFAULT 0,
    ADD CONSTRAINT agent_app_revision_execution_budget_check CHECK (max_llm_calls BETWEEN 0 AND 10000 AND max_tool_calls BETWEEN 0 AND 10000 AND max_parallel_tools BETWEEN 0 AND 1000 AND execution_timeout_seconds BETWEEN 0 AND 86400);

ALTER TABLE public.execution_record
    ADD COLUMN max_llm_calls integer NOT NULL DEFAULT 0,
    ADD COLUMN max_tool_calls integer NOT NULL DEFAULT 0,
    ADD COLUMN max_parallel_tools integer NOT NULL DEFAULT 0,
    ADD COLUMN execution_timeout_seconds integer NOT NULL DEFAULT 0,
    ADD CONSTRAINT execution_record_execution_budget_check CHECK (max_llm_calls BETWEEN 0 AND 10000 AND max_tool_calls BETWEEN 0 AND 10000 AND max_parallel_tools BETWEEN 0 AND 1000 AND execution_timeout_seconds BETWEEN 0 AND 86400);

CREATE FUNCTION public.hydrate_execution_budget() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' AS $$
DECLARE budget public.agent_app_revision%ROWTYPE;
BEGIN
    SELECT * INTO budget FROM public.agent_app_revision WHERE tenant_id=NEW.tenant_id AND agent_app_id=NEW.agent_app_id AND revision=NEW.agent_app_revision AND state='published' AND content_digest=NEW.agent_content_digest;
    IF NOT FOUND THEN RAISE EXCEPTION 'published revision budget not found' USING ERRCODE='40001'; END IF;
    NEW.max_llm_calls := budget.max_llm_calls; NEW.max_tool_calls := budget.max_tool_calls;
    NEW.max_parallel_tools := budget.max_parallel_tools; NEW.execution_timeout_seconds := budget.execution_timeout_seconds;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_record_hydrate_execution_budget BEFORE INSERT ON public.execution_record FOR EACH ROW EXECUTE FUNCTION public.hydrate_execution_budget();
COMMIT;
