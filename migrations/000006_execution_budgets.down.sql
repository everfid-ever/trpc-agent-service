BEGIN;
DROP TRIGGER execution_record_hydrate_execution_budget ON public.execution_record;
DROP FUNCTION public.hydrate_execution_budget();
ALTER TABLE public.execution_record DROP CONSTRAINT execution_record_execution_budget_check, DROP COLUMN execution_timeout_seconds, DROP COLUMN max_parallel_tools, DROP COLUMN max_tool_calls, DROP COLUMN max_llm_calls;
ALTER TABLE public.agent_app_revision DROP CONSTRAINT agent_app_revision_execution_budget_check, DROP COLUMN execution_timeout_seconds, DROP COLUMN max_parallel_tools, DROP COLUMN max_tool_calls, DROP COLUMN max_llm_calls;
COMMIT;
