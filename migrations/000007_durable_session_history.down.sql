BEGIN;
DROP TRIGGER IF EXISTS agent_model_profile_publish_guard ON public.agent_app_revision;
DROP FUNCTION IF EXISTS public.guard_agent_model_profile_publish();
ALTER TABLE public.agent_app_revision DROP CONSTRAINT IF EXISTS agent_revision_model_profile_version_fk;
DROP TABLE IF EXISTS public.result_payload;
DROP TRIGGER IF EXISTS session_event_unpack_payload ON public.session_event;
DROP FUNCTION IF EXISTS public.unpack_session_event_payload();
ALTER TABLE public.session_event DROP COLUMN IF EXISTS event_payload;
COMMIT;
