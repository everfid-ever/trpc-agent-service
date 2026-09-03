BEGIN;

DROP TRIGGER IF EXISTS session_summary_content_guard ON session_summary_content;
DROP FUNCTION IF EXISTS public.guard_session_summary_content();
DROP TABLE IF EXISTS session_summary_content;

COMMIT;
