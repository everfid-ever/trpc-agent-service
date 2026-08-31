BEGIN;
DROP TRIGGER IF EXISTS audit_event_immutable ON public.audit_event;
DROP FUNCTION IF EXISTS public.reject_audit_event_change();
DROP TABLE IF EXISTS public.audit_event;
COMMIT;
