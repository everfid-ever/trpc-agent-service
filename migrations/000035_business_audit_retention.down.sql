BEGIN;

DROP TRIGGER IF EXISTS business_audit_purge_certificate_immutable ON public.business_audit_purge_certificate;
DROP FUNCTION IF EXISTS public.reject_business_audit_purge_certificate_change();
DROP TRIGGER IF EXISTS business_audit_purge_batch_guard ON public.business_audit_purge_batch;
DROP FUNCTION IF EXISTS public.guard_business_audit_purge_batch_update();
DROP TABLE IF EXISTS public.business_audit_purge_certificate;
DROP TABLE IF EXISTS public.business_audit_purge_batch;

DROP FUNCTION IF EXISTS public.execute_business_audit_purge(text,text,text,bigint);
DROP FUNCTION IF EXISTS public.quarantine_business_audit_purge(text,text,text);
DROP FUNCTION IF EXISTS public.plan_business_audit_purge(text,timestamptz,text,text,timestamptz);
DROP FUNCTION IF EXISTS public.business_audit_watermark(text);

-- Restore the original 000026 immutable guard (UPDATE and DELETE both refused).
CREATE OR REPLACE FUNCTION public.reject_audit_event_change() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'audit event is immutable' USING ERRCODE='55000';
END;
$$;

DROP ROLE IF EXISTS audit_retention_purger;

COMMIT;
