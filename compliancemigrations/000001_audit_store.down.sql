BEGIN;

DROP TRIGGER IF EXISTS compliance_quarantine_alert_immutable ON compliance.quarantine_alert;
DROP TRIGGER IF EXISTS compliance_audit_event_immutable ON compliance.audit_event;
DROP FUNCTION IF EXISTS compliance.reject_immutable_change();
DROP TABLE IF EXISTS compliance.quarantine_alert;
DROP TABLE IF EXISTS compliance.audit_event;

COMMIT;
