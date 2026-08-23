BEGIN;

DROP FUNCTION IF EXISTS park_execution(text,text,bigint,integer);
DROP FUNCTION IF EXISTS request_cancel_execution(text,text,bigint,text);
DROP FUNCTION IF EXISTS commit_turn(text,text,text,text,text,text,text,bigint,bigint,bigint,text,jsonb,jsonb,jsonb,text,text,jsonb);
DROP FUNCTION IF EXISTS prepare_dispatch(text,bigint,text,bigint,bigint,text,bigint,bigint,text,text,text,text,text,text);
DROP FUNCTION IF EXISTS claim_inbox(text,text,text,text,text,text,text,text,text,bigint,text);
DROP TABLE IF EXISTS delivery_ledger;
DROP TABLE IF EXISTS execution_record;
DROP TABLE IF EXISTS inbox;
DROP TABLE IF EXISTS session_summary;
DROP TABLE IF EXISTS session_commit;
DROP TABLE IF EXISTS session_event;
DROP TABLE IF EXISTS session_head;

COMMIT;
