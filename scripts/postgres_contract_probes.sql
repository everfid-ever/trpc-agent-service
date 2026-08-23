\set ON_ERROR_STOP on

INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-a','Probe A'),
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW','probe-b','Probe B')
ON CONFLICT DO NOTHING;

INSERT INTO agent_app(tenant_id,agent_app_id,agent_app_key,display_name,status,current_revision,next_revision) VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-a','Probe A','draft',NULL,2),
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW','app_01ARZ3NDEKTSV4RRFFQ69G5FAW','probe-b','Probe B','draft',NULL,2)
ON CONFLICT DO NOTHING;

INSERT INTO session_head(tenant_id,agent_app_id,session_id) VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-session')
ON CONFLICT DO NOTHING;

DO $$
BEGIN
  BEGIN
    INSERT INTO session_event(tenant_id,agent_app_id,session_id,session_seq,request_id,input_seq,event_seq,event_id,event_type,payload_ref)
    VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW','app_01ARZ3NDEKTSV4RRFFQ69G5FAW','probe-session',1,'cross-tenant',1,1,'event','test','payload://probe');
    RAISE EXCEPTION 'cross-tenant session FK was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL;
  END;

  INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
  VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-outbox-1','dispatch','probe',1,'probe-duplicate','payload://probe')
  ON CONFLICT DO NOTHING;
  BEGIN
    INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
    VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-outbox-2','dispatch','probe',1,'probe-duplicate','payload://other');
    RAISE EXCEPTION 'duplicate outbox key was accepted';
  EXCEPTION WHEN unique_violation THEN NULL;
  END;

  BEGIN
    PERFORM * FROM commit_turn(
      't_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-session',
      'probe-request','probe-commit',repeat('a',64),'terminal',1,1,99,'succeeded',
      '[]'::jsonb,'{}'::jsonb,NULL,NULL,NULL,'[]'::jsonb);
    RAISE EXCEPTION 'stale session version was accepted';
  EXCEPTION WHEN serialization_failure THEN NULL;
  END;
END;
$$;
