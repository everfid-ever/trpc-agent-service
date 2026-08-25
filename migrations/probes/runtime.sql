INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-a','Probe A'),
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW','probe-b','Probe B')
ON CONFLICT DO NOTHING;

INSERT INTO model_profile(tenant_id,model_profile_id,profile_key,display_name,status,current_version)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','model','probe-model','Probe Model','active',NULL)
ON CONFLICT DO NOTHING;
INSERT INTO model_profile_revision(tenant_id,model_profile_id,profile_version,schema_version,provider,model_name,content_digest)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','model',1,1,'probe','probe',repeat('d',64))
ON CONFLICT DO NOTHING;
UPDATE model_profile SET current_version=1
WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND model_profile_id='model' AND current_version IS NULL;

INSERT INTO agent_app(tenant_id,agent_app_id,agent_app_key,display_name,status,current_revision,next_revision) VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-a','Probe A','disabled',NULL,2),
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW','app_01ARZ3NDEKTSV4RRFFQ69G5FAW','probe-b','Probe B','draft',NULL,2)
ON CONFLICT DO NOTHING;

INSERT INTO agent_app_revision(tenant_id,agent_app_id,revision,state,draft_version,agent_kind,schema_version,instruction,model_profile_id,model_profile_version,content_digest,published_at)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV',1,'published',1,'llm',1,'probe','model',1,repeat('a',64),now())
ON CONFLICT DO NOTHING;

UPDATE agent_app SET status='active',current_revision=1,version=version+1
WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV'
  AND agent_app_id='app_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND current_revision IS NULL;

INSERT INTO config_snapshot(tenant_id,config_version,schema_version,payload,content_digest,state,actor_id,reason_code,correlation_id,trace_id,published_at)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV',1,1,'{"policy_version":1}'::jsonb,repeat('b',64),'published','probe','probe','probe','probe',now())
ON CONFLICT DO NOTHING;

INSERT INTO session_head(tenant_id,agent_app_id,session_id) VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-session')
ON CONFLICT DO NOTHING;

INSERT INTO inbox(tenant_id,channel,external_account_id,external_message_id,request_id,agent_app_id,session_id,input_seq,state,payload_ref,payload_digest,key_version)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','fake','probe','probe-message','probe-request','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-session',1,'dispatch_ready','payload://probe',repeat('c',64),1)
ON CONFLICT DO NOTHING;

INSERT INTO execution_record(tenant_id,request_id,tenant_version,agent_app_id,agent_app_version,agent_app_revision,agent_content_digest,config_version,policy_version,session_id,user_id,channel,input_seq,payload_ref)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-request',1,'app_01ARZ3NDEKTSV4RRFFQ69G5FAV',2,1,repeat('a',64),1,1,'probe-session','probe-user','fake',1,'payload://probe')
ON CONFLICT DO NOTHING;

DO $$
BEGIN
  BEGIN
    INSERT INTO session_event(tenant_id,agent_app_id,session_id,session_seq,request_id,input_seq,event_seq,event_id,event_type,payload_ref)
    VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW','app_01ARZ3NDEKTSV4RRFFQ69G5FAW','probe-session',1,'cross-tenant',1,1,'event','test','{"ref":"payload://probe","payload":{}}');
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
