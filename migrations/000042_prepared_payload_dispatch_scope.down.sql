BEGIN;

CREATE OR REPLACE FUNCTION public.prepare_dispatch(
  p_tenant_id text, p_expected_tenant_version bigint,
  p_agent_app_id text, p_expected_app_version bigint,
  p_agent_app_revision bigint, p_agent_content_digest text,
  p_config_version bigint, p_policy_version bigint,
  p_request_id text, p_session_id text, p_user_id text,
  p_channel text, p_payload_ref text, p_traceparent text
) RETURNS TABLE(input_seq bigint, accepted boolean, terminal_reason text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  v_tenant_status text; v_tenant_version bigint; v_active_config bigint;
  v_app_status text; v_app_version bigint; v_current_revision bigint;
  v_revision_digest text; v_inbox public.inbox%ROWTYPE; v_input bigint;
BEGIN
  SELECT status,version,active_config_version INTO v_tenant_status,v_tenant_version,v_active_config
    FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant not found' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_inbox FROM public.inbox WHERE tenant_id=p_tenant_id AND request_id=p_request_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'inbox not found' USING ERRCODE='P0002'; END IF;
  IF v_inbox.state='dispatch_ready' THEN
    SELECT e.input_seq INTO v_input FROM public.execution_record e
      WHERE e.tenant_id=p_tenant_id AND e.request_id=p_request_id
        AND e.agent_app_id=p_agent_app_id AND e.session_id=p_session_id
        AND e.payload_ref=p_payload_ref AND e.agent_app_version=p_expected_app_version
        AND e.agent_app_revision=p_agent_app_revision AND e.agent_content_digest=p_agent_content_digest
        AND e.config_version=p_config_version AND e.policy_version=p_policy_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'dispatch idempotency collision' USING ERRCODE='23505'; END IF;
    RETURN QUERY SELECT v_input,true,NULL::text; RETURN;
  END IF;
  IF v_inbox.state='terminal' THEN
    RETURN QUERY SELECT NULL::bigint,false,v_inbox.terminal_reason; RETURN;
  END IF;
  IF v_inbox.state NOT IN ('preprocess_pending','dispatch_pending')
     OR v_inbox.agent_app_id<>p_agent_app_id OR v_inbox.session_id IS DISTINCT FROM p_session_id
     OR v_inbox.payload_ref<>p_payload_ref OR v_inbox.channel<>p_channel THEN
    RAISE EXCEPTION 'inbox scope or state mismatch' USING ERRCODE='42501';
  END IF;
  IF v_tenant_status<>'active' THEN
    UPDATE public.inbox SET state='terminal',terminal_reason=v_tenant_status,version=version+1,updated_at=now()
      WHERE tenant_id=p_tenant_id AND request_id=p_request_id;
    INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent)
      VALUES (p_tenant_id,format('dispatch-denied-reply:%s',p_request_id),'reply',p_request_id,1,
        format('dispatch-denied:%s:reply',p_request_id),format('inbox://%s/%s',p_tenant_id,p_request_id),p_traceparent),
      (p_tenant_id,format('dispatch-denied-audit:%s',p_request_id),'audit',p_request_id,1,
        format('dispatch-denied:%s:audit',p_request_id),format('inbox://%s/%s',p_tenant_id,p_request_id),p_traceparent)
      ON CONFLICT (tenant_id,kind,idempotency_key) DO NOTHING;
    RETURN QUERY SELECT NULL::bigint,false,v_tenant_status; RETURN;
  END IF;
  IF v_tenant_version<>p_expected_tenant_version OR v_active_config<>p_config_version THEN
    RAISE EXCEPTION 'tenant or config version conflict' USING ERRCODE='40001';
  END IF;
  SELECT status,version,current_revision INTO v_app_status,v_app_version,v_current_revision
    FROM public.agent_app WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id FOR UPDATE;
  IF NOT FOUND OR v_app_status<>'active' OR v_app_version<>p_expected_app_version
     OR v_current_revision<>p_agent_app_revision THEN
    RAISE EXCEPTION 'agent app binding changed' USING ERRCODE='40001';
  END IF;
  SELECT content_digest INTO v_revision_digest FROM public.agent_app_revision
    WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id AND revision=p_agent_app_revision AND state='published';
  IF NOT FOUND OR v_revision_digest<>p_agent_content_digest THEN
    RAISE EXCEPTION 'agent revision binding changed' USING ERRCODE='40001';
  END IF;
  PERFORM 1 FROM public.config_snapshot WHERE tenant_id=p_tenant_id AND config_version=p_config_version
    AND state='published' AND (payload->>'policy_version')::bigint=p_policy_version;
  IF NOT FOUND THEN RAISE EXCEPTION 'policy binding changed' USING ERRCODE='40001'; END IF;
  INSERT INTO public.session_head(tenant_id,agent_app_id,session_id)
    VALUES(p_tenant_id,p_agent_app_id,p_session_id) ON CONFLICT DO NOTHING;
  SELECT last_allocated_input_seq+1 INTO v_input FROM public.session_head
    WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id AND session_id=p_session_id FOR UPDATE;
  UPDATE public.session_head SET last_allocated_input_seq=v_input,updated_at=now()
    WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id AND session_id=p_session_id;
  INSERT INTO public.execution_record(tenant_id,request_id,tenant_version,agent_app_id,agent_app_version,
    agent_app_revision,agent_content_digest,config_version,policy_version,session_id,user_id,channel,input_seq,payload_ref,traceparent)
    VALUES(p_tenant_id,p_request_id,v_tenant_version,p_agent_app_id,v_app_version,p_agent_app_revision,
      p_agent_content_digest,p_config_version,p_policy_version,p_session_id,p_user_id,p_channel,v_input,p_payload_ref,p_traceparent);
  UPDATE public.inbox SET state='dispatch_ready',input_seq=v_input,version=version+1,updated_at=now()
    WHERE tenant_id=p_tenant_id AND request_id=p_request_id;
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent)
    VALUES(p_tenant_id,format('dispatch:%s',p_request_id),'dispatch',p_request_id,1,
      format('dispatch:%s',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id),p_traceparent);
  RETURN QUERY SELECT v_input,true,NULL::text;
END;
$$;

REVOKE ALL ON FUNCTION public.prepare_dispatch(text,bigint,text,bigint,bigint,text,bigint,bigint,text,text,text,text,text,text) FROM PUBLIC;

COMMIT;
