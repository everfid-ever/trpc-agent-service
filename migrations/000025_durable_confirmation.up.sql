BEGIN;

CREATE TABLE public.confirmation (
  tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
  confirmation_id text NOT NULL,
  request_id text NOT NULL,
  request_digest text NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  agent_app_id text NOT NULL,
  session_id text NOT NULL,
  input_seq bigint NOT NULL CHECK (input_seq >= 1),
  subject_id text NOT NULL,
  channel_binding_id text NOT NULL,
  tool_id text NOT NULL,
  tool_version bigint NOT NULL CHECK (tool_version >= 1),
  tool_call_id text NOT NULL,
  args_digest text NOT NULL CHECK (args_digest ~ '^[0-9a-f]{64}$'),
  policy_version bigint NOT NULL,
  checkpoint_ref text NOT NULL,
  input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
  output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
  cached_input_tokens bigint NOT NULL DEFAULT 0 CHECK (cached_input_tokens >= 0 AND cached_input_tokens <= input_tokens),
  state text NOT NULL CHECK (state IN ('pending','approved','denied','expired','consumed')),
  expires_at timestamptz NOT NULL,
  decision_at timestamptz,
  version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,confirmation_id),
  UNIQUE (tenant_id,request_id,tool_call_id),
  FOREIGN KEY (tenant_id,request_id) REFERENCES public.execution_record(tenant_id,request_id),
  FOREIGN KEY (tenant_id,policy_version) REFERENCES public.policy_snapshot(tenant_id,policy_version),
  CHECK ((state='pending' AND decision_at IS NULL) OR (state<>'pending' AND decision_at IS NOT NULL))
);

CREATE TABLE public.confirmation_grant (
  tenant_id text NOT NULL,
  grant_id text NOT NULL,
  confirmation_id text NOT NULL,
  request_id text NOT NULL,
  subject_id text NOT NULL,
  tool_id text NOT NULL,
  tool_version bigint NOT NULL CHECK (tool_version >= 1),
  tool_call_id text NOT NULL,
  args_digest text NOT NULL CHECK (args_digest ~ '^[0-9a-f]{64}$'),
  policy_version bigint NOT NULL,
  state text NOT NULL CHECK (state IN ('available','consumed')),
  version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
  consumed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,grant_id),
  UNIQUE (tenant_id,confirmation_id),
  FOREIGN KEY (tenant_id,confirmation_id) REFERENCES public.confirmation(tenant_id,confirmation_id),
  CHECK ((state='available' AND consumed_at IS NULL) OR (state='consumed' AND consumed_at IS NOT NULL))
);

CREATE TABLE public.tool_attempt (
  tenant_id text NOT NULL,
  grant_id text NOT NULL,
  request_id text NOT NULL,
  tool_call_id text NOT NULL,
  state text NOT NULL CHECK (state IN ('effect_unknown','succeeded','failed')),
  result_ref text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,grant_id),
  UNIQUE (tenant_id,request_id,tool_call_id),
  FOREIGN KEY (tenant_id,grant_id) REFERENCES public.confirmation_grant(tenant_id,grant_id)
);

CREATE TABLE public.tool_result_payload (
  tenant_id text NOT NULL,
  grant_id text NOT NULL,
  request_id text NOT NULL,
  result_ref text NOT NULL,
  result_ciphertext bytea NOT NULL,
  result_nonce bytea NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  key_version bigint NOT NULL CHECK (key_version >= 1),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,grant_id),
  UNIQUE (tenant_id,result_ref),
  FOREIGN KEY (tenant_id,grant_id) REFERENCES public.tool_attempt(tenant_id,grant_id),
  FOREIGN KEY (tenant_id,request_id) REFERENCES public.execution_record(tenant_id,request_id)
);

CREATE TABLE public.interaction_payload (
  tenant_id text NOT NULL,
  request_id text NOT NULL,
  content_ref text NOT NULL,
  content_ciphertext bytea NOT NULL,
  content_nonce bytea NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  key_version bigint NOT NULL CHECK (key_version >= 1),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,request_id,content_ref),
  FOREIGN KEY (tenant_id,request_id) REFERENCES public.execution_record(tenant_id,request_id)
);

CREATE FUNCTION public.suspend_turn(
  p_tenant_id text, p_agent_app_id text, p_session_id text,
  p_request_id text, p_commit_id text, p_request_digest text,
  p_input_seq bigint, p_fence bigint, p_expected_version bigint,
  p_events jsonb, p_state_delta jsonb,
  p_confirmation_id text, p_subject_id text, p_channel_binding_id text,
  p_tool_id text, p_tool_version bigint, p_tool_call_id text,
  p_args_digest text, p_policy_version bigint, p_checkpoint_ref text,
  p_input_tokens bigint,p_output_tokens bigint,p_cached_input_tokens bigint,
  p_expires_at timestamptz, p_outbox jsonb
) RETURNS TABLE(confirmation_id text,state text,version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_commit record; c public.confirmation%ROWTYPE;
BEGIN
  IF p_expires_at <= now() THEN RAISE EXCEPTION 'confirmation already expired' USING ERRCODE='22023'; END IF;
  SELECT * INTO v_commit FROM public.commit_turn(p_tenant_id,p_agent_app_id,p_session_id,
    p_request_id,p_commit_id,p_request_digest,'waiting',p_input_seq,p_fence,p_expected_version,
    'waiting_confirmation',p_events,p_state_delta,'null'::jsonb,p_checkpoint_ref,NULL,p_outbox);
  INSERT INTO public.confirmation(tenant_id,confirmation_id,request_id,request_digest,agent_app_id,session_id,input_seq,
    subject_id,channel_binding_id,tool_id,tool_version,tool_call_id,args_digest,policy_version,checkpoint_ref,
    input_tokens,output_tokens,cached_input_tokens,state,expires_at)
  VALUES(p_tenant_id,p_confirmation_id,p_request_id,p_request_digest,p_agent_app_id,p_session_id,p_input_seq,p_subject_id,
    p_channel_binding_id,p_tool_id,p_tool_version,p_tool_call_id,p_args_digest,p_policy_version,p_checkpoint_ref,
    p_input_tokens,p_output_tokens,p_cached_input_tokens,'pending',p_expires_at)
  ON CONFLICT ON CONSTRAINT confirmation_pkey DO NOTHING;
  SELECT x.* INTO c FROM public.confirmation x WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
  IF c.request_id<>p_request_id OR c.request_digest<>p_request_digest OR c.agent_app_id<>p_agent_app_id
     OR c.session_id<>p_session_id OR c.input_seq<>p_input_seq OR c.subject_id<>p_subject_id
     OR c.channel_binding_id<>p_channel_binding_id OR c.tool_id<>p_tool_id OR c.tool_version<>p_tool_version
     OR c.tool_call_id<>p_tool_call_id OR c.args_digest<>p_args_digest OR c.policy_version<>p_policy_version
     OR c.checkpoint_ref<>p_checkpoint_ref OR c.input_tokens<>p_input_tokens OR c.output_tokens<>p_output_tokens
     OR c.cached_input_tokens<>p_cached_input_tokens OR c.expires_at<>p_expires_at THEN
    RAISE EXCEPTION 'confirmation id collision' USING ERRCODE='23505';
  END IF;
  RETURN QUERY SELECT x.confirmation_id,x.state,x.version FROM public.confirmation x
    WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
END;
$$;

CREATE FUNCTION public.decide_confirmation(
  p_tenant_id text,p_confirmation_id text,p_subject_id text,p_approve boolean,
  p_expected_version bigint,p_decided_at timestamptz
) RETURNS TABLE(confirmation_id text,state text,version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE c public.confirmation%ROWTYPE; v_grant_id text; v_cancel_requested boolean; v_outcome text;
BEGIN
  SELECT x.* INTO c FROM public.confirmation x WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'confirmation not found' USING ERRCODE='P0002'; END IF;
  IF c.subject_id<>p_subject_id THEN RAISE EXCEPTION 'confirmation subject mismatch' USING ERRCODE='42501'; END IF;
  IF c.state<>'pending' THEN RETURN QUERY SELECT c.confirmation_id,c.state,c.version; RETURN; END IF;
  IF c.version<>p_expected_version THEN RAISE EXCEPTION 'confirmation version conflict' USING ERRCODE='40001'; END IF;
  SELECT e.cancel_requested_at IS NOT NULL,e.outcome INTO v_cancel_requested,v_outcome FROM public.execution_record e
    WHERE e.tenant_id=p_tenant_id AND e.request_id=c.request_id FOR UPDATE;
  IF v_cancel_requested OR v_outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN
    UPDATE public.confirmation x SET state='denied',decision_at=p_decided_at,version=x.version+1,updated_at=now()
      WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
  ELSIF c.expires_at<=p_decided_at THEN
    UPDATE public.confirmation x SET state='expired',decision_at=p_decided_at,version=x.version+1,updated_at=now()
      WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
  ELSIF p_approve THEN
    UPDATE public.confirmation x SET state='approved',decision_at=p_decided_at,version=x.version+1,updated_at=now()
      WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
    v_grant_id := 'grant_'||substr(p_confirmation_id,6);
    INSERT INTO public.confirmation_grant(tenant_id,grant_id,confirmation_id,request_id,subject_id,tool_id,tool_version,tool_call_id,args_digest,policy_version,state)
      VALUES(p_tenant_id,v_grant_id,p_confirmation_id,c.request_id,c.subject_id,c.tool_id,c.tool_version,c.tool_call_id,c.args_digest,c.policy_version,'available');
  ELSE
    UPDATE public.confirmation x SET state='denied',decision_at=p_decided_at,version=x.version+1,updated_at=now()
      WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
  END IF;
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
    VALUES(p_tenant_id,'confirmation-dispatch:'||p_confirmation_id,'dispatch',c.request_id,2,
      'confirmation:'||p_confirmation_id||':dispatch','execution://'||p_tenant_id||'/'||c.request_id)
    ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING;
  RETURN QUERY SELECT x.confirmation_id,x.state,x.version FROM public.confirmation x
    WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
END;
$$;

CREATE FUNCTION public.expire_confirmations(p_now timestamptz,p_limit integer)
RETURNS TABLE(tenant_id text,confirmation_id text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_limit < 1 OR p_limit > 1000 THEN RAISE EXCEPTION 'invalid expiry batch size' USING ERRCODE='22023'; END IF;
  RETURN QUERY
  WITH due AS (
    SELECT c.tenant_id,c.confirmation_id,c.request_id
    FROM public.confirmation c
    WHERE c.state='pending' AND c.expires_at<=p_now
    ORDER BY c.expires_at,c.tenant_id,c.confirmation_id
    FOR UPDATE SKIP LOCKED LIMIT p_limit
  ), expired AS (
    UPDATE public.confirmation c SET state='expired',decision_at=p_now,version=c.version+1,updated_at=now()
    FROM due d WHERE c.tenant_id=d.tenant_id AND c.confirmation_id=d.confirmation_id
    RETURNING c.tenant_id,c.confirmation_id,c.request_id
  ), emitted AS (
    INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
    SELECT e.tenant_id,'confirmation-dispatch:'||e.confirmation_id,'dispatch',e.request_id,2,
      'confirmation:'||e.confirmation_id||':dispatch','execution://'||e.tenant_id||'/'||e.request_id FROM expired e
    ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING RETURNING 1
  ) SELECT e.tenant_id,e.confirmation_id FROM expired e;
END;
$$;

REVOKE ALL ON FUNCTION public.suspend_turn(text,text,text,text,text,text,bigint,bigint,bigint,jsonb,jsonb,text,text,text,text,bigint,text,text,bigint,text,bigint,bigint,bigint,timestamptz,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.decide_confirmation(text,text,text,boolean,bigint,timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.expire_confirmations(timestamptz,integer) FROM PUBLIC;

COMMIT;
