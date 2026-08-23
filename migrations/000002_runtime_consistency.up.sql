BEGIN;

CREATE TABLE session_head (
  tenant_id                text        NOT NULL,
  agent_app_id              text        NOT NULL,
  session_id                text        NOT NULL,
  version                   bigint      NOT NULL DEFAULT 0 CHECK (version >= 0),
  last_fence                bigint      NOT NULL DEFAULT 0 CHECK (last_fence >= 0),
  last_session_seq          bigint      NOT NULL DEFAULT 0 CHECK (last_session_seq >= 0),
  next_input_seq            bigint      NOT NULL DEFAULT 1 CHECK (next_input_seq >= 1),
  last_allocated_input_seq  bigint      NOT NULL DEFAULT 0 CHECK (last_allocated_input_seq >= 0),
  state_json                jsonb       NOT NULL DEFAULT '{}'::jsonb,
  summary_id                text,
  created_at                timestamptz NOT NULL DEFAULT now(),
  updated_at                timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_app_id, session_id),
  FOREIGN KEY (tenant_id, agent_app_id)
    REFERENCES agent_app(tenant_id, agent_app_id)
);

CREATE TABLE session_event (
  tenant_id text NOT NULL, agent_app_id text NOT NULL, session_id text NOT NULL,
  session_seq bigint NOT NULL CHECK (session_seq >= 1),
  request_id text NOT NULL, input_seq bigint NOT NULL CHECK (input_seq >= 1),
  event_seq bigint NOT NULL CHECK (event_seq >= 1), event_id text NOT NULL,
  event_type text NOT NULL, payload_ref text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_app_id, session_id, session_seq),
  UNIQUE (tenant_id, request_id, event_seq),
  UNIQUE (tenant_id, agent_app_id, session_id, event_id),
  FOREIGN KEY (tenant_id, agent_app_id, session_id)
    REFERENCES session_head(tenant_id, agent_app_id, session_id)
);

CREATE TABLE session_commit (
  tenant_id text NOT NULL, agent_app_id text NOT NULL, session_id text NOT NULL,
  commit_id text NOT NULL, request_id text NOT NULL,
  request_digest text NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  input_seq bigint NOT NULL CHECK (input_seq >= 1), stage text NOT NULL,
  outcome text NOT NULL, fence bigint NOT NULL CHECK (fence >= 1),
  session_version bigint NOT NULL CHECK (session_version >= 1),
  reply_cursor text, result_ref text, created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_app_id, session_id, commit_id),
  UNIQUE (tenant_id, agent_app_id, session_id, input_seq, stage),
  FOREIGN KEY (tenant_id, agent_app_id, session_id)
    REFERENCES session_head(tenant_id, agent_app_id, session_id),
  CHECK (outcome IN ('pending', 'queued', 'running', 'waiting_confirmation',
    'succeeded', 'denied', 'failed', 'cancelled',
    'confirmation_denied', 'confirmation_timeout'))
);
CREATE UNIQUE INDEX session_commit_terminal_input_idx
  ON session_commit(tenant_id, agent_app_id, session_id, input_seq)
  WHERE outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout');

CREATE TABLE session_summary (
  tenant_id text NOT NULL, agent_app_id text NOT NULL, session_id text NOT NULL,
  summary_id text NOT NULL, base_session_seq bigint NOT NULL CHECK (base_session_seq >= 1),
  last_event_id text NOT NULL, cutoff_at timestamptz NOT NULL,
  content_ref text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_app_id, session_id, summary_id),
  UNIQUE (tenant_id, agent_app_id, session_id, base_session_seq),
  FOREIGN KEY (tenant_id, agent_app_id, session_id)
    REFERENCES session_head(tenant_id, agent_app_id, session_id)
);

CREATE TABLE inbox (
  tenant_id text NOT NULL, channel text NOT NULL, external_account_id text NOT NULL,
  external_message_id text NOT NULL, request_id text NOT NULL,
  agent_app_id text NOT NULL, session_id text,
  input_seq bigint CHECK (input_seq IS NULL OR input_seq >= 1),
  state text NOT NULL, payload_ref text NOT NULL, payload_digest text NOT NULL,
  terminal_reason text, result_ref text,
  key_version bigint NOT NULL CHECK (key_version >= 1),
  version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, channel, external_account_id, external_message_id),
  UNIQUE (tenant_id, request_id),
  FOREIGN KEY (tenant_id, agent_app_id)
    REFERENCES agent_app(tenant_id, agent_app_id),
  CHECK (state IN ('preprocess_pending', 'dispatch_pending', 'dispatch_ready', 'terminal')),
  CHECK (payload_digest ~ '^[0-9a-f]{64}$')
);

CREATE TABLE execution_record (
  tenant_id text NOT NULL, request_id text NOT NULL, tenant_version bigint NOT NULL,
  agent_app_id text NOT NULL, agent_app_version bigint NOT NULL,
  agent_app_revision bigint NOT NULL, agent_content_digest text NOT NULL,
  config_version bigint NOT NULL, policy_version bigint NOT NULL,
  session_id text NOT NULL, user_id text NOT NULL, channel text NOT NULL,
  input_seq bigint NOT NULL CHECK (input_seq >= 1), payload_ref text NOT NULL,
  traceparent text, outcome text NOT NULL DEFAULT 'queued', result_ref text,
  park_attempt integer NOT NULL DEFAULT 0 CHECK (park_attempt >= 0), not_before timestamptz,
  version bigint NOT NULL DEFAULT 0 CHECK (version >= 0), created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, request_id),
  UNIQUE (tenant_id, agent_app_id, session_id, input_seq),
  FOREIGN KEY (tenant_id, request_id) REFERENCES inbox(tenant_id, request_id),
  FOREIGN KEY (tenant_id, agent_app_id, agent_app_revision)
    REFERENCES agent_app_revision(tenant_id, agent_app_id, revision),
  FOREIGN KEY (tenant_id, config_version)
    REFERENCES config_snapshot(tenant_id, config_version),
  CHECK (agent_content_digest ~ '^[0-9a-f]{64}$'),
  CHECK (outcome IN ('queued', 'running', 'pending', 'succeeded', 'denied', 'failed',
    'cancelled', 'confirmation_denied', 'confirmation_timeout'))
);

CREATE TABLE delivery_ledger (
  tenant_id text NOT NULL, delivery_key text NOT NULL,
  segment_no integer NOT NULL CHECK (segment_no >= 0),
  provider_message_id text, state text NOT NULL,
  version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, delivery_key, segment_no),
  CHECK (state IN ('pending', 'sent', 'confirmed', 'failed'))
);

CREATE OR REPLACE FUNCTION claim_inbox(
  p_tenant_id text, p_channel text, p_external_account_id text,
  p_external_message_id text, p_request_id text, p_agent_app_id text,
  p_session_id text, p_payload_ref text, p_payload_digest text,
  p_key_version bigint, p_initial_state text
) RETURNS SETOF inbox LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_row public.inbox%ROWTYPE;
BEGIN
  IF p_initial_state NOT IN ('preprocess_pending', 'dispatch_pending')
     OR p_payload_digest !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'invalid inbox claim' USING ERRCODE = '22023';
  END IF;
  INSERT INTO public.inbox(
    tenant_id, channel, external_account_id, external_message_id, request_id,
    agent_app_id, session_id, state, payload_ref, payload_digest, key_version
  ) VALUES (
    p_tenant_id, p_channel, p_external_account_id, p_external_message_id,
    p_request_id, p_agent_app_id, p_session_id, p_initial_state,
    p_payload_ref, p_payload_digest, p_key_version
  ) ON CONFLICT (tenant_id, channel, external_account_id, external_message_id) DO NOTHING;
  SELECT * INTO v_row FROM public.inbox
    WHERE tenant_id = p_tenant_id AND channel = p_channel
      AND external_account_id = p_external_account_id
      AND external_message_id = p_external_message_id FOR UPDATE;
  IF v_row.payload_digest <> p_payload_digest OR v_row.payload_ref <> p_payload_ref
     OR v_row.agent_app_id <> p_agent_app_id
     OR v_row.session_id IS DISTINCT FROM p_session_id THEN
    RAISE EXCEPTION 'inbox idempotency collision' USING ERRCODE = '23505';
  END IF;
  RETURN NEXT v_row;
END;
$$;

CREATE OR REPLACE FUNCTION prepare_dispatch(
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
  SELECT status, version, active_config_version
    INTO v_tenant_status, v_tenant_version, v_active_config
    FROM public.tenant WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant not found' USING ERRCODE = 'P0002'; END IF;
  SELECT * INTO v_inbox FROM public.inbox
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'inbox not found' USING ERRCODE = 'P0002'; END IF;
  IF v_inbox.state = 'dispatch_ready' THEN
    SELECT e.input_seq INTO v_input FROM public.execution_record e
      WHERE e.tenant_id = p_tenant_id AND e.request_id = p_request_id
        AND e.agent_app_id = p_agent_app_id AND e.session_id = p_session_id
        AND e.payload_ref = p_payload_ref AND e.agent_app_version = p_expected_app_version
        AND e.agent_app_revision = p_agent_app_revision AND e.agent_content_digest = p_agent_content_digest
        AND e.config_version = p_config_version AND e.policy_version = p_policy_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'dispatch idempotency collision' USING ERRCODE = '23505'; END IF;
    RETURN QUERY SELECT v_input, true, NULL::text;
    RETURN;
  END IF;
  IF v_inbox.state = 'terminal' THEN
    RETURN QUERY SELECT NULL::bigint, false, v_inbox.terminal_reason;
    RETURN;
  END IF;
  IF v_inbox.state NOT IN ('preprocess_pending', 'dispatch_pending')
     OR v_inbox.agent_app_id <> p_agent_app_id OR v_inbox.session_id IS DISTINCT FROM p_session_id
     OR v_inbox.payload_ref <> p_payload_ref OR v_inbox.channel <> p_channel THEN
    RAISE EXCEPTION 'inbox scope or state mismatch' USING ERRCODE = '42501';
  END IF;
  IF v_tenant_status <> 'active' THEN
    UPDATE public.inbox SET state = 'terminal', terminal_reason = v_tenant_status,
      version = version + 1, updated_at = now()
      WHERE tenant_id = p_tenant_id AND request_id = p_request_id;
    INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
      VALUES
      (p_tenant_id, format('dispatch-denied-reply:%s', p_request_id), 'reply', p_request_id, 1,
       format('dispatch-denied:%s:reply', p_request_id), format('inbox://%s/%s', p_tenant_id, p_request_id), p_traceparent),
      (p_tenant_id, format('dispatch-denied-audit:%s', p_request_id), 'audit', p_request_id, 1,
       format('dispatch-denied:%s:audit', p_request_id), format('inbox://%s/%s', p_tenant_id, p_request_id), p_traceparent)
      ON CONFLICT (tenant_id, kind, idempotency_key) DO NOTHING;
    RETURN QUERY SELECT NULL::bigint, false, v_tenant_status;
    RETURN;
  END IF;
  IF v_tenant_version <> p_expected_tenant_version OR v_active_config <> p_config_version THEN
    RAISE EXCEPTION 'tenant or config version conflict' USING ERRCODE = '40001';
  END IF;
  SELECT status, version, current_revision INTO v_app_status, v_app_version, v_current_revision
    FROM public.agent_app WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id FOR UPDATE;
  IF NOT FOUND OR v_app_status <> 'active' OR v_app_version <> p_expected_app_version
     OR v_current_revision <> p_agent_app_revision THEN
    RAISE EXCEPTION 'agent app binding changed' USING ERRCODE = '40001';
  END IF;
  SELECT content_digest INTO v_revision_digest FROM public.agent_app_revision
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id
      AND revision = p_agent_app_revision AND state = 'published';
  IF NOT FOUND OR v_revision_digest <> p_agent_content_digest THEN
    RAISE EXCEPTION 'agent revision binding changed' USING ERRCODE = '40001';
  END IF;
  PERFORM 1 FROM public.config_snapshot WHERE tenant_id = p_tenant_id
    AND config_version = p_config_version AND state = 'published'
    AND (payload->>'policy_version')::bigint = p_policy_version;
  IF NOT FOUND THEN RAISE EXCEPTION 'policy binding changed' USING ERRCODE = '40001'; END IF;
  INSERT INTO public.session_head(tenant_id, agent_app_id, session_id)
    VALUES (p_tenant_id, p_agent_app_id, p_session_id) ON CONFLICT DO NOTHING;
  SELECT last_allocated_input_seq + 1 INTO v_input FROM public.session_head
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id
      AND session_id = p_session_id FOR UPDATE;
  UPDATE public.session_head SET last_allocated_input_seq = v_input, updated_at = now()
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id AND session_id = p_session_id;
  INSERT INTO public.execution_record(
    tenant_id, request_id, tenant_version, agent_app_id, agent_app_version,
    agent_app_revision, agent_content_digest, config_version, policy_version,
    session_id, user_id, channel, input_seq, payload_ref, traceparent
  ) VALUES (
    p_tenant_id, p_request_id, v_tenant_version, p_agent_app_id, v_app_version,
    p_agent_app_revision, p_agent_content_digest, p_config_version, p_policy_version,
    p_session_id, p_user_id, p_channel, v_input, p_payload_ref, p_traceparent
  );
  UPDATE public.inbox SET state = 'dispatch_ready', input_seq = v_input,
    version = version + 1, updated_at = now()
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id;
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
    VALUES (p_tenant_id, format('dispatch:%s', p_request_id), 'dispatch', p_request_id, 1,
      format('dispatch:%s', p_request_id), format('execution://%s/%s', p_tenant_id, p_request_id), p_traceparent);
  RETURN QUERY SELECT v_input, true, NULL::text;
END;
$$;

CREATE OR REPLACE FUNCTION commit_turn(
  p_tenant_id text, p_agent_app_id text, p_session_id text,
  p_request_id text, p_commit_id text, p_request_digest text, p_stage text,
  p_input_seq bigint, p_fence bigint, p_expected_version bigint, p_outcome text,
  p_events jsonb, p_state_delta jsonb, p_summary jsonb,
  p_result_ref text, p_reply_cursor text, p_outbox jsonb
) RETURNS TABLE(commit_id text, outcome text, input_seq bigint, session_version bigint, result_ref text, reply_cursor text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  v_head public.session_head%ROWTYPE; v_existing public.session_commit%ROWTYPE;
  v_terminal public.session_commit%ROWTYPE; v_event jsonb; v_out jsonb;
  v_ordinal bigint; v_new_version bigint; v_new_last_seq bigint;
BEGIN
  SELECT * INTO v_head FROM public.session_head WHERE tenant_id = p_tenant_id
    AND agent_app_id = p_agent_app_id AND session_id = p_session_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'session not found' USING ERRCODE = 'P0002'; END IF;
  SELECT * INTO v_existing FROM public.session_commit WHERE tenant_id = p_tenant_id
    AND agent_app_id = p_agent_app_id AND session_id = p_session_id AND session_commit.commit_id = p_commit_id;
  IF FOUND THEN
    IF v_existing.request_digest <> p_request_digest THEN
      RAISE EXCEPTION 'commit id collision' USING ERRCODE = '23505';
    END IF;
    RETURN QUERY SELECT v_existing.commit_id, v_existing.outcome, v_existing.input_seq,
      v_existing.session_version, v_existing.result_ref, v_existing.reply_cursor;
    RETURN;
  END IF;
  IF p_input_seq < v_head.next_input_seq THEN
    SELECT * INTO v_terminal FROM public.session_commit WHERE tenant_id = p_tenant_id
      AND agent_app_id = p_agent_app_id AND session_id = p_session_id AND session_commit.input_seq = p_input_seq
      AND session_commit.outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout');
    IF NOT FOUND THEN RAISE EXCEPTION 'terminal invariant missing' USING ERRCODE = 'XX001'; END IF;
    RETURN QUERY SELECT v_terminal.commit_id, v_terminal.outcome, v_terminal.input_seq,
      v_terminal.session_version, v_terminal.result_ref, v_terminal.reply_cursor;
    RETURN;
  END IF;
  IF p_input_seq > v_head.next_input_seq THEN RAISE EXCEPTION 'input not ready' USING ERRCODE = '55000'; END IF;
  IF p_expected_version <> v_head.version THEN RAISE EXCEPTION 'session version conflict' USING ERRCODE = '40001'; END IF;
  IF p_fence < v_head.last_fence THEN RAISE EXCEPTION 'stale fence' USING ERRCODE = '40001'; END IF;
  v_new_version := v_head.version + 1;
  v_new_last_seq := v_head.last_session_seq + jsonb_array_length(COALESCE(p_events, '[]'::jsonb));
  FOR v_event, v_ordinal IN SELECT value, ordinality FROM jsonb_array_elements(COALESCE(p_events, '[]'::jsonb)) WITH ORDINALITY LOOP
    INSERT INTO public.session_event(tenant_id, agent_app_id, session_id, session_seq,
      request_id, input_seq, event_seq, event_id, event_type, payload_ref)
    VALUES (p_tenant_id, p_agent_app_id, p_session_id, v_head.last_session_seq + v_ordinal,
      p_request_id, p_input_seq, (v_event->>'event_seq')::bigint, v_event->>'event_id', v_event->>'event_type', v_event->>'payload_ref');
  END LOOP;
  UPDATE public.session_head SET version = v_new_version,
    last_fence = GREATEST(last_fence, p_fence), last_session_seq = v_new_last_seq,
    next_input_seq = next_input_seq + CASE WHEN p_outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN 1 ELSE 0 END,
    state_json = state_json || COALESCE(p_state_delta, '{}'::jsonb), updated_at = now()
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id AND session_id = p_session_id;
  IF p_summary IS NOT NULL AND p_summary <> 'null'::jsonb THEN
    INSERT INTO public.session_summary(tenant_id, agent_app_id, session_id, summary_id,
      base_session_seq, last_event_id, cutoff_at, content_ref)
    VALUES (p_tenant_id, p_agent_app_id, p_session_id, p_summary->>'summary_id',
      (p_summary->>'base_session_seq')::bigint, p_summary->>'last_event_id',
      (p_summary->>'cutoff_at')::timestamptz, p_summary->>'content_ref')
    ON CONFLICT (tenant_id, agent_app_id, session_id, base_session_seq) DO NOTHING;
    UPDATE public.session_head SET summary_id = p_summary->>'summary_id'
      WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id AND session_id = p_session_id
        AND (summary_id IS NULL OR (p_summary->>'base_session_seq')::bigint > COALESCE((SELECT base_session_seq FROM public.session_summary s WHERE s.tenant_id = p_tenant_id AND s.agent_app_id = p_agent_app_id AND s.session_id = p_session_id AND s.summary_id = session_head.summary_id), 0));
  END IF;
  INSERT INTO public.session_commit(tenant_id, agent_app_id, session_id, commit_id,
    request_id, request_digest, input_seq, stage, outcome, fence, session_version, reply_cursor, result_ref)
  VALUES (p_tenant_id, p_agent_app_id, p_session_id, p_commit_id, p_request_id,
    p_request_digest, p_input_seq, p_stage, p_outcome, p_fence, v_new_version, p_reply_cursor, p_result_ref);
  FOR v_out IN SELECT value FROM jsonb_array_elements(COALESCE(p_outbox, '[]'::jsonb)) LOOP
    INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq,
      idempotency_key, payload_ref, traceparent)
    VALUES (p_tenant_id, format('%s:%s', v_out->>'kind', v_out->>'idempotency_key'),
      v_out->>'kind', p_request_id, (v_out->>'event_seq')::bigint,
      v_out->>'idempotency_key', v_out->>'payload_ref', v_out->>'traceparent');
  END LOOP;
  RETURN QUERY SELECT p_commit_id, p_outcome, p_input_seq, v_new_version, p_result_ref, p_reply_cursor;
END;
$$;

CREATE OR REPLACE FUNCTION request_cancel_execution(
  p_tenant_id text, p_request_id text, p_expected_version bigint, p_traceparent text
) RETURNS TABLE(accepted boolean, execution_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_outcome text; v_version bigint;
BEGIN
  PERFORM 1 FROM public.tenant WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant not found' USING ERRCODE = 'P0002'; END IF;
  SELECT outcome, version INTO v_outcome, v_version FROM public.execution_record
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'execution not found' USING ERRCODE = 'P0002'; END IF;
  IF v_outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN
    RETURN QUERY SELECT false, v_version;
    RETURN;
  END IF;
  IF v_version <> p_expected_version THEN RAISE EXCEPTION 'execution version conflict' USING ERRCODE = '40001'; END IF;
  UPDATE public.execution_record SET outcome = 'cancelled', version = version + 1
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id;
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent)
    VALUES
      (p_tenant_id,format('cancel-reply:%s',p_request_id),'reply',p_request_id,1,
       format('cancel:%s:reply',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id),p_traceparent),
      (p_tenant_id,format('cancel-audit:%s',p_request_id),'audit',p_request_id,1,
       format('cancel:%s:audit',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id),p_traceparent);
  RETURN QUERY SELECT true, v_version + 1;
END;
$$;

CREATE OR REPLACE FUNCTION park_execution(
  p_tenant_id text, p_request_id text, p_input_seq bigint, p_attempt integer
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_input_seq bigint; v_outcome text;
BEGIN
  IF p_attempt < 1 THEN RAISE EXCEPTION 'invalid park attempt' USING ERRCODE = '22023'; END IF;
  SELECT input_seq, outcome INTO v_input_seq, v_outcome FROM public.execution_record
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'execution not found' USING ERRCODE = 'P0002'; END IF;
  IF v_input_seq <> p_input_seq OR v_outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN
    RAISE EXCEPTION 'execution cannot be parked' USING ERRCODE = '55000';
  END IF;
  UPDATE public.execution_record SET outcome = 'pending', park_attempt = GREATEST(park_attempt,p_attempt),
    not_before = now() + make_interval(secs => LEAST(300, power(2,GREATEST(0,p_attempt-1))::integer)),
    version = version + 1
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id;
END;
$$;

REVOKE ALL ON FUNCTION claim_inbox(text,text,text,text,text,text,text,text,text,bigint,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION prepare_dispatch(text,bigint,text,bigint,bigint,text,bigint,bigint,text,text,text,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION commit_turn(text,text,text,text,text,text,text,bigint,bigint,bigint,text,jsonb,jsonb,jsonb,text,text,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION request_cancel_execution(text,text,bigint,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION park_execution(text,text,bigint,integer) FROM PUBLIC;

COMMIT;
