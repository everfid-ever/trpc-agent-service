BEGIN;

ALTER TABLE public.inbox
  ADD COLUMN external_chat_id text NOT NULL DEFAULT '',
  ADD COLUMN external_user_id text NOT NULL DEFAULT '';

CREATE OR REPLACE FUNCTION public.claim_channel_inbox(
  p_tenant_id text, p_channel text, p_external_account_id text,
  p_external_message_id text, p_request_id text, p_agent_app_id text,
  p_session_id text, p_external_chat_id text, p_external_user_id text,
  p_payload_ref text, p_payload_digest text, p_key_version bigint,
  p_initial_state text
) RETURNS SETOF public.inbox LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_row public.inbox%ROWTYPE;
BEGIN
  IF p_initial_state NOT IN ('preprocess_pending', 'dispatch_pending')
     OR p_payload_digest !~ '^[0-9a-f]{64}$'
     OR length(btrim(p_external_chat_id)) > 512
     OR length(btrim(p_external_user_id)) > 512 THEN
    RAISE EXCEPTION 'invalid channel inbox claim' USING ERRCODE = '22023';
  END IF;
  INSERT INTO public.inbox(
    tenant_id, channel, external_account_id, external_message_id, request_id,
    agent_app_id, session_id, external_chat_id, external_user_id, state,
    payload_ref, payload_digest, key_version
  ) VALUES (
    p_tenant_id, p_channel, p_external_account_id, p_external_message_id,
    p_request_id, p_agent_app_id, p_session_id, p_external_chat_id,
    p_external_user_id, p_initial_state, p_payload_ref, p_payload_digest,
    p_key_version
  ) ON CONFLICT (tenant_id, channel, external_account_id, external_message_id) DO NOTHING;
  SELECT * INTO v_row FROM public.inbox
    WHERE tenant_id = p_tenant_id AND channel = p_channel
      AND external_account_id = p_external_account_id
      AND external_message_id = p_external_message_id FOR UPDATE;
  IF v_row.payload_digest <> p_payload_digest OR v_row.payload_ref <> p_payload_ref
     OR v_row.agent_app_id <> p_agent_app_id
     OR v_row.session_id IS DISTINCT FROM p_session_id
     OR v_row.external_chat_id <> p_external_chat_id
     OR v_row.external_user_id <> p_external_user_id THEN
    RAISE EXCEPTION 'channel inbox idempotency collision' USING ERRCODE = '23505';
  END IF;
  RETURN NEXT v_row;
END;
$$;

REVOKE ALL ON FUNCTION public.claim_channel_inbox(
  text,text,text,text,text,text,text,text,text,text,text,bigint,text
) FROM PUBLIC;

CREATE TABLE public.channel_public_route (
  channel text NOT NULL,
  route_key_digest text NOT NULL,
  opaque_binding_id text NOT NULL,
  binding_version bigint NOT NULL CHECK (binding_version >= 1),
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (channel, route_key_digest),
  UNIQUE (opaque_binding_id),
  CHECK (length(btrim(channel)) BETWEEN 1 AND 64),
  CHECK (length(btrim(route_key_digest)) BETWEEN 16 AND 256),
  CHECK (length(btrim(opaque_binding_id)) BETWEEN 16 AND 256)
);

CREATE TABLE public.channel_binding_locator (
  opaque_binding_id text PRIMARY KEY REFERENCES public.channel_public_route(opaque_binding_id),
  tenant_id text NOT NULL,
  config_version bigint NOT NULL,
  binding_id text NOT NULL,
  FOREIGN KEY (tenant_id, config_version, binding_id)
    REFERENCES public.channel_binding(tenant_id, config_version, binding_id)
);

CREATE TABLE public.channel_ingress_candidate (
  candidate_token_digest text PRIMARY KEY,
  opaque_binding_id text NOT NULL REFERENCES public.channel_public_route(opaque_binding_id),
  channel text NOT NULL,
  route_key_digest text NOT NULL,
  purpose text NOT NULL,
  binding_version bigint NOT NULL CHECK (binding_version >= 1),
  state text NOT NULL,
  receipt_token_digest text,
  protocol_identity_digest text,
  issued_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  verified_at timestamptz,
  version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
  CHECK (state IN ('issued','verifier_acquired','verified','promoted','burned')),
  CHECK (purpose = 'channel_verify'),
  CHECK (expires_at > issued_at),
  CHECK (length(btrim(candidate_token_digest)) BETWEEN 16 AND 256),
  CHECK (
    (state IN ('issued','verifier_acquired','burned') AND receipt_token_digest IS NULL AND protocol_identity_digest IS NULL AND verified_at IS NULL)
    OR
    (state IN ('verified','promoted') AND receipt_token_digest IS NOT NULL AND protocol_identity_digest IS NOT NULL AND verified_at IS NOT NULL)
  )
);

CREATE INDEX channel_ingress_candidate_expiry_idx
  ON public.channel_ingress_candidate(state, expires_at);

COMMIT;
