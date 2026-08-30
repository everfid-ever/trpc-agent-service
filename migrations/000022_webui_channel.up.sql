BEGIN;

CREATE OR REPLACE FUNCTION public.populate_channel_send_secret()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_secret jsonb;
  v_ref text;
  v_version bigint;
BEGIN
  NEW.send_secret_ref := NULL;
  NEW.send_secret_version := NULL;
  SELECT entries.item->'send_secret_ref'
    INTO v_secret
  FROM public.config_snapshot snapshot,
       LATERAL jsonb_array_elements(COALESCE(snapshot.payload->'channel_bindings', '[]'::jsonb)) AS entries(item)
  WHERE snapshot.tenant_id = NEW.tenant_id
    AND snapshot.config_version = NEW.config_version
    AND entries.item->>'binding_id' = NEW.binding_id
  LIMIT 1;

  IF v_secret IS NOT NULL AND jsonb_typeof(v_secret) = 'object' THEN
    v_ref := NULLIF(btrim(v_secret->>'ref'), '');
    BEGIN
      v_version := NULLIF(v_secret->>'version', '')::bigint;
    EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
      RAISE EXCEPTION 'invalid channel send secret version' USING ERRCODE = '22023';
    END;
    IF v_ref IS NOT NULL AND v_version >= 1 THEN
      NEW.send_secret_ref := v_ref;
      NEW.send_secret_version := v_version;
    END IF;
  END IF;

  IF NEW.channel IN ('feishu', 'wecom') AND
     (NEW.send_secret_ref IS NULL OR NEW.send_secret_version IS NULL) THEN
    RAISE EXCEPTION 'channel send secret is required' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TABLE public.webui_message (
  tenant_id             text        NOT NULL,
  config_version        bigint      NOT NULL CHECK (config_version >= 1),
  channel_binding_id    text        NOT NULL,
  external_account_id   text        NOT NULL,
  external_user_id      text        NOT NULL,
  external_chat_id      text        NOT NULL,
  request_id            text        NOT NULL,
  client_request_id     text        NOT NULL,
  provider_message_id   text        NOT NULL,
  content_ref           text        NOT NULL,
  content_digest        text        NOT NULL,
  created_at            timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, client_request_id),
  UNIQUE (tenant_id, provider_message_id),
  FOREIGN KEY (tenant_id, config_version, channel_binding_id)
    REFERENCES public.channel_binding(tenant_id, config_version, binding_id),
  FOREIGN KEY (tenant_id, request_id)
    REFERENCES public.execution_record(tenant_id, request_id),
  CHECK (length(btrim(external_account_id)) BETWEEN 1 AND 512),
  CHECK (length(btrim(external_user_id)) BETWEEN 1 AND 512),
  CHECK (length(btrim(external_chat_id)) BETWEEN 1 AND 512),
  CHECK (length(btrim(client_request_id)) BETWEEN 1 AND 256),
  CHECK (length(btrim(provider_message_id)) BETWEEN 1 AND 256),
  CHECK (content_digest ~ '^[0-9a-f]{64}$')
);

CREATE INDEX webui_message_mailbox_idx
  ON public.webui_message(
    tenant_id, channel_binding_id, external_account_id,
    external_user_id, external_chat_id, created_at, provider_message_id
  );

REVOKE ALL ON FUNCTION public.populate_channel_send_secret() FROM PUBLIC;

COMMIT;
