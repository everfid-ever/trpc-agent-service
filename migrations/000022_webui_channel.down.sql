BEGIN;

DROP TABLE IF EXISTS public.webui_message;

CREATE OR REPLACE FUNCTION public.populate_channel_send_secret()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_secret jsonb;
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
    NEW.send_secret_ref := NULLIF(btrim(v_secret->>'ref'), '');
    BEGIN
      NEW.send_secret_version := NULLIF(v_secret->>'version', '')::bigint;
    EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
      RAISE EXCEPTION 'invalid channel send secret version' USING ERRCODE = '22023';
    END;
  END IF;

  IF NEW.channel IN ('feishu', 'wecom') AND
     (NEW.send_secret_ref IS NULL OR NEW.send_secret_version IS NULL OR NEW.send_secret_version < 1) THEN
    RAISE EXCEPTION 'channel send secret is required' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION public.populate_channel_send_secret() FROM PUBLIC;

COMMIT;
