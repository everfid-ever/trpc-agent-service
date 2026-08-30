BEGIN;

CREATE TABLE public.pricing_snapshot (
  tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
  pricing_version bigint NOT NULL CHECK (pricing_version >= 1),
  schema_version integer NOT NULL CHECK (schema_version = 1),
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  state text NOT NULL CHECK (state='published'),
  published_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, pricing_version)
);

CREATE TABLE public.policy_snapshot (
  tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
  policy_version bigint NOT NULL CHECK (policy_version >= 1),
  schema_version integer NOT NULL CHECK (schema_version = 1),
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  pricing_version bigint,
  state text NOT NULL CHECK (state='published'),
  published_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, policy_version),
  FOREIGN KEY (tenant_id, pricing_version) REFERENCES public.pricing_snapshot(tenant_id, pricing_version),
  CHECK ((COALESCE((payload->'budget'->>'max_cost_micros_per_run')::bigint,0)=0 AND pricing_version IS NULL)
      OR (COALESCE((payload->'budget'->>'max_cost_micros_per_run')::bigint,0)>0 AND pricing_version IS NOT NULL))
);

CREATE FUNCTION public.reject_governance_snapshot_change() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'published governance snapshot is immutable' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER policy_snapshot_immutable BEFORE UPDATE OR DELETE ON public.policy_snapshot
FOR EACH ROW EXECUTE FUNCTION public.reject_governance_snapshot_change();
CREATE TRIGGER pricing_snapshot_immutable BEFORE UPDATE OR DELETE ON public.pricing_snapshot
FOR EACH ROW EXECUTE FUNCTION public.reject_governance_snapshot_change();

-- Existing configurations are upgraded to an explicit deny-by-default policy.
-- This prevents dangling policy versions without silently granting capability.
INSERT INTO public.policy_snapshot(tenant_id,policy_version,schema_version,payload,content_digest,pricing_version,state)
SELECT DISTINCT tenant_id,(payload->>'policy_version')::bigint,1,
  '{"schema_version":1,"default_action":"deny","input_dlp":"disabled","output_dlp":"disabled","budget":{"max_input_tokens":0,"max_output_tokens":0,"max_cost_micros_per_run":0}}'::jsonb,
  '9e0969fee4ce512275943bd0a66d147d1fba1bc6498f67abf88d6b7d1742ef20',NULL::bigint,'published'
FROM public.config_snapshot
WHERE COALESCE((payload->>'policy_version')::bigint,0) >= 1
ON CONFLICT (tenant_id,policy_version) DO NOTHING;

CREATE TABLE public.budget_reservation (
  tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
  reservation_id text NOT NULL,
  request_id text NOT NULL,
  resource_id text NOT NULL,
  attempt_class text NOT NULL,
  policy_version bigint NOT NULL,
  pricing_version bigint,
  budget_period date NOT NULL,
  reserved_cost_micros bigint NOT NULL CHECK (reserved_cost_micros >= 0),
  reserved_tokens bigint NOT NULL CHECK (reserved_tokens >= 0),
  actual_cost_micros bigint NOT NULL DEFAULT 0 CHECK (actual_cost_micros >= 0),
  input_tokens bigint NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
  output_tokens bigint NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
  state text NOT NULL CHECK (state IN ('reserved','settled','refunded')),
  version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
  refund_reason text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,reservation_id),
  UNIQUE (tenant_id,request_id,resource_id,attempt_class),
  FOREIGN KEY (tenant_id,policy_version) REFERENCES public.policy_snapshot(tenant_id,policy_version),
  FOREIGN KEY (tenant_id,pricing_version) REFERENCES public.pricing_snapshot(tenant_id,pricing_version),
  CHECK (actual_cost_micros <= reserved_cost_micros),
  CHECK (reserved_tokens = 0 OR input_tokens + output_tokens <= reserved_tokens)
);
CREATE INDEX budget_reservation_period_idx ON public.budget_reservation(tenant_id,budget_period,state);

CREATE TABLE public.usage_ledger (
  tenant_id text NOT NULL,
  request_id text NOT NULL,
  stage text NOT NULL,
  usage_kind text NOT NULL,
  reservation_id text NOT NULL,
  input_tokens bigint NOT NULL CHECK (input_tokens >= 0),
  output_tokens bigint NOT NULL CHECK (output_tokens >= 0),
  cached_input_tokens bigint NOT NULL CHECK (cached_input_tokens >= 0 AND cached_input_tokens <= input_tokens),
  cost_micros bigint NOT NULL CHECK (cost_micros >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,request_id,stage,usage_kind),
  FOREIGN KEY (tenant_id,reservation_id) REFERENCES public.budget_reservation(tenant_id,reservation_id)
);

CREATE TABLE public.governance_decision (
  tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
  decision_id text NOT NULL,
  request_id text NOT NULL,
  stage text NOT NULL,
  action text NOT NULL CHECK (action IN ('allow','deny','ask','redact','throttle')),
  reason_code text NOT NULL,
  policy_version bigint NOT NULL,
  rule_ids jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(rule_ids)='array'),
  reservation_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,decision_id),
  FOREIGN KEY (tenant_id,policy_version) REFERENCES public.policy_snapshot(tenant_id,policy_version),
  FOREIGN KEY (tenant_id,reservation_id) REFERENCES public.budget_reservation(tenant_id,reservation_id)
);

ALTER TABLE public.config_snapshot ADD COLUMN policy_version bigint
  GENERATED ALWAYS AS ((payload->>'policy_version')::bigint) STORED;
CREATE FUNCTION public.ensure_config_policy_snapshot() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE v_policy_version bigint;
BEGIN
  v_policy_version := (NEW.payload->>'policy_version')::bigint;
  IF v_policy_version IS NULL OR v_policy_version < 1 THEN
    RAISE EXCEPTION 'config policy version is invalid' USING ERRCODE='22023';
  END IF;
  INSERT INTO public.policy_snapshot(tenant_id,policy_version,schema_version,payload,content_digest,pricing_version,state)
  VALUES(NEW.tenant_id,v_policy_version,1,
    '{"schema_version":1,"default_action":"deny","input_dlp":"disabled","output_dlp":"disabled","budget":{"max_input_tokens":0,"max_output_tokens":0,"max_cost_micros_per_run":0}}'::jsonb,
    '9e0969fee4ce512275943bd0a66d147d1fba1bc6498f67abf88d6b7d1742ef20',NULL,'published')
  ON CONFLICT (tenant_id,policy_version) DO NOTHING;
  RETURN NEW;
END;
$$;
CREATE TRIGGER config_snapshot_policy_default BEFORE INSERT ON public.config_snapshot
FOR EACH ROW EXECUTE FUNCTION public.ensure_config_policy_snapshot();
ALTER TABLE public.config_snapshot ADD CONSTRAINT config_snapshot_policy_fk
  FOREIGN KEY (tenant_id, policy_version)
  REFERENCES public.policy_snapshot(tenant_id,policy_version) NOT VALID;
ALTER TABLE public.config_snapshot VALIDATE CONSTRAINT config_snapshot_policy_fk;

COMMIT;
