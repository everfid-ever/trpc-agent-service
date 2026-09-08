BEGIN;

DROP FUNCTION public.update_tenant_configuration(text, bigint, text, bigint, integer, bigint, bigint, character, integer, text, text, jsonb, numeric, text, text, bigint, text, text, text, text, text);

ALTER TABLE public.tenant
  DROP CONSTRAINT tenant_redaction_rules_array_check,
  DROP COLUMN redaction_rules;

CREATE FUNCTION public.update_tenant_configuration(p_tenant_id text, p_expected_version bigint, p_display_name text, p_request_limit_per_minute bigint, p_max_concurrent_executions integer, p_monthly_token_budget bigint, p_monthly_cost_budget_micros bigint, p_billing_currency character, p_audit_retention_days integer, p_audit_payload_mode text, p_log_masking_level text, p_trace_sampling_rate numeric, p_default_agent_app_id text, p_default_backend_profile_id text, p_active_config_version bigint, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS bigint
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
AS $$
DECLARE
  v_status text;
  v_version bigint;
  v_next_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'tenant configuration metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  SELECT status, version INTO v_status, v_version FROM public.tenant
    WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_status = 'disabled' THEN RAISE EXCEPTION 'disabled tenant is immutable' USING ERRCODE = '55000'; END IF;
  IF v_version <> p_expected_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE = '40001'; END IF;
  v_next_version := v_version + 1;
  UPDATE public.tenant SET
    display_name = p_display_name,
    request_limit_per_minute = p_request_limit_per_minute,
    max_concurrent_executions = p_max_concurrent_executions,
    monthly_token_budget = p_monthly_token_budget,
    monthly_cost_budget_micros = p_monthly_cost_budget_micros,
    billing_currency = p_billing_currency,
    audit_retention_days = p_audit_retention_days,
    audit_payload_mode = p_audit_payload_mode,
    log_masking_level = p_log_masking_level,
    trace_sampling_rate = p_trace_sampling_rate,
    default_agent_app_id = p_default_agent_app_id,
    default_backend_profile_id = p_default_backend_profile_id,
    active_config_version = p_active_config_version,
    version = v_next_version
  WHERE tenant_id = p_tenant_id;
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('tenant-config-audit:%s:%s', p_tenant_id, v_next_version), 'audit', p_tenant_id, v_next_version, format('tenant-config:%s:%s:audit', p_tenant_id, v_next_version), format('tenant-config://%s/%s', p_tenant_id, v_next_version), p_traceparent),
    (p_tenant_id, format('tenant-config-control:%s:%s', p_tenant_id, v_next_version), 'tenant-control', p_tenant_id, v_next_version, format('tenant-config:%s:%s:control', p_tenant_id, v_next_version), format('tenant-config://%s/%s', p_tenant_id, v_next_version), p_traceparent);
  RETURN v_next_version;
END;
$$;

COMMIT;
