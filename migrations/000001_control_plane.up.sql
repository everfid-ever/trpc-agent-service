BEGIN;

CREATE TABLE outbox (
  tenant_id        text        NOT NULL,
  outbox_id        text        NOT NULL,
  kind             text        NOT NULL,
  aggregate_id     text        NOT NULL,
  event_seq        bigint      NOT NULL CHECK (event_seq >= 0),
  idempotency_key  text        NOT NULL,
  payload_ref      text        NOT NULL,
  traceparent      text,
  state            text        NOT NULL DEFAULT 'pending',
  version          bigint      NOT NULL DEFAULT 0 CHECK (version >= 0),
  attempt          integer     NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  next_attempt_at  timestamptz NOT NULL DEFAULT now(),
  claim_owner      text,
  claim_until      timestamptz,
  published_at     timestamptz,
  created_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, outbox_id),
  UNIQUE (tenant_id, kind, idempotency_key),
  CHECK (kind IN ('audit', 'tenant-control', 'config-invalidation', 'dispatch', 'reply', 'wakeup')),
  CHECK (state IN ('pending', 'claimed', 'retry_wait', 'published', 'dead_letter'))
);

CREATE INDEX outbox_claim_idx
  ON outbox(kind, state, next_attempt_at, created_at);

CREATE TABLE tenant (
  tenant_id                    text           PRIMARY KEY,
  tenant_key                   text           NOT NULL UNIQUE,
  display_name                 text           NOT NULL,
  status                       text           NOT NULL DEFAULT 'active',
  request_limit_per_minute     bigint,
  max_concurrent_executions    integer,
  monthly_token_budget         bigint,
  monthly_cost_budget_micros   bigint,
  billing_currency             char(3)        NOT NULL DEFAULT 'USD',
  audit_retention_days         integer        NOT NULL DEFAULT 180,
  audit_payload_mode           text           NOT NULL DEFAULT 'redacted',
  log_masking_level            text           NOT NULL DEFAULT 'basic',
  trace_sampling_rate          numeric(5,4)   NOT NULL DEFAULT 0.0100,
  default_agent_app_id         text,
  default_backend_profile_id   text,
  active_config_version        bigint,
  version                      bigint         NOT NULL DEFAULT 1,
  created_at                   timestamptz    NOT NULL DEFAULT clock_timestamp(),
  updated_at                   timestamptz    NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT tenant_id_format_ck CHECK (tenant_id ~ '^t_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
  CONSTRAINT tenant_key_format_ck CHECK (
    tenant_key = lower(tenant_key)
    AND length(tenant_key) BETWEEN 2 AND 64
    AND tenant_key ~ '^[a-z][a-z0-9-]{1,63}$'
  ),
  CONSTRAINT tenant_display_name_ck CHECK (length(btrim(display_name)) BETWEEN 1 AND 200),
  CONSTRAINT tenant_status_ck CHECK (status IN ('active', 'suspended', 'disabled')),
  CONSTRAINT tenant_request_limit_ck CHECK (request_limit_per_minute IS NULL OR request_limit_per_minute >= 0),
  CONSTRAINT tenant_concurrency_limit_ck CHECK (max_concurrent_executions IS NULL OR max_concurrent_executions >= 0),
  CONSTRAINT tenant_token_budget_ck CHECK (monthly_token_budget IS NULL OR monthly_token_budget >= 0),
  CONSTRAINT tenant_cost_budget_ck CHECK (monthly_cost_budget_micros IS NULL OR monthly_cost_budget_micros >= 0),
  CONSTRAINT tenant_currency_ck CHECK (billing_currency ~ '^[A-Z]{3}$'),
  CONSTRAINT tenant_audit_retention_ck CHECK (audit_retention_days >= 1),
  CONSTRAINT tenant_audit_payload_mode_ck CHECK (audit_payload_mode IN ('metadata_only', 'redacted', 'encrypted_reference')),
  CONSTRAINT tenant_log_masking_level_ck CHECK (log_masking_level IN ('none', 'basic', 'strict')),
  CONSTRAINT tenant_trace_sampling_ck CHECK (trace_sampling_rate BETWEEN 0 AND 1),
  CONSTRAINT tenant_active_config_version_ck CHECK (active_config_version IS NULL OR active_config_version >= 1),
  CONSTRAINT tenant_version_ck CHECK (version >= 1)
);

CREATE TABLE tenant_status_change (
  tenant_id        text        NOT NULL REFERENCES tenant(tenant_id),
  event_id         bigint      GENERATED ALWAYS AS IDENTITY,
  previous_status  text        NOT NULL,
  next_status      text        NOT NULL,
  previous_version bigint      NOT NULL CHECK (previous_version >= 1),
  next_version     bigint      NOT NULL CHECK (next_version = previous_version + 1),
  actor_type       text        NOT NULL CHECK (length(btrim(actor_type)) BETWEEN 1 AND 64),
  actor_id         text        NOT NULL CHECK (length(btrim(actor_id)) BETWEEN 1 AND 256),
  reason_code      text        NOT NULL CHECK (length(btrim(reason_code)) BETWEEN 1 AND 128),
  reason_text_ref  text,
  correlation_id   text        NOT NULL CHECK (length(btrim(correlation_id)) > 0),
  trace_id         text        NOT NULL CHECK (length(btrim(trace_id)) > 0),
  occurred_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, event_id),
  UNIQUE (tenant_id, next_version),
  CHECK ((previous_status, next_status) IN (
    ('active', 'suspended'), ('active', 'disabled'),
    ('suspended', 'active'), ('suspended', 'disabled')
  )),
  CHECK (reason_text_ref IS NULL OR length(btrim(reason_text_ref)) > 0)
);

CREATE TABLE agent_app (
  tenant_id         text        NOT NULL REFERENCES tenant(tenant_id),
  agent_app_id      text        NOT NULL,
  agent_app_key     text        NOT NULL,
  display_name      text        NOT NULL,
  description       text        NOT NULL DEFAULT '',
  status            text        NOT NULL DEFAULT 'draft',
  current_revision  bigint,
  next_revision     bigint      NOT NULL DEFAULT 1,
  version           bigint      NOT NULL DEFAULT 1,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_app_id),
  UNIQUE (tenant_id, agent_app_key),
  CHECK (agent_app_id ~ '^app_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
  CHECK (agent_app_key ~ '^[a-z][a-z0-9-]{1,63}$'),
  CHECK (length(btrim(display_name)) BETWEEN 1 AND 200),
  CHECK (length(description) <= 2000),
  CHECK (status IN ('draft', 'active', 'suspended', 'disabled')),
  CHECK (next_revision >= 1),
  CHECK (version >= 1),
  CHECK (
    (status = 'draft' AND current_revision IS NULL)
    OR (status IN ('active', 'suspended') AND current_revision IS NOT NULL)
    OR status = 'disabled'
  )
);

CREATE TABLE agent_app_revision (
  tenant_id             text        NOT NULL,
  agent_app_id          text        NOT NULL,
  revision              bigint      NOT NULL,
  state                 text        NOT NULL DEFAULT 'draft',
  draft_version         bigint      NOT NULL DEFAULT 1,
  agent_kind            text        NOT NULL,
  schema_version        integer     NOT NULL DEFAULT 1,
  description           text        NOT NULL DEFAULT '',
  instruction           text        NOT NULL,
  global_instruction    text        NOT NULL DEFAULT '',
  model_profile_id      text        NOT NULL,
  model_profile_version bigint      NOT NULL,
  generation_config     jsonb       NOT NULL DEFAULT '{}'::jsonb,
  runtime_policy        jsonb       NOT NULL DEFAULT '{}'::jsonb,
  content_digest        text,
  published_at          timestamptz,
  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_app_id, revision),
  FOREIGN KEY (tenant_id, agent_app_id) REFERENCES agent_app(tenant_id, agent_app_id),
  CHECK (revision >= 1),
  CHECK (draft_version >= 1),
  CHECK (state IN ('draft', 'published')),
  CHECK (agent_kind = 'llm'),
  CHECK (schema_version = 1),
  CHECK (length(description) <= 2000),
  CHECK (length(btrim(instruction)) BETWEEN 1 AND 65536),
  CHECK (length(global_instruction) <= 65536),
  CHECK (length(btrim(model_profile_id)) > 0),
  CHECK (model_profile_version >= 1),
  CHECK (jsonb_typeof(generation_config) = 'object'),
  CHECK (jsonb_typeof(runtime_policy) = 'object'),
  CHECK (
    (state = 'draft' AND content_digest IS NULL AND published_at IS NULL)
    OR (state = 'published' AND content_digest ~ '^[0-9a-f]{64}$' AND published_at IS NOT NULL)
  )
);

ALTER TABLE agent_app ADD CONSTRAINT agent_app_current_revision_fk
  FOREIGN KEY (tenant_id, agent_app_id, current_revision)
  REFERENCES agent_app_revision(tenant_id, agent_app_id, revision)
  DEFERRABLE INITIALLY IMMEDIATE;

CREATE TABLE agent_app_revision_tool (
  tenant_id      text    NOT NULL,
  agent_app_id   text    NOT NULL,
  revision       bigint  NOT NULL,
  tool_id        text    NOT NULL,
  tool_version   bigint  NOT NULL CHECK (tool_version >= 1),
  required       boolean NOT NULL DEFAULT false,
  PRIMARY KEY (tenant_id, agent_app_id, revision, tool_id),
  FOREIGN KEY (tenant_id, agent_app_id, revision)
    REFERENCES agent_app_revision(tenant_id, agent_app_id, revision) ON DELETE CASCADE
);

CREATE TABLE agent_app_revision_knowledge (
  tenant_id        text   NOT NULL,
  agent_app_id     text   NOT NULL,
  revision         bigint NOT NULL,
  knowledge_id     text   NOT NULL,
  knowledge_version bigint NOT NULL CHECK (knowledge_version >= 1),
  PRIMARY KEY (tenant_id, agent_app_id, revision, knowledge_id),
  FOREIGN KEY (tenant_id, agent_app_id, revision)
    REFERENCES agent_app_revision(tenant_id, agent_app_id, revision) ON DELETE CASCADE
);

CREATE TABLE agent_app_change (
  tenant_id        text        NOT NULL,
  agent_app_id     text        NOT NULL,
  event_id         bigint      GENERATED ALWAYS AS IDENTITY,
  kind             text        NOT NULL CHECK (kind IN ('published', 'rolled_back', 'status_changed')),
  previous_revision bigint,
  current_revision bigint,
  previous_status  text,
  current_status   text,
  previous_version bigint      NOT NULL CHECK (previous_version >= 1),
  next_version     bigint      NOT NULL CHECK (next_version = previous_version + 1),
  actor_id         text        NOT NULL,
  reason_code      text        NOT NULL,
  correlation_id   text        NOT NULL,
  trace_id         text        NOT NULL,
  occurred_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, agent_app_id, event_id),
  UNIQUE (tenant_id, agent_app_id, next_version),
  FOREIGN KEY (tenant_id, agent_app_id) REFERENCES agent_app(tenant_id, agent_app_id)
  ,CHECK (length(btrim(actor_id)) > 0)
  ,CHECK (length(btrim(reason_code)) > 0)
  ,CHECK (length(btrim(correlation_id)) > 0)
  ,CHECK (length(btrim(trace_id)) > 0)
);

CREATE TABLE config_snapshot (
  tenant_id      text        NOT NULL REFERENCES tenant(tenant_id),
  config_version bigint      NOT NULL CHECK (config_version >= 1),
  schema_version integer     NOT NULL CHECK (schema_version = 1),
  payload        jsonb       NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  content_digest text        NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  state          text        NOT NULL CHECK (state = 'published'),
  actor_id       text        NOT NULL,
  reason_code    text        NOT NULL,
  correlation_id text        NOT NULL,
  trace_id       text        NOT NULL,
  published_at   timestamptz NOT NULL DEFAULT now(),
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, config_version),
  UNIQUE (tenant_id, content_digest, config_version)
  ,CHECK (length(btrim(actor_id)) > 0)
  ,CHECK (length(btrim(reason_code)) > 0)
  ,CHECK (length(btrim(correlation_id)) > 0)
  ,CHECK (length(btrim(trace_id)) > 0)
);

CREATE TABLE channel_binding (
  tenant_id          text        NOT NULL,
  config_version     bigint      NOT NULL,
  binding_id         text        NOT NULL,
  channel            text        NOT NULL,
  external_account_id text       NOT NULL,
  agent_app_id       text        NOT NULL,
  secret_ref         text        NOT NULL,
  secret_version     bigint      NOT NULL CHECK (secret_version >= 1),
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, config_version, binding_id),
  UNIQUE (tenant_id, config_version, channel, external_account_id),
  FOREIGN KEY (tenant_id, config_version) REFERENCES config_snapshot(tenant_id, config_version),
  FOREIGN KEY (tenant_id, agent_app_id) REFERENCES agent_app(tenant_id, agent_app_id)
);

CREATE TABLE backend_profile (
  tenant_id          text        NOT NULL REFERENCES tenant(tenant_id),
  backend_profile_id text        NOT NULL,
  display_name       text        NOT NULL,
  status             text        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'disabled')),
  version            bigint      NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, backend_profile_id)
);

CREATE TABLE backend_binding (
  tenant_id          text        NOT NULL,
  config_version     bigint      NOT NULL,
  domain             text        NOT NULL,
  backend_type       text        NOT NULL,
  backend_ref        text        NOT NULL,
  backend_profile_id text,
  credential_ref     text        NOT NULL,
  credential_version bigint      NOT NULL CHECK (credential_version >= 1),
  capabilities       text[]      NOT NULL DEFAULT '{}',
  PRIMARY KEY (tenant_id, config_version, domain),
  FOREIGN KEY (tenant_id, config_version) REFERENCES config_snapshot(tenant_id, config_version),
  FOREIGN KEY (tenant_id, backend_profile_id) REFERENCES backend_profile(tenant_id, backend_profile_id)
);

ALTER TABLE tenant
  ADD CONSTRAINT tenant_default_agent_app_fk
    FOREIGN KEY (tenant_id, default_agent_app_id)
    REFERENCES agent_app(tenant_id, agent_app_id) DEFERRABLE INITIALLY IMMEDIATE,
  ADD CONSTRAINT tenant_default_backend_profile_fk
    FOREIGN KEY (tenant_id, default_backend_profile_id)
    REFERENCES backend_profile(tenant_id, backend_profile_id) DEFERRABLE INITIALLY IMMEDIATE,
  ADD CONSTRAINT tenant_active_config_fk
    FOREIGN KEY (tenant_id, active_config_version)
    REFERENCES config_snapshot(tenant_id, config_version) DEFERRABLE INITIALLY IMMEDIATE;

CREATE OR REPLACE FUNCTION maintain_tenant_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id OR NEW.tenant_key <> OLD.tenant_key THEN
    RAISE EXCEPTION 'tenant identity is immutable' USING ERRCODE = '23000';
  END IF;
  IF NEW.version <> OLD.version + 1 THEN
    RAISE EXCEPTION 'tenant version must increase by exactly one' USING ERRCODE = '40001';
  END IF;
  IF NEW.status <> OLD.status AND NOT (
    (OLD.status = 'active' AND NEW.status IN ('suspended', 'disabled'))
    OR (OLD.status = 'suspended' AND NEW.status IN ('active', 'disabled'))
  ) THEN
    RAISE EXCEPTION 'illegal tenant status transition' USING ERRCODE = '23514';
  END IF;
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END;
$$;

CREATE TRIGGER tenant_maintain_update_trg
BEFORE UPDATE ON tenant FOR EACH ROW EXECUTE FUNCTION maintain_tenant_update();

CREATE OR REPLACE FUNCTION reject_agent_app_identity_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
     OR NEW.agent_app_id IS DISTINCT FROM OLD.agent_app_id
     OR NEW.agent_app_key IS DISTINCT FROM OLD.agent_app_key THEN
    RAISE EXCEPTION 'agent app identity is immutable' USING ERRCODE = '23000';
  END IF;
  IF NEW.version <> OLD.version + 1 THEN
    RAISE EXCEPTION 'agent app version must increase by exactly one' USING ERRCODE = '40001';
  END IF;
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_identity_version_guard
BEFORE UPDATE ON agent_app FOR EACH ROW EXECUTE FUNCTION reject_agent_app_identity_change();

CREATE OR REPLACE FUNCTION reject_published_revision_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state = 'published' THEN
    RAISE EXCEPTION 'published agent app revision is immutable' USING ERRCODE = '55000';
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_revision_published_immutable
BEFORE UPDATE OR DELETE ON agent_app_revision
FOR EACH ROW EXECUTE FUNCTION reject_published_revision_change();

CREATE OR REPLACE FUNCTION guard_agent_app_current_revision()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_state text;
BEGIN
  IF NEW.current_revision IS NULL OR NEW.current_revision IS NOT DISTINCT FROM OLD.current_revision THEN
    RETURN NEW;
  END IF;
  SELECT state INTO v_state FROM agent_app_revision
    WHERE tenant_id = NEW.tenant_id AND agent_app_id = NEW.agent_app_id
      AND revision = NEW.current_revision;
  IF NOT FOUND OR v_state <> 'published' THEN
    RAISE EXCEPTION 'current revision must be published' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_current_revision_published
BEFORE UPDATE ON agent_app FOR EACH ROW EXECUTE FUNCTION guard_agent_app_current_revision();

CREATE OR REPLACE FUNCTION guard_revision_child_write()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_state text;
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    SELECT state INTO v_state FROM agent_app_revision
      WHERE tenant_id = OLD.tenant_id AND agent_app_id = OLD.agent_app_id
        AND revision = OLD.revision FOR UPDATE;
    IF NOT FOUND OR v_state <> 'draft' THEN
      RAISE EXCEPTION 'source revision is not mutable' USING ERRCODE = '55000';
    END IF;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    SELECT state INTO v_state FROM agent_app_revision
      WHERE tenant_id = NEW.tenant_id AND agent_app_id = NEW.agent_app_id
        AND revision = NEW.revision FOR UPDATE;
    IF NOT FOUND OR v_state <> 'draft' THEN
      RAISE EXCEPTION 'target revision is not mutable' USING ERRCODE = '55000';
    END IF;
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_revision_tool_draft_only
BEFORE INSERT OR UPDATE OR DELETE ON agent_app_revision_tool
FOR EACH ROW EXECUTE FUNCTION guard_revision_child_write();
CREATE TRIGGER agent_app_revision_knowledge_draft_only
BEFORE INSERT OR UPDATE OR DELETE ON agent_app_revision_knowledge
FOR EACH ROW EXECUTE FUNCTION guard_revision_child_write();

CREATE OR REPLACE FUNCTION reject_published_config_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'published config snapshot is immutable' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER config_snapshot_immutable
BEFORE UPDATE OR DELETE ON config_snapshot
FOR EACH ROW EXECUTE FUNCTION reject_published_config_change();

CREATE OR REPLACE FUNCTION publish_agent_app_revision(
  p_tenant_id text, p_agent_app_id text, p_revision bigint,
  p_expected_app_version bigint, p_expected_draft_version bigint,
  p_content_digest text, p_actor_id text, p_reason_code text,
  p_correlation_id text, p_trace_id text, p_traceparent text
) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  v_tenant_status text;
  v_app_status text;
  v_app_version bigint;
  v_revision_state text;
  v_draft_version bigint;
  v_next_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'agent app publish metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  SELECT status INTO v_tenant_status FROM public.tenant
    WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_tenant_status = 'disabled' THEN RAISE EXCEPTION 'tenant is disabled' USING ERRCODE = '55000'; END IF;
  SELECT status, version INTO v_app_status, v_app_version FROM public.agent_app
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'agent app does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_app_status = 'disabled' THEN RAISE EXCEPTION 'agent app is disabled' USING ERRCODE = '55000'; END IF;
  IF v_app_version <> p_expected_app_version THEN RAISE EXCEPTION 'agent app version conflict' USING ERRCODE = '40001'; END IF;
  SELECT state, draft_version INTO v_revision_state, v_draft_version
    FROM public.agent_app_revision
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id
      AND revision = p_revision FOR UPDATE;
  IF NOT FOUND OR v_revision_state <> 'draft' THEN RAISE EXCEPTION 'revision is not draft' USING ERRCODE = '55000'; END IF;
  IF v_draft_version <> p_expected_draft_version THEN RAISE EXCEPTION 'draft version conflict' USING ERRCODE = '40001'; END IF;
  IF p_content_digest !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'invalid content digest' USING ERRCODE = '22023'; END IF;
  UPDATE public.agent_app_revision SET
    state = 'published', content_digest = p_content_digest,
    published_at = now(), updated_at = now()
  WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id
    AND revision = p_revision;
  v_next_version := v_app_version + 1;
  UPDATE public.agent_app SET
    current_revision = p_revision,
    status = CASE WHEN status = 'draft' THEN 'active' ELSE status END,
    version = v_next_version
  WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id;
  INSERT INTO public.agent_app_change(
    tenant_id, agent_app_id, kind, current_revision,
    previous_status, current_status, previous_version, next_version,
    actor_id, reason_code, correlation_id, trace_id
  ) VALUES (
    p_tenant_id, p_agent_app_id, 'published', p_revision,
    v_app_status, CASE WHEN v_app_status = 'draft' THEN 'active' ELSE v_app_status END,
    v_app_version, v_next_version, p_actor_id, p_reason_code, p_correlation_id, p_trace_id
  );
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('app-publish-audit:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'audit', p_agent_app_id, v_next_version, format('app:%s:%s:%s:audit', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app-revision://%s/%s/%s', p_tenant_id, p_agent_app_id, p_revision), p_traceparent),
    (p_tenant_id, format('app-publish-invalidation:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'config-invalidation', p_agent_app_id, v_next_version, format('app:%s:%s:%s:invalidate', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app://%s/%s/%s', p_tenant_id, p_agent_app_id, v_next_version), p_traceparent);
  RETURN v_next_version;
END;
$$;

CREATE OR REPLACE FUNCTION rollback_agent_app_revision(
  p_tenant_id text, p_agent_app_id text, p_target_revision bigint,
  p_expected_app_version bigint, p_actor_id text, p_reason_code text,
  p_correlation_id text, p_trace_id text, p_traceparent text
) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  v_app_version bigint;
  v_app_status text;
  v_previous_revision bigint;
  v_target_state text;
  v_next_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'agent app rollback metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  PERFORM 1 FROM public.tenant WHERE tenant_id = p_tenant_id AND status <> 'disabled' FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant unavailable' USING ERRCODE = '55000'; END IF;
  SELECT version, status, current_revision INTO v_app_version, v_app_status, v_previous_revision FROM public.agent_app
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id FOR UPDATE;
  IF NOT FOUND OR v_app_status = 'disabled' THEN RAISE EXCEPTION 'agent app unavailable' USING ERRCODE = '55000'; END IF;
  IF v_app_version <> p_expected_app_version THEN RAISE EXCEPTION 'agent app version conflict' USING ERRCODE = '40001'; END IF;
  SELECT state INTO v_target_state FROM public.agent_app_revision
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id
      AND revision = p_target_revision;
  IF NOT FOUND OR v_target_state <> 'published' THEN RAISE EXCEPTION 'target revision is not published' USING ERRCODE = '55000'; END IF;
  v_next_version := v_app_version + 1;
  UPDATE public.agent_app SET current_revision = p_target_revision, version = v_next_version
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id;
  INSERT INTO public.agent_app_change(
    tenant_id, agent_app_id, kind, previous_revision, current_revision,
    previous_status, current_status, previous_version, next_version,
    actor_id, reason_code, correlation_id, trace_id
  ) VALUES (
    p_tenant_id, p_agent_app_id, 'rolled_back', v_previous_revision, p_target_revision,
    v_app_status, v_app_status, v_app_version, v_next_version,
    p_actor_id, p_reason_code, p_correlation_id, p_trace_id
  );
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('app-rollback-audit:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'audit', p_agent_app_id, v_next_version, format('app:%s:%s:%s:rollback-audit', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app-revision://%s/%s/%s', p_tenant_id, p_agent_app_id, p_target_revision), p_traceparent),
    (p_tenant_id, format('app-rollback-invalidation:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'config-invalidation', p_agent_app_id, v_next_version, format('app:%s:%s:%s:rollback-invalidate', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app://%s/%s/%s', p_tenant_id, p_agent_app_id, v_next_version), p_traceparent);
  RETURN v_next_version;
END;
$$;

CREATE OR REPLACE FUNCTION transition_agent_app_status(
  p_tenant_id text, p_agent_app_id text, p_expected_app_version bigint,
  p_next_status text, p_actor_id text, p_reason_code text,
  p_correlation_id text, p_trace_id text, p_traceparent text
) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  v_status text;
  v_version bigint;
  v_current_revision bigint;
  v_next_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'agent app status metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  PERFORM 1 FROM public.tenant WHERE tenant_id = p_tenant_id AND status <> 'disabled' FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant unavailable' USING ERRCODE = '55000'; END IF;
  SELECT status, version, current_revision INTO v_status, v_version, v_current_revision
    FROM public.agent_app WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'agent app not found' USING ERRCODE = 'P0002'; END IF;
  IF v_version <> p_expected_app_version THEN RAISE EXCEPTION 'agent app version conflict' USING ERRCODE = '40001'; END IF;
  IF NOT (
    (v_status = 'active' AND p_next_status IN ('suspended', 'disabled'))
    OR (v_status = 'suspended' AND p_next_status IN ('active', 'disabled'))
    OR (v_status = 'draft' AND p_next_status = 'disabled')
  ) THEN RAISE EXCEPTION 'illegal agent app status transition' USING ERRCODE = '23514'; END IF;
  v_next_version := v_version + 1;
  UPDATE public.agent_app SET status = p_next_status, version = v_next_version
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id;
  INSERT INTO public.agent_app_change(
    tenant_id, agent_app_id, kind, current_revision, previous_status,
    current_status, previous_version, next_version, actor_id, reason_code,
    correlation_id, trace_id
  ) VALUES (
    p_tenant_id, p_agent_app_id, 'status_changed', v_current_revision,
    v_status, p_next_status, v_version, v_next_version,
    p_actor_id, p_reason_code, p_correlation_id, p_trace_id
  );
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('app-status-audit:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'audit', p_agent_app_id, v_next_version, format('app:%s:%s:%s:status-audit', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app-change://%s/%s/%s', p_tenant_id, p_agent_app_id, v_next_version), p_traceparent),
    (p_tenant_id, format('app-status-invalidation:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'config-invalidation', p_agent_app_id, v_next_version, format('app:%s:%s:%s:status-invalidate', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app://%s/%s/%s', p_tenant_id, p_agent_app_id, v_next_version), p_traceparent);
  RETURN v_next_version;
END;
$$;

CREATE OR REPLACE FUNCTION transition_tenant_status(
  p_tenant_id text, p_expected_version bigint, p_next_status text,
  p_actor_type text, p_actor_id text, p_reason_code text, p_reason_text_ref text,
  p_correlation_id text, p_trace_id text, p_traceparent text
) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  v_previous_status text;
  v_current_version bigint;
  v_next_version bigint;
  v_payload_ref text;
BEGIN
  IF p_actor_type IS NULL OR p_actor_id IS NULL OR p_reason_code IS NULL
     OR p_correlation_id IS NULL OR p_trace_id IS NULL THEN
    RAISE EXCEPTION 'tenant status metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  SELECT status, version INTO v_previous_status, v_current_version
    FROM public.tenant WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_current_version <> p_expected_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE = '40001'; END IF;
  v_next_version := v_current_version + 1;
  UPDATE public.tenant SET status = p_next_status, version = v_next_version WHERE tenant_id = p_tenant_id;
  INSERT INTO public.tenant_status_change(
    tenant_id, previous_status, next_status, previous_version, next_version,
    actor_type, actor_id, reason_code, reason_text_ref, correlation_id, trace_id
  ) VALUES (
    p_tenant_id, v_previous_status, p_next_status, v_current_version, v_next_version,
    p_actor_type, p_actor_id, p_reason_code, p_reason_text_ref, p_correlation_id, p_trace_id
  );
  v_payload_ref := format('tenant-status-change://%s/%s', p_tenant_id, v_next_version);
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('tenant-status-audit:%s:%s', p_tenant_id, v_next_version), 'audit', p_tenant_id, v_next_version, format('tenant-status:%s:%s:audit', p_tenant_id, v_next_version), v_payload_ref, p_traceparent),
    (p_tenant_id, format('tenant-status-control:%s:%s', p_tenant_id, v_next_version), 'tenant-control', p_tenant_id, v_next_version, format('tenant-status:%s:%s:control', p_tenant_id, v_next_version), v_payload_ref, p_traceparent);
  RETURN v_next_version;
END;
$$;

CREATE OR REPLACE FUNCTION publish_config_snapshot(
  p_tenant_id text, p_expected_tenant_version bigint, p_schema_version integer,
  p_payload jsonb, p_content_digest text, p_default_agent_app_id text,
  p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text,
  p_traceparent text
) RETURNS TABLE(config_version bigint, tenant_version bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  v_status text;
  v_current_version bigint;
  v_config_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'config publish metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  SELECT status, version INTO v_status, v_current_version
    FROM public.tenant WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_status = 'disabled' THEN RAISE EXCEPTION 'disabled tenant is immutable' USING ERRCODE = '55000'; END IF;
  IF v_current_version <> p_expected_tenant_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE = '40001'; END IF;
  IF p_schema_version <> 1 OR jsonb_typeof(p_payload) <> 'object'
     OR p_content_digest !~ '^[0-9a-f]{64}$'
     OR p_payload->>'default_agent_app_id' IS DISTINCT FROM p_default_agent_app_id
     OR COALESCE((p_payload->>'policy_version')::bigint, 0) < 1 THEN
    RAISE EXCEPTION 'invalid config snapshot' USING ERRCODE = '22023';
  END IF;
  PERFORM 1 FROM public.agent_app
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_default_agent_app_id
      AND status = 'active' AND current_revision IS NOT NULL;
  IF NOT FOUND THEN RAISE EXCEPTION 'default app is not active' USING ERRCODE = '23503'; END IF;
  SELECT COALESCE(max(cs.config_version), 0) + 1 INTO v_config_version
    FROM public.config_snapshot cs WHERE cs.tenant_id = p_tenant_id;
  INSERT INTO public.config_snapshot(
    tenant_id, config_version, schema_version, payload, content_digest, state,
    actor_id, reason_code, correlation_id, trace_id
  ) VALUES (
    p_tenant_id, v_config_version, p_schema_version, p_payload, p_content_digest,
    'published', p_actor_id, p_reason_code, p_correlation_id, p_trace_id
  );
  INSERT INTO public.channel_binding(
    tenant_id, config_version, binding_id, channel, external_account_id,
    agent_app_id, secret_ref, secret_version
  )
  SELECT
    p_tenant_id, v_config_version, item.binding_id, item.channel,
    item.external_account_id, item.agent_app_id,
    item.secret_ref->>'ref', (item.secret_ref->>'version')::bigint
  FROM jsonb_to_recordset(COALESCE(p_payload->'channel_bindings', '[]'::jsonb))
    AS item(binding_id text, channel text, external_account_id text, agent_app_id text, secret_ref jsonb);
  INSERT INTO public.backend_binding(
    tenant_id, config_version, domain, backend_type, backend_ref,
    credential_ref, credential_version, capabilities
  )
  SELECT
    p_tenant_id, v_config_version, item.domain, item.backend_type,
    item.backend_ref, item.credential_ref->>'ref',
    (item.credential_ref->>'version')::bigint,
    ARRAY(SELECT jsonb_array_elements_text(COALESCE(item.capabilities, '[]'::jsonb)))
  FROM jsonb_to_recordset(COALESCE(p_payload->'backend_bindings', '[]'::jsonb))
    AS item(domain text, backend_type text, backend_ref text, credential_ref jsonb, capabilities jsonb);
  UPDATE public.tenant SET
    default_agent_app_id = p_default_agent_app_id,
    active_config_version = v_config_version,
    version = v_current_version + 1
  WHERE tenant_id = p_tenant_id;
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('config-audit:%s:%s', p_tenant_id, v_config_version), 'audit', p_tenant_id, v_config_version, format('config:%s:%s:audit', p_tenant_id, v_config_version), format('config://%s/%s', p_tenant_id, v_config_version), p_traceparent),
    (p_tenant_id, format('config-invalidation:%s:%s', p_tenant_id, v_config_version), 'config-invalidation', p_tenant_id, v_config_version, format('config:%s:%s:invalidate', p_tenant_id, v_config_version), format('config://%s/%s', p_tenant_id, v_config_version), p_traceparent);
  RETURN QUERY SELECT v_config_version, v_current_version + 1;
END;
$$;

CREATE OR REPLACE FUNCTION update_tenant_configuration(
  p_tenant_id text, p_expected_version bigint, p_display_name text,
  p_request_limit_per_minute bigint, p_max_concurrent_executions integer,
  p_monthly_token_budget bigint, p_monthly_cost_budget_micros bigint,
  p_billing_currency char(3), p_audit_retention_days integer,
  p_audit_payload_mode text, p_log_masking_level text,
  p_trace_sampling_rate numeric, p_default_agent_app_id text,
  p_default_backend_profile_id text, p_active_config_version bigint,
  p_actor_id text, p_reason_code text, p_correlation_id text,
  p_trace_id text, p_traceparent text
) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
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

REVOKE ALL ON FUNCTION transition_tenant_status(text,bigint,text,text,text,text,text,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION publish_config_snapshot(text,bigint,integer,jsonb,text,text,text,text,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION publish_agent_app_revision(text,text,bigint,bigint,bigint,text,text,text,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION rollback_agent_app_revision(text,text,bigint,bigint,text,text,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION transition_agent_app_status(text,text,bigint,text,text,text,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION update_tenant_configuration(text,bigint,text,bigint,integer,bigint,bigint,character,integer,text,text,numeric,text,text,bigint,text,text,text,text,text) FROM PUBLIC;

COMMIT;
