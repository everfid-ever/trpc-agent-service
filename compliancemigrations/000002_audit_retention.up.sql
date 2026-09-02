BEGIN;

-- =============================================================================
-- 000002_audit_retention: retention policy, legal hold, quarantine resolution,
-- and the guarded purge path that is the ONLY legal exception to the immutable
-- compliance facts created by 000001.
-- =============================================================================

-- Roles. The purger role is the sole authority able to delete compliance facts,
-- and only through the SECURITY DEFINER functions below. It is NOLOGIN; the
-- deployment grants membership to the purge service account out of band.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'compliance_purger') THEN
    CREATE ROLE compliance_purger NOLOGIN;
  END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- Retention floor: per-class platform minimum, monotonic, never decreases.
-- ---------------------------------------------------------------------------
CREATE TABLE compliance.audit_retention_floor (
  class text NOT NULL CHECK (class IN ('default','security','billing')),
  min_retention_seconds bigint NOT NULL CHECK (min_retention_seconds > 0),
  floor_version bigint NOT NULL DEFAULT 1 CHECK (floor_version >= 1),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (class)
);

INSERT INTO compliance.audit_retention_floor(class, min_retention_seconds) VALUES
  ('default',  15552000),   -- 180 days
  ('security', 31536000),   -- 365 days
  ('billing',  315360000);  -- 10 years (365-day years)

CREATE FUNCTION compliance.guard_audit_retention_floor()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'retention floor is immutable' USING ERRCODE = '55000';
  END IF;
  IF TG_OP = 'UPDATE' THEN
    IF NEW.class <> OLD.class THEN
      RAISE EXCEPTION 'retention floor class is immutable' USING ERRCODE = '23000';
    END IF;
    IF NEW.floor_version <> OLD.floor_version + 1 THEN
      RAISE EXCEPTION 'retention floor version must advance exactly once' USING ERRCODE = '40001';
    END IF;
    IF NEW.min_retention_seconds < OLD.min_retention_seconds THEN
      RAISE EXCEPTION 'retention floor may only increase' USING ERRCODE = '23514';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER audit_retention_floor_guard BEFORE UPDATE OR DELETE ON compliance.audit_retention_floor
FOR EACH ROW EXECUTE FUNCTION compliance.guard_audit_retention_floor();

-- ---------------------------------------------------------------------------
-- Class rules: longest-prefix mapping from audit action to retention class.
-- ---------------------------------------------------------------------------
CREATE TABLE compliance.audit_retention_class_rule (
  action_prefix text NOT NULL CHECK (length(btrim(action_prefix)) BETWEEN 1 AND 128),
  class text NOT NULL CHECK (class IN ('default','security','billing')),
  PRIMARY KEY (action_prefix)
);

INSERT INTO compliance.audit_retention_class_rule(action_prefix, class) VALUES
  ('governance.', 'security'),
  ('artifact.quarantine', 'security'),
  ('tool_confirmation', 'security'),
  ('usage.', 'billing');

CREATE FUNCTION compliance.audit_retention_class(action text) RETURNS text
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
  SELECT COALESCE(
    (SELECT class FROM compliance.audit_retention_class_rule
     WHERE action LIKE action_prefix || '%'
     ORDER BY length(action_prefix) DESC LIMIT 1),
    'default')
$$;

-- ---------------------------------------------------------------------------
-- Per-tenant retention policy: append-only versioned ledger.
-- ---------------------------------------------------------------------------
CREATE TABLE compliance.audit_retention_policy (
  tenant_id text NOT NULL CHECK (length(btrim(tenant_id)) BETWEEN 1 AND 128),
  version bigint NOT NULL CHECK (version >= 1),
  retention_seconds bigint NOT NULL CHECK (retention_seconds > 0),
  actor text NOT NULL CHECK (length(btrim(actor)) BETWEEN 1 AND 128),
  reason text NOT NULL DEFAULT '' CHECK (length(reason) <= 1024),
  effective_from timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, version)
);

CREATE FUNCTION compliance.guard_audit_retention_policy()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF TG_OP <> 'INSERT' THEN
    RAISE EXCEPTION 'retention policy is append-only' USING ERRCODE = '55000';
  END IF;
  IF NEW.version <> 1 AND NEW.version <>
     (SELECT COALESCE(MAX(version),0)+1 FROM compliance.audit_retention_policy WHERE tenant_id = NEW.tenant_id) THEN
    RAISE EXCEPTION 'retention policy version must be contiguous' USING ERRCODE = '40001';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER audit_retention_policy_guard BEFORE INSERT OR UPDATE OR DELETE ON compliance.audit_retention_policy
FOR EACH ROW EXECUTE FUNCTION compliance.guard_audit_retention_policy();

CREATE FUNCTION compliance.audit_effective_retention(p_tenant text, p_class text) RETURNS bigint
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
  SELECT GREATEST(
    COALESCE((SELECT retention_seconds FROM compliance.audit_retention_policy
              WHERE tenant_id = p_tenant ORDER BY version DESC LIMIT 1), 0),
    COALESCE((SELECT min_retention_seconds FROM compliance.audit_retention_floor
              WHERE class = p_class), 0))
$$;

-- ---------------------------------------------------------------------------
-- Legal hold: append-only placed/released facts.
-- ---------------------------------------------------------------------------
CREATE TABLE compliance.audit_legal_hold (
  tenant_id text NOT NULL CHECK (length(btrim(tenant_id)) BETWEEN 1 AND 128),
  hold_id text NOT NULL CHECK (length(btrim(hold_id)) BETWEEN 1 AND 128),
  event text NOT NULL CHECK (event IN ('placed','released')),
  scope_start timestamptz,
  scope_end timestamptz,
  actor text NOT NULL CHECK (length(btrim(actor)) BETWEEN 1 AND 128),
  reason text NOT NULL DEFAULT '' CHECK (length(reason) <= 1024),
  at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, hold_id, event),
  CHECK (event = 'released' OR (scope_start IS NOT NULL AND scope_end IS NOT NULL AND scope_start < scope_end))
);

CREATE FUNCTION compliance.reject_any_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'compliance fact is immutable' USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER audit_legal_hold_immutable BEFORE UPDATE OR DELETE ON compliance.audit_legal_hold
FOR EACH ROW EXECUTE FUNCTION compliance.reject_any_change();

CREATE FUNCTION compliance.audit_event_on_hold(p_tenant text, p_at timestamptz) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
  SELECT EXISTS (
    SELECT 1 FROM compliance.audit_legal_hold h
    WHERE h.tenant_id = p_tenant AND h.event = 'placed'
      AND h.scope_start <= p_at AND p_at < h.scope_end
      AND NOT EXISTS (
        SELECT 1 FROM compliance.audit_legal_hold r
        WHERE r.tenant_id = h.tenant_id AND r.hold_id = h.hold_id AND r.event = 'released'
      )
  )
$$;

-- ---------------------------------------------------------------------------
-- Quarantine resolution: append-only closure fact for an open quarantine alert.
-- Governed by the guarded immutable trigger so the purge path may remove it
-- together with its backing alert and event.
-- ---------------------------------------------------------------------------
CREATE TABLE compliance.audit_quarantine_resolution (
  tenant_id text NOT NULL,
  audit_id text NOT NULL,
  resolved_by text NOT NULL CHECK (length(btrim(resolved_by)) BETWEEN 1 AND 128),
  reason text NOT NULL DEFAULT '' CHECK (length(reason) <= 1024),
  resolved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, audit_id),
  FOREIGN KEY (tenant_id, audit_id) REFERENCES compliance.audit_event(tenant_id, audit_id)
);

-- ---------------------------------------------------------------------------
-- Immutable change guard rework: UPDATE is always rejected; DELETE is allowed
-- ONLY when both the session is a compliance_purger member AND the transaction
-- set the local compliance.purge_authorized marker inside a SECURITY DEFINER
-- purge function. A caller may not grant itself delete by SETting the GUC.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION compliance.reject_immutable_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    RAISE EXCEPTION 'compliance fact is immutable' USING ERRCODE = '55000';
  END IF;
  IF current_setting('compliance.purge_authorized', true) IS DISTINCT FROM 'on'
     OR NOT pg_has_role(session_user, 'compliance_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'compliance fact is immutable' USING ERRCODE = '55000';
  END IF;
  RETURN OLD;
END;
$$;

CREATE TRIGGER audit_quarantine_resolution_immutable BEFORE UPDATE OR DELETE ON compliance.audit_quarantine_resolution
FOR EACH ROW EXECUTE FUNCTION compliance.reject_immutable_change();

-- ---------------------------------------------------------------------------
-- Purge batch: durable intent + guarded state machine.
-- ---------------------------------------------------------------------------
CREATE TABLE compliance.audit_purge_batch (
  tenant_id text NOT NULL CHECK (length(btrim(tenant_id)) BETWEEN 1 AND 128),
  batch_id text NOT NULL CHECK (length(btrim(batch_id)) BETWEEN 1 AND 128),
  state text NOT NULL CHECK (state IN ('planned','approved','executing','completed','failed','quarantined')),
  cutoff_at timestamptz NOT NULL,
  class text NOT NULL CHECK (class IN ('default','security','billing')),
  planned_count bigint NOT NULL DEFAULT 0 CHECK (planned_count >= 0),
  planned_digest text NOT NULL DEFAULT '' CHECK (planned_digest = '' OR planned_digest ~ '^[0-9a-f]{64}$'),
  verified_digest text NOT NULL DEFAULT '' CHECK (verified_digest = '' OR verified_digest ~ '^[0-9a-f]{64}$'),
  deleted_count bigint NOT NULL DEFAULT 0 CHECK (deleted_count >= 0),
  alert_count bigint NOT NULL DEFAULT 0 CHECK (alert_count >= 0),
  claim_owner text,
  claim_until timestamptz,
  delete_attempt integer NOT NULL DEFAULT 0 CHECK (delete_attempt >= 0),
  last_error_class text,
  not_before timestamptz,
  previewed_at timestamptz,
  ttl_until timestamptz,
  policy_version bigint,
  floor_version bigint,
  approved_by text,
  approved_at timestamptz,
  reason text NOT NULL DEFAULT '' CHECK (length(reason) <= 1024),
  version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, batch_id)
);

CREATE INDEX audit_purge_batch_state_idx
  ON compliance.audit_purge_batch(state, tenant_id, batch_id);

CREATE FUNCTION compliance.guard_audit_purge_batch_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE ok boolean;
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'purge batch is immutable' USING ERRCODE = '55000';
  END IF;
  IF (NEW.tenant_id,NEW.batch_id,NEW.cutoff_at,NEW.class,NEW.created_at)
     IS DISTINCT FROM (OLD.tenant_id,OLD.batch_id,OLD.cutoff_at,OLD.class,OLD.created_at) THEN
    RAISE EXCEPTION 'purge batch identity is immutable' USING ERRCODE = '23000';
  END IF;
  IF NEW.version <> OLD.version + 1 OR NEW.updated_at < OLD.updated_at THEN
    RAISE EXCEPTION 'purge batch version must advance exactly once' USING ERRCODE = '40001';
  END IF;
  IF (NEW.planned_count,NEW.planned_digest) IS DISTINCT FROM (OLD.planned_count,OLD.planned_digest) THEN
    RAISE EXCEPTION 'purge plan is immutable' USING ERRCODE = '23000';
  END IF;
  ok := false;
  IF NEW.state = OLD.state THEN
    ok := NEW.state IN ('planned','approved','executing','failed');
  ELSIF OLD.state = 'planned' AND NEW.state = 'approved' THEN ok := true;
  ELSIF OLD.state = 'approved' AND NEW.state = 'executing' THEN ok := true;
  ELSIF OLD.state = 'executing' AND NEW.state IN ('completed','failed','quarantined') THEN ok := true;
  ELSIF OLD.state = 'failed' AND NEW.state IN ('executing','quarantined') THEN ok := true;
  END IF;
  IF NOT ok THEN
    RAISE EXCEPTION 'illegal purge batch transition' USING ERRCODE = '23514';
  END IF;
  IF NEW.state = 'executing' AND (NEW.claim_owner IS NULL OR NEW.claim_until IS NULL) THEN
    RAISE EXCEPTION 'executing purge batch requires a claim' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER audit_purge_batch_guard BEFORE UPDATE OR DELETE ON compliance.audit_purge_batch
FOR EACH ROW EXECUTE FUNCTION compliance.guard_audit_purge_batch_update();

-- ---------------------------------------------------------------------------
-- Destruction certificate: immutable proof of what was destroyed.
-- ---------------------------------------------------------------------------
CREATE TABLE compliance.audit_purge_certificate (
  tenant_id text NOT NULL,
  batch_id text NOT NULL,
  from_occurred_at timestamptz NOT NULL,
  to_occurred_at timestamptz NOT NULL,
  count bigint NOT NULL CHECK (count >= 0),
  alert_count bigint NOT NULL CHECK (alert_count >= 0),
  event_digest text NOT NULL CHECK (event_digest ~ '^[0-9a-f]{64}$'),
  policy_version bigint NOT NULL,
  floor_version bigint NOT NULL,
  class text NOT NULL CHECK (class IN ('default','security','billing')),
  approved_by text NOT NULL,
  reason text NOT NULL DEFAULT '',
  executed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, batch_id)
);
CREATE TRIGGER audit_purge_certificate_immutable BEFORE UPDATE OR DELETE ON compliance.audit_purge_certificate
FOR EACH ROW EXECUTE FUNCTION compliance.reject_any_change();

-- ---------------------------------------------------------------------------
-- Query secondary-audit record: immutable, NEVER purged by the event path.
-- ---------------------------------------------------------------------------
CREATE TABLE compliance.audit_query_record (
  query_id text NOT NULL CHECK (length(btrim(query_id)) BETWEEN 1 AND 128),
  tenant_id text NOT NULL CHECK (length(btrim(tenant_id)) BETWEEN 1 AND 128),
  subject text NOT NULL CHECK (length(btrim(subject)) BETWEEN 1 AND 256),
  cross_tenant boolean NOT NULL,
  from_occurred_at timestamptz NOT NULL,
  to_occurred_at timestamptz NOT NULL,
  filter_digest text NOT NULL CHECK (filter_digest ~ '^[0-9a-f]{64}$'),
  result_count bigint NOT NULL CHECK (result_count >= 0),
  result_digest text NOT NULL CHECK (result_digest ~ '^[0-9a-f]{64}$'),
  decision text NOT NULL CHECK (decision IN ('allowed','denied')),
  reason_code text NOT NULL DEFAULT '',
  trace_id text NOT NULL DEFAULT '',
  occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (query_id)
);
CREATE TRIGGER audit_query_record_immutable BEFORE UPDATE OR DELETE ON compliance.audit_query_record
FOR EACH ROW EXECUTE FUNCTION compliance.reject_any_change();

-- Cross-tenant query pagination needs a globally total ordering. The original
-- tenant-leading index remains optimal for the normal tenant-scoped path.
CREATE INDEX compliance_audit_event_global_time_idx
  ON compliance.audit_event(occurred_at DESC, tenant_id DESC, audit_id DESC);

-- =============================================================================
-- Guarded purge functions. Each is SECURITY DEFINER with a fixed search_path,
-- EXECUTE revoked from PUBLIC and granted only to compliance_purger.
-- =============================================================================

CREATE FUNCTION compliance.plan_audit_purge_batch(
  p_tenant text, p_class text, p_cutoff timestamptz, p_actor text, p_reason text, p_ttl interval, p_max bigint)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, compliance AS $$
DECLARE
  v_batch text;
  v_count bigint;
  v_digest text;
  v_policy bigint;
  v_floor bigint;
BEGIN
  IF NOT pg_has_role(session_user, 'compliance_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to plan purge' USING ERRCODE = '42501';
  END IF;
  IF p_tenant IS NULL OR btrim(p_tenant) = '' OR p_class NOT IN ('default','security','billing')
     OR p_actor IS NULL OR btrim(p_actor) = '' OR p_ttl <= interval '0 seconds' OR p_max < 1 THEN
    RAISE EXCEPTION 'invalid purge plan input' USING ERRCODE = '22023';
  END IF;
  IF p_cutoff > clock_timestamp() - make_interval(
       secs => compliance.audit_effective_retention(p_tenant, p_class)) THEN
    RAISE EXCEPTION 'purge cutoff violates effective retention' USING ERRCODE = '23514';
  END IF;
  v_batch := 'apb_' || encode(sha256(convert_to(
    'audit-purge-v1' || chr(31) || p_tenant || chr(31) || p_class || chr(31) || (EXTRACT(EPOCH FROM p_cutoff))::bigint::text,
    'UTF8')), 'hex');
  IF EXISTS (SELECT 1 FROM compliance.audit_purge_batch WHERE tenant_id = p_tenant AND batch_id = v_batch) THEN
    RETURN v_batch;
  END IF;

  SELECT count(*),
         COALESCE(encode(sha256(convert_to(string_agg(e.tenant_id || ':' || e.audit_id, E'\n'
                 ORDER BY e.occurred_at, e.audit_id), 'UTF8')), 'hex'),
                  encode(sha256(convert_to('', 'UTF8')), 'hex'))
    INTO v_count, v_digest
    FROM compliance.audit_event e
    WHERE e.tenant_id = p_tenant
      AND e.occurred_at < p_cutoff
      AND compliance.audit_retention_class(e.event_json->>'action') = p_class
      AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at);

  -- Refuse to plan a batch too large to execute in one bounded transaction;
  -- the operator narrows the cutoff window instead.
  IF v_count > p_max THEN
    RAISE EXCEPTION 'purge candidate set exceeds max batch size' USING ERRCODE = '23514';
  END IF;

  SELECT COALESCE((SELECT version FROM compliance.audit_retention_policy
                   WHERE tenant_id = p_tenant ORDER BY version DESC LIMIT 1), 0) INTO v_policy;
  SELECT floor_version FROM compliance.audit_retention_floor WHERE class = p_class INTO v_floor;

  INSERT INTO compliance.audit_purge_batch(
    tenant_id,batch_id,state,cutoff_at,class,planned_count,planned_digest,
    policy_version,floor_version,previewed_at,ttl_until,reason)
  VALUES (p_tenant, v_batch, 'planned', p_cutoff, p_class, v_count, v_digest,
          v_policy, v_floor, clock_timestamp(), clock_timestamp() + p_ttl, p_reason);
  RETURN v_batch;
END;
$$;
REVOKE ALL ON FUNCTION compliance.plan_audit_purge_batch(text,text,timestamptz,text,text,interval,bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION compliance.plan_audit_purge_batch(text,text,timestamptz,text,text,interval,bigint) TO compliance_purger;

CREATE FUNCTION compliance.approve_audit_purge_batch(p_tenant text, p_batch text, p_approver text, p_reason text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, compliance AS $$
BEGIN
  IF NOT pg_has_role(session_user, 'compliance_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to approve purge' USING ERRCODE = '42501';
  END IF;
  UPDATE compliance.audit_purge_batch SET
    state = 'approved',
    approved_by = p_approver,
    approved_at = clock_timestamp(),
    reason = CASE WHEN btrim(p_reason) <> '' THEN p_reason ELSE reason END,
    version = version + 1,
    updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch AND state = 'planned';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'purge batch is not approvable' USING ERRCODE = '23514';
  END IF;
END;
$$;
REVOKE ALL ON FUNCTION compliance.approve_audit_purge_batch(text,text,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION compliance.approve_audit_purge_batch(text,text,text,text) TO compliance_purger;

CREATE FUNCTION compliance.execute_audit_purge_batch(p_tenant text, p_batch text, p_owner text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, compliance AS $$
DECLARE
  b compliance.audit_purge_batch%ROWTYPE;
  v_count bigint;
  v_digest text;
  v_from timestamptz;
  v_to timestamptz;
  v_alert_count bigint := 0;
  v_deleted bigint;
  v_chunk int := 1000;
  v_policy bigint;
  v_floor bigint;
  v_ids text[];
BEGIN
  IF NOT pg_has_role(session_user, 'compliance_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to execute purge' USING ERRCODE = '42501';
  END IF;

  SELECT * INTO b FROM compliance.audit_purge_batch
    WHERE tenant_id = p_tenant AND batch_id = p_batch FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'purge batch not found' USING ERRCODE = '02000';
  END IF;
  IF b.state = 'completed' THEN
    RETURN 'completed';
  END IF;
  IF b.state NOT IN ('approved','executing','failed') THEN
    RETURN 'not_executable';
  END IF;
  IF b.state = 'executing'
     AND b.claim_owner IS DISTINCT FROM p_owner
     AND (b.claim_until IS NULL OR b.claim_until >= clock_timestamp()) THEN
    RETURN 'claimed_by_another';
  END IF;
  IF b.state = 'failed' AND b.not_before IS NOT NULL AND b.not_before > clock_timestamp() THEN
    RETURN 'backoff';
  END IF;

  SELECT COALESCE((SELECT version FROM compliance.audit_retention_policy
                   WHERE tenant_id = b.tenant_id ORDER BY version DESC LIMIT 1), 0) INTO v_policy;
  SELECT floor_version FROM compliance.audit_retention_floor WHERE class = b.class INTO v_floor;
  IF v_policy IS DISTINCT FROM b.policy_version OR v_floor IS DISTINCT FROM b.floor_version
     OR b.cutoff_at > clock_timestamp() - make_interval(
          secs => compliance.audit_effective_retention(b.tenant_id, b.class)) THEN
    IF b.state = 'approved' THEN
      UPDATE compliance.audit_purge_batch SET state = 'executing',
        claim_owner = p_owner, claim_until = clock_timestamp() + interval '5 minutes',
        version = version + 1, updated_at = clock_timestamp()
        WHERE tenant_id = p_tenant AND batch_id = p_batch;
    END IF;
    UPDATE compliance.audit_purge_batch SET state = 'failed', last_error_class = 'retention_changed',
      delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'retention_changed';
  END IF;

  -- Advance to executing before any validation branch may set failed, keeping
  -- the state machine transitions legal (approved -> executing -> ...). The
  -- deterministic failure branches below persist their state and RETURN a
  -- result code instead of RAISE so the transition is not rolled back.
  UPDATE compliance.audit_purge_batch SET
    state = 'executing',
    claim_owner = p_owner,
    claim_until = clock_timestamp() + interval '5 minutes',
    version = version + 1,
    updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch;

  IF b.ttl_until IS NOT NULL AND b.ttl_until < clock_timestamp() THEN
    UPDATE compliance.audit_purge_batch SET state = 'failed', last_error_class = 'preview_expired',
      delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'preview_expired';
  END IF;

  -- Recompute the candidate set; any divergence from the plan is fail closed.
  -- Freeze and row-lock the exact candidate IDs before computing the digest.
  -- Delayed relay inserts with historical occurred_at are left for a later
  -- batch instead of being deleted without appearing in this certificate.
  SELECT ARRAY(
    SELECT e.audit_id FROM compliance.audit_event e
    WHERE e.tenant_id = p_tenant
      AND e.occurred_at < b.cutoff_at
      AND compliance.audit_retention_class(e.event_json->>'action') = b.class
      AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at)
    ORDER BY e.occurred_at, e.audit_id
    FOR UPDATE) INTO v_ids;

  SELECT count(*), COALESCE(min(occurred_at), b.cutoff_at), COALESCE(max(occurred_at), b.cutoff_at),
         COALESCE(encode(sha256(convert_to(string_agg(e.tenant_id || ':' || e.audit_id, E'\n'
                 ORDER BY e.occurred_at, e.audit_id), 'UTF8')), 'hex'),
                  encode(sha256(convert_to('', 'UTF8')), 'hex'))
    INTO v_count, v_from, v_to, v_digest
    FROM compliance.audit_event e
    WHERE e.tenant_id = p_tenant
      AND e.audit_id = ANY(v_ids);

  IF v_count <> b.planned_count OR v_digest <> b.planned_digest THEN
    UPDATE compliance.audit_purge_batch SET state = 'failed', last_error_class = 'divergence',
      verified_digest = v_digest, delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'divergence';
  END IF;

  -- Refuse if any candidate backs an open quarantine alert without resolution.
  IF EXISTS (
    SELECT 1 FROM compliance.quarantine_alert q
    WHERE q.tenant_id = p_tenant
      AND EXISTS (
        SELECT 1 FROM compliance.audit_event e
        WHERE e.tenant_id = q.tenant_id AND e.audit_id = q.audit_id
          AND e.occurred_at < b.cutoff_at
          AND compliance.audit_retention_class(e.event_json->>'action') = b.class
          AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at))
      AND NOT EXISTS (
        SELECT 1 FROM compliance.audit_quarantine_resolution r
        WHERE r.tenant_id = q.tenant_id AND r.audit_id = q.audit_id)
  ) THEN
    UPDATE compliance.audit_purge_batch SET state = 'failed', last_error_class = 'unresolved_quarantine',
      delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'unresolved_quarantine';
  END IF;

  -- Guarded deletion: children (resolution + alert) first, then the event.
  -- Every statement re-applies the hold predicate so legal-hold events keep
  -- their quarantine evidence while the parent is retained.
  PERFORM set_config('compliance.purge_authorized', 'on', true);

  DELETE FROM compliance.audit_quarantine_resolution r
    USING compliance.audit_event e
    WHERE r.tenant_id = p_tenant AND e.tenant_id = p_tenant AND r.audit_id = e.audit_id
      AND e.audit_id = ANY(v_ids)
      AND e.occurred_at < b.cutoff_at
      AND compliance.audit_retention_class(e.event_json->>'action') = b.class
      AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at);

  DELETE FROM compliance.quarantine_alert q
    USING compliance.audit_event e
    WHERE q.tenant_id = p_tenant AND e.tenant_id = p_tenant AND q.audit_id = e.audit_id
      AND e.audit_id = ANY(v_ids)
      AND e.occurred_at < b.cutoff_at
      AND compliance.audit_retention_class(e.event_json->>'action') = b.class
      AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at);
  GET DIAGNOSTICS v_alert_count = ROW_COUNT;

  -- Chunked event deletion keeps any single DELETE statement bounded.
  v_count := 0;
  LOOP
    DELETE FROM compliance.audit_event e
      WHERE e.tenant_id = p_tenant
        AND e.occurred_at < b.cutoff_at
        AND e.audit_id = ANY(v_ids)
        AND compliance.audit_retention_class(e.event_json->>'action') = b.class
        AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at)
        AND e.ctid IN (
          SELECT e2.ctid FROM compliance.audit_event e2
          WHERE e2.tenant_id = p_tenant
            AND e2.occurred_at < b.cutoff_at
            AND e2.audit_id = ANY(v_ids)
            AND compliance.audit_retention_class(e2.event_json->>'action') = b.class
            AND NOT compliance.audit_event_on_hold(e2.tenant_id, e2.occurred_at)
          LIMIT v_chunk
        );
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    v_count := v_count + v_deleted;
    EXIT WHEN v_deleted = 0;
  END LOOP;

  INSERT INTO compliance.audit_purge_certificate(
    tenant_id,batch_id,from_occurred_at,to_occurred_at,count,alert_count,event_digest,
    policy_version,floor_version,class,approved_by,reason)
  VALUES (p_tenant, b.batch_id, v_from, v_to, v_count, v_alert_count, v_digest,
          b.policy_version, b.floor_version, b.class, COALESCE(b.approved_by, ''), b.reason)
  ON CONFLICT (tenant_id,batch_id) DO NOTHING;

  UPDATE compliance.audit_purge_batch SET
    state = 'completed', deleted_count = v_count, alert_count = v_alert_count,
    version = version + 1, updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch;

  RETURN 'completed';
END;
$$;
REVOKE ALL ON FUNCTION compliance.execute_audit_purge_batch(text,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION compliance.execute_audit_purge_batch(text,text,text) TO compliance_purger;

-- Terminal quarantine: the reconciler gives up on a batch after repeated
-- deterministic failures. Only executable or failed batches may be quarantined.
CREATE FUNCTION compliance.quarantine_audit_purge_batch(p_tenant text, p_batch text, p_owner text, p_error text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, compliance AS $$
BEGIN
  IF NOT pg_has_role(session_user, 'compliance_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to quarantine purge' USING ERRCODE = '42501';
  END IF;
  UPDATE compliance.audit_purge_batch SET
    state = 'quarantined', last_error_class = p_error,
    version = version + 1, updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch AND state IN ('executing','failed');
  IF NOT FOUND THEN
    RAISE EXCEPTION 'purge batch is not quarantinable' USING ERRCODE = '23514';
  END IF;
END;
$$;
REVOKE ALL ON FUNCTION compliance.quarantine_audit_purge_batch(text,text,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION compliance.quarantine_audit_purge_batch(text,text,text,text) TO compliance_purger;

COMMIT;
