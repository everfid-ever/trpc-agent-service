BEGIN;

-- Business-library audit retention with a relay watermark gate.
--
-- outbox(kind='audit').state=published is the sole authority that an audit
-- fact has been exported to the compliance database. The watermark is the
-- earliest created_at of an un-exported outbox row (empty set means fully
-- exported). Deletion is allowed only inside occurred_at/created_at <
-- min(retention_cutoff, watermark): the business action writes audit_event
-- before outbox in one transaction, so occurred_at <= created_at, and every
-- audit_event inside the window has an already-published outbox and therefore
-- a compliance copy. Un-exported outbox rows are never deleted.
--
-- audit_event.audit_id is a one-way hash of outbox_id, so retention must use
-- the time-window gate rather than a reverse join.

DO $do$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'audit_retention_purger') THEN
    CREATE ROLE audit_retention_purger NOLOGIN;
  END IF;
END
$do$;

-- Replace the 000026 immutable guard: UPDATE stays forbidden; DELETE is
-- permitted only inside a SECURITY DEFINER purge function that both sets the
-- transaction-local audit.purge_authorized marker and runs as an
-- audit_retention_purger member. A caller cannot grant itself delete by
-- SETting the GUC alone.
CREATE OR REPLACE FUNCTION public.reject_audit_event_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    RAISE EXCEPTION 'audit event is immutable' USING ERRCODE = '55000';
  END IF;
  IF current_setting('audit.purge_authorized', true) IS DISTINCT FROM 'on'
     OR NOT pg_has_role(session_user, 'audit_retention_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'audit event is immutable' USING ERRCODE = '55000';
  END IF;
  RETURN OLD;
END;
$$;

CREATE TABLE public.business_audit_purge_batch (
  tenant_id text NOT NULL,
  batch_id text NOT NULL,
  state text NOT NULL DEFAULT 'planned' CHECK (state IN ('planned','executing','completed','failed','quarantined')),
  cutoff_at timestamptz NOT NULL,
  watermark_at timestamptz,
  safe_cutoff_at timestamptz NOT NULL,
  planned_events bigint NOT NULL DEFAULT 0 CHECK (planned_events >= 0),
  planned_outbox bigint NOT NULL DEFAULT 0 CHECK (planned_outbox >= 0),
  planned_digest text NOT NULL DEFAULT '' CHECK (planned_digest = '' OR planned_digest ~ '^[0-9a-f]{64}$'),
  deleted_events bigint NOT NULL DEFAULT 0 CHECK (deleted_events >= 0),
  deleted_outbox bigint NOT NULL DEFAULT 0 CHECK (deleted_outbox >= 0),
  delete_attempt integer NOT NULL DEFAULT 0 CHECK (delete_attempt >= 0),
  last_error_class text NOT NULL DEFAULT '',
  claim_owner text NOT NULL DEFAULT '',
  claim_until timestamptz,
  not_before timestamptz NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
  PRIMARY KEY (tenant_id, batch_id),
  CHECK (length(btrim(batch_id)) BETWEEN 1 AND 128),
  CHECK (updated_at >= created_at)
);

CREATE TABLE public.business_audit_purge_certificate (
  tenant_id text NOT NULL,
  batch_id text NOT NULL,
  from_occurred_at timestamptz NOT NULL,
  to_occurred_at timestamptz NOT NULL,
  event_count bigint NOT NULL,
  outbox_count bigint NOT NULL,
  watermark_at timestamptz,
  event_digest text NOT NULL CHECK (event_digest ~ '^[0-9a-f]{64}$'),
  approved_by text NOT NULL,
  reason text NOT NULL DEFAULT '',
  PRIMARY KEY (tenant_id, batch_id),
  FOREIGN KEY (tenant_id, batch_id) REFERENCES public.business_audit_purge_batch(tenant_id, batch_id)
);

CREATE FUNCTION public.guard_business_audit_purge_batch_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE ok boolean;
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'business audit purge batch is immutable' USING ERRCODE='55000';
  END IF;
  IF (NEW.tenant_id,NEW.batch_id,NEW.cutoff_at,NEW.watermark_at,NEW.safe_cutoff_at,NEW.planned_events,
      NEW.planned_outbox,NEW.planned_digest,NEW.created_at)
    IS DISTINCT FROM
     (OLD.tenant_id,OLD.batch_id,OLD.cutoff_at,OLD.watermark_at,OLD.safe_cutoff_at,OLD.planned_events,
      OLD.planned_outbox,OLD.planned_digest,OLD.created_at) THEN
    RAISE EXCEPTION 'business audit purge batch identity is immutable' USING ERRCODE='23000';
  END IF;
  IF NEW.version <> OLD.version + 1 OR NEW.updated_at < OLD.updated_at THEN
    RAISE EXCEPTION 'business audit purge batch version must advance exactly once' USING ERRCODE='40001';
  END IF;
  ok := false;
  IF NEW.state = OLD.state THEN
    ok := NEW.state IN ('planned','executing','failed');
  ELSIF OLD.state='planned' AND NEW.state IN ('executing','failed') THEN
    ok := true;
  ELSIF OLD.state='executing' AND NEW.state IN ('completed','failed','quarantined') THEN
    ok := true;
  ELSIF OLD.state='failed' AND NEW.state IN ('executing','quarantined') THEN
    ok := true;
  END IF;
  IF NOT ok THEN
    RAISE EXCEPTION 'illegal business audit purge batch transition' USING ERRCODE='23514';
  END IF;
  IF NEW.state = 'executing' AND (NEW.claim_owner = '' OR NEW.claim_until IS NULL) THEN
    RAISE EXCEPTION 'executing business audit purge batch requires a claim' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER business_audit_purge_batch_guard
BEFORE UPDATE OR DELETE ON public.business_audit_purge_batch
FOR EACH ROW EXECUTE FUNCTION public.guard_business_audit_purge_batch_update();

CREATE FUNCTION public.reject_business_audit_purge_certificate_change()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'business audit purge certificate is immutable' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER business_audit_purge_certificate_immutable
BEFORE UPDATE OR DELETE ON public.business_audit_purge_certificate
FOR EACH ROW EXECUTE FUNCTION public.reject_business_audit_purge_certificate_change();

CREATE FUNCTION public.business_audit_watermark(p_tenant text)
RETURNS timestamptz LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
  SELECT min(created_at) FROM public.outbox
  WHERE tenant_id = p_tenant AND kind = 'audit' AND state <> 'published';
$$;

CREATE FUNCTION public.plan_business_audit_purge(
  p_tenant text, p_cutoff timestamptz, p_actor text, p_reason text, p_now timestamptz
) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  v_batch text;
  v_watermark timestamptz;
  v_safe timestamptz;
  v_events bigint;
  v_outbox bigint;
  v_digest text;
BEGIN
  IF NOT pg_has_role(session_user, 'audit_retention_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to plan business audit purge' USING ERRCODE = '42501';
  END IF;
  IF p_tenant IS NULL OR length(btrim(p_tenant))=0 OR p_cutoff IS NULL OR p_now IS NULL OR
     length(btrim(p_actor))=0 OR length(btrim(p_reason))=0 THEN
    RAISE EXCEPTION 'business audit purge input is invalid' USING ERRCODE = '22023';
  END IF;

  v_batch := 'bap_' || encode(sha256(convert_to(
    'business-audit-purge-v1' || chr(31) || p_tenant || chr(31) || (EXTRACT(EPOCH FROM p_cutoff))::bigint::text,
    'UTF8')), 'hex');
  IF EXISTS (SELECT 1 FROM public.business_audit_purge_batch WHERE tenant_id = p_tenant AND batch_id = v_batch) THEN
    RETURN v_batch;
  END IF;

  v_watermark := public.business_audit_watermark(p_tenant);
  v_safe := p_cutoff;
  IF v_watermark IS NOT NULL AND v_watermark < p_cutoff THEN
    v_safe := v_watermark;
  END IF;

  SELECT count(*) INTO v_events FROM public.audit_event e
    WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe;
  SELECT count(*) INTO v_outbox FROM public.outbox o
    WHERE o.tenant_id = p_tenant AND o.kind = 'audit' AND o.state = 'published' AND o.created_at < v_safe;
  SELECT COALESCE(encode(sha256(convert_to(string_agg(e.audit_id, chr(10) ORDER BY e.occurred_at, e.audit_id), 'UTF8')), 'hex'),
                  encode(sha256(convert_to('', 'UTF8')), 'hex'))
    INTO v_digest
    FROM public.audit_event e
    WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe;

  INSERT INTO public.business_audit_purge_batch(tenant_id,batch_id,state,cutoff_at,watermark_at,safe_cutoff_at,
    planned_events,planned_outbox,planned_digest,not_before,created_at,updated_at)
  VALUES(p_tenant, v_batch, 'planned', p_cutoff, v_watermark, v_safe,
    v_events, v_outbox, v_digest, p_now, p_now, p_now);
  RETURN v_batch;
END;
$$;
REVOKE ALL ON FUNCTION public.plan_business_audit_purge(text,timestamptz,text,text,timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.plan_business_audit_purge(text,timestamptz,text,text,timestamptz) TO audit_retention_purger;

CREATE FUNCTION public.execute_business_audit_purge(p_tenant text, p_batch text, p_owner text, p_chunk bigint)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  b public.business_audit_purge_batch%ROWTYPE;
  v_watermark timestamptz;
  v_safe timestamptz;
  v_events bigint;
  v_outbox bigint;
  v_digest text;
  v_from timestamptz;
  v_to timestamptz;
  v_deleted bigint;
BEGIN
  IF NOT pg_has_role(session_user, 'audit_retention_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to execute business audit purge' USING ERRCODE = '42501';
  END IF;
  IF p_tenant IS NULL OR btrim(p_tenant) = '' OR p_batch IS NULL OR btrim(p_batch) = '' OR
     p_owner IS NULL OR btrim(p_owner) = '' OR p_chunk IS NULL OR p_chunk < 1 OR p_chunk > 1000000 THEN
    RAISE EXCEPTION 'business audit purge execution input is invalid' USING ERRCODE = '22023';
  END IF;

  SELECT * INTO b FROM public.business_audit_purge_batch
    WHERE tenant_id = p_tenant AND batch_id = p_batch FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'business audit purge batch not found' USING ERRCODE = '02000';
  END IF;
  IF b.state = 'completed' THEN
    RETURN 'completed';
  END IF;
  IF b.state NOT IN ('planned','executing','failed') THEN
    RAISE EXCEPTION 'business audit purge batch is not executable' USING ERRCODE = '23514';
  END IF;
  IF b.not_before > clock_timestamp() THEN
    RETURN 'not_before';
  END IF;
  IF b.state = 'executing'
     AND b.claim_owner IS DISTINCT FROM p_owner
     AND (b.claim_until IS NULL OR b.claim_until >= clock_timestamp()) THEN
    RETURN 'claimed_by_another';
  END IF;

  UPDATE public.business_audit_purge_batch SET
    state = 'executing', claim_owner = p_owner,
    claim_until = clock_timestamp() + interval '5 minutes',
    version = version + 1, updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch;

  -- Recompute the watermark and candidate set; any divergence from the plan
  -- fails closed and never deletes.
  v_watermark := public.business_audit_watermark(p_tenant);
  v_safe := b.cutoff_at;
  IF v_watermark IS NOT NULL AND v_watermark < b.cutoff_at THEN
    v_safe := v_watermark;
  END IF;
  IF v_safe <> b.safe_cutoff_at THEN
    UPDATE public.business_audit_purge_batch SET state = 'failed', last_error_class = 'watermark_drift',
      delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'watermark_drift';
  END IF;

  SELECT count(*) INTO v_events FROM public.audit_event e
    WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe;
  SELECT count(*) INTO v_outbox FROM public.outbox o
    WHERE o.tenant_id = p_tenant AND o.kind = 'audit' AND o.state = 'published' AND o.created_at < v_safe;
  SELECT COALESCE(encode(sha256(convert_to(string_agg(e.audit_id, chr(10) ORDER BY e.occurred_at, e.audit_id), 'UTF8')), 'hex'),
                  encode(sha256(convert_to('', 'UTF8')), 'hex'))
    INTO v_digest
    FROM public.audit_event e
    WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe;
  IF v_events <> b.planned_events OR v_outbox <> b.planned_outbox OR v_digest <> b.planned_digest THEN
    UPDATE public.business_audit_purge_batch SET state = 'failed', last_error_class = 'divergence',
      delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'divergence';
  END IF;

  -- Guarded deletion via the transaction-local marker. Un-exported outbox rows
  -- are excluded by the state predicate and never deleted.
  PERFORM set_config('audit.purge_authorized', 'on', true);

  SELECT COALESCE(min(occurred_at), v_safe), COALESCE(max(occurred_at), v_safe)
    INTO v_from, v_to FROM public.audit_event e
    WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe;

  v_deleted := 0;
  LOOP
    WITH doomed AS (
      SELECT e.audit_id FROM public.audit_event e
      WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe
      ORDER BY e.occurred_at, e.audit_id LIMIT p_chunk
    )
    DELETE FROM public.audit_event e USING doomed d WHERE e.tenant_id = p_tenant AND e.audit_id = d.audit_id;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    EXIT WHEN v_deleted = 0;
  END LOOP;

  DELETE FROM public.outbox o
    WHERE o.tenant_id = p_tenant AND o.kind = 'audit' AND o.state = 'published' AND o.created_at < v_safe;

  INSERT INTO public.business_audit_purge_certificate(tenant_id,batch_id,from_occurred_at,to_occurred_at,
    event_count,outbox_count,watermark_at,event_digest,approved_by,reason)
  VALUES(p_tenant, b.batch_id, v_from, v_to, v_events, v_outbox, b.watermark_at, v_digest, p_owner, 'retention');

  UPDATE public.business_audit_purge_batch SET state = 'completed',
    deleted_events = v_events, deleted_outbox = v_outbox,
    version = version + 1, updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch;

  RETURN 'completed';
END;
$$;
REVOKE ALL ON FUNCTION public.execute_business_audit_purge(text,text,text,bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.execute_business_audit_purge(text,text,text,bigint) TO audit_retention_purger;

CREATE FUNCTION public.quarantine_business_audit_purge(p_tenant text, p_batch text, p_owner text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE b public.business_audit_purge_batch%ROWTYPE;
BEGIN
  IF NOT pg_has_role(session_user, 'audit_retention_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to quarantine business audit purge' USING ERRCODE = '42501';
  END IF;
  IF p_tenant IS NULL OR btrim(p_tenant) = '' OR p_batch IS NULL OR btrim(p_batch) = '' OR p_owner IS NULL OR btrim(p_owner) = '' THEN
    RAISE EXCEPTION 'business audit purge quarantine input is invalid' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO b FROM public.business_audit_purge_batch WHERE tenant_id=p_tenant AND batch_id=p_batch FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'business audit purge batch not found' USING ERRCODE = '02000';
  END IF;
  IF b.state = 'quarantined' THEN
    RETURN;
  END IF;
  IF b.state NOT IN ('executing','failed') THEN
    RAISE EXCEPTION 'business audit purge batch is not quarantinable' USING ERRCODE = '23514';
  END IF;
  UPDATE public.business_audit_purge_batch SET state='quarantined',claim_owner=p_owner,claim_until=NULL,
    last_error_class=CASE WHEN last_error_class='' THEN 'max_attempts' ELSE last_error_class END,
    version=version+1,updated_at=clock_timestamp()
  WHERE tenant_id=p_tenant AND batch_id=p_batch;
END;
$$;
REVOKE ALL ON FUNCTION public.quarantine_business_audit_purge(text,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.quarantine_business_audit_purge(text,text,text) TO audit_retention_purger;

REVOKE ALL ON FUNCTION public.guard_business_audit_purge_batch_update() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.reject_business_audit_purge_certificate_change() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.reject_audit_event_change() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.business_audit_watermark(text) FROM PUBLIC;

COMMIT;
