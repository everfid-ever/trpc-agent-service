BEGIN;

-- Knowledge ingestion/version publish lifecycle durable authority.
--
-- knowledge_manifest is the ingestion task entry and published-version
-- authority: the normalized source manifest fixes source digest, chunking
-- pipeline version, embedder profile/version, vector collection generation,
-- the metadata schema allowed back to the model, and the content watermark.
-- The state machine is staging -> indexing -> verifying -> published, with
-- failed reachable from any non-terminal state. Agent Revisions may only
-- reference published versions.
--
-- knowledge_chunk is the chunk authority; indexed_at marks that the chunk has
-- been indexed into the vector backend. knowledge_probe is the retrieval
-- sample-verification authority. Both block publish until satisfied.
--
-- Ingestion writes a chunk through stage_knowledge_chunk, which records a
-- forward mutation intent in the same source-DB transaction whenever an active
-- knowledge backend migration is in flight, so the migration repair loop picks
-- up chunks ingested during the migration window.

CREATE TABLE public.knowledge_manifest (
  tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
  knowledge_id text NOT NULL,
  version bigint NOT NULL CHECK (version >= 1),
  source_uri text NOT NULL,
  source_digest text NOT NULL CHECK (source_digest ~ '^[0-9a-f]{64}$'),
  chunking_pipeline_version text NOT NULL,
  embedder_profile_id text NOT NULL,
  embedder_version bigint NOT NULL CHECK (embedder_version >= 1),
  vector_collection_generation text NOT NULL,
  metadata_schema jsonb NOT NULL,
  content_watermark text NOT NULL,
  state text NOT NULL DEFAULT 'staging' CHECK (state IN ('staging','indexing','verifying','published','failed')),
  chunk_total bigint CHECK (chunk_total IS NULL OR chunk_total >= 1),
  verification_digest text NOT NULL DEFAULT '' CHECK (verification_digest = '' OR verification_digest ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  record_version bigint NOT NULL DEFAULT 1 CHECK (record_version >= 1),
  PRIMARY KEY (tenant_id, knowledge_id, version),
  CHECK (length(btrim(knowledge_id)) BETWEEN 1 AND 256),
  CHECK (length(btrim(source_uri)) BETWEEN 1 AND 2048),
  CHECK (length(btrim(embedder_profile_id)) BETWEEN 1 AND 256),
  CHECK (length(btrim(vector_collection_generation)) BETWEEN 1 AND 256),
  CHECK (updated_at >= created_at)
);

CREATE INDEX knowledge_manifest_state_idx
  ON public.knowledge_manifest(tenant_id, state);

CREATE TABLE public.knowledge_chunk (
  tenant_id text NOT NULL,
  knowledge_id text NOT NULL,
  knowledge_version bigint NOT NULL CHECK (knowledge_version >= 1),
  chunk_id text NOT NULL,
  source_digest text NOT NULL CHECK (source_digest ~ '^[0-9a-f]{64}$'),
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  metadata_digest text NOT NULL CHECK (metadata_digest ~ '^[0-9a-f]{64}$'),
  embedding_profile_id text NOT NULL,
  embedding_version bigint NOT NULL CHECK (embedding_version >= 1),
  vector_generation text NOT NULL,
  image_digest text NOT NULL CHECK (image_digest ~ '^[0-9a-f]{64}$'),
  content text NOT NULL,
  metadata jsonb NOT NULL,
  vector jsonb NOT NULL,
  indexed_at timestamptz,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, knowledge_id, knowledge_version, chunk_id),
  FOREIGN KEY (tenant_id, knowledge_id, knowledge_version)
    REFERENCES public.knowledge_manifest(tenant_id, knowledge_id, version),
  CHECK (length(btrim(chunk_id)) BETWEEN 1 AND 512),
  CHECK (length(btrim(embedding_profile_id)) BETWEEN 1 AND 256),
  CHECK (length(btrim(vector_generation)) BETWEEN 1 AND 256)
);

CREATE TABLE public.knowledge_probe (
  tenant_id text NOT NULL,
  knowledge_id text NOT NULL,
  knowledge_version bigint NOT NULL CHECK (knowledge_version >= 1),
  probe_id text NOT NULL,
  query text NOT NULL,
  expected_chunks jsonb NOT NULL,
  min_recall_ppm bigint NOT NULL CHECK (min_recall_ppm BETWEEN 1 AND 1000000),
  verified boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, knowledge_id, knowledge_version, probe_id),
  FOREIGN KEY (tenant_id, knowledge_id, knowledge_version)
    REFERENCES public.knowledge_manifest(tenant_id, knowledge_id, version),
  CHECK (length(btrim(probe_id)) BETWEEN 1 AND 128),
  CHECK (length(btrim(query)) BETWEEN 1 AND 4096)
);

CREATE FUNCTION public.guard_knowledge_manifest_update()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
  IF (NEW.tenant_id,NEW.knowledge_id,NEW.version,NEW.source_uri,NEW.source_digest,
      NEW.chunking_pipeline_version,NEW.embedder_profile_id,NEW.embedder_version,
      NEW.vector_collection_generation,NEW.metadata_schema,NEW.content_watermark,NEW.created_at)
    IS DISTINCT FROM
     (OLD.tenant_id,OLD.knowledge_id,OLD.version,OLD.source_uri,OLD.source_digest,
      OLD.chunking_pipeline_version,OLD.embedder_profile_id,OLD.embedder_version,
      OLD.vector_collection_generation,OLD.metadata_schema,OLD.content_watermark,OLD.created_at) THEN
    RAISE EXCEPTION 'knowledge manifest identity is immutable' USING ERRCODE='23000';
  END IF;
  IF NEW.record_version <> OLD.record_version + 1 OR NEW.updated_at < OLD.updated_at THEN
    RAISE EXCEPTION 'knowledge manifest version must advance exactly once' USING ERRCODE='40001';
  END IF;
  IF NOT ((OLD.state='staging' AND NEW.state IN ('indexing','failed')) OR
          (OLD.state='indexing' AND NEW.state IN ('verifying','failed')) OR
          (OLD.state='verifying' AND NEW.state IN ('published','failed'))) THEN
    RAISE EXCEPTION 'illegal knowledge manifest state transition' USING ERRCODE='23514';
  END IF;
  IF OLD.chunk_total IS NOT NULL AND NEW.chunk_total IS DISTINCT FROM OLD.chunk_total THEN
    RAISE EXCEPTION 'knowledge manifest chunk_total is frozen' USING ERRCODE='23514';
  END IF;
  IF OLD.verification_digest <> '' AND NEW.verification_digest IS DISTINCT FROM OLD.verification_digest THEN
    RAISE EXCEPTION 'knowledge manifest verification_digest is frozen' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_manifest_guard
BEFORE UPDATE ON public.knowledge_manifest
FOR EACH ROW EXECUTE FUNCTION public.guard_knowledge_manifest_update();

CREATE FUNCTION public.begin_knowledge_manifest(
  p_tenant_id text,p_knowledge_id text,p_version bigint,p_source_uri text,p_source_digest text,
  p_chunking_pipeline_version text,p_embedder_profile_id text,p_embedder_version bigint,
  p_vector_collection_generation text,p_metadata_schema jsonb,p_content_watermark text,p_created_at timestamptz
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_existing public.knowledge_manifest%ROWTYPE;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_version<1 OR
     length(btrim(p_source_uri))=0 OR p_source_digest!~'^[0-9a-f]{64}$' OR
     length(btrim(p_chunking_pipeline_version))=0 OR length(btrim(p_embedder_profile_id))=0 OR p_embedder_version<1 OR
     length(btrim(p_vector_collection_generation))=0 OR p_metadata_schema IS NULL OR jsonb_typeof(p_metadata_schema)<>'array' OR
     EXISTS (SELECT 1 FROM jsonb_array_elements(p_metadata_schema) AS schema_item(value) WHERE jsonb_typeof(schema_item.value)<>'string' OR length(btrim(schema_item.value #>> '{}'))=0) OR
     (SELECT count(*) FROM jsonb_array_elements_text(p_metadata_schema)) <>
       (SELECT count(DISTINCT value) FROM jsonb_array_elements_text(p_metadata_schema) AS schema_value(value)) OR
     p_content_watermark IS NULL OR p_created_at IS NULL THEN
    RAISE EXCEPTION 'knowledge manifest input is invalid' USING ERRCODE='22023';
  END IF;
  INSERT INTO public.knowledge_manifest(tenant_id,knowledge_id,version,source_uri,source_digest,
    chunking_pipeline_version,embedder_profile_id,embedder_version,vector_collection_generation,
    metadata_schema,content_watermark,state,created_at,updated_at)
  VALUES(p_tenant_id,p_knowledge_id,p_version,p_source_uri,p_source_digest,
    p_chunking_pipeline_version,p_embedder_profile_id,p_embedder_version,p_vector_collection_generation,
    p_metadata_schema,p_content_watermark,'staging',p_created_at,p_created_at)
  ON CONFLICT (tenant_id,knowledge_id,version) DO NOTHING;
  SELECT * INTO v_existing FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version;
  IF (v_existing.source_uri,v_existing.source_digest,v_existing.chunking_pipeline_version,
      v_existing.embedder_profile_id,v_existing.embedder_version,v_existing.vector_collection_generation,
      v_existing.metadata_schema,v_existing.content_watermark,v_existing.created_at)
     IS DISTINCT FROM
     (p_source_uri,p_source_digest,p_chunking_pipeline_version,
      p_embedder_profile_id,p_embedder_version,p_vector_collection_generation,
      p_metadata_schema,p_content_watermark,p_created_at) THEN
    RAISE EXCEPTION 'knowledge manifest id collision' USING ERRCODE='23505';
  END IF;
END;
$$;

CREATE FUNCTION public.stage_knowledge_chunk(
  p_tenant_id text,p_knowledge_id text,p_knowledge_version bigint,p_chunk_id text,
  p_source_digest text,p_content_digest text,p_metadata_digest text,
  p_embedding_profile_id text,p_embedding_version bigint,p_vector_generation text,
  p_content text,p_metadata jsonb,p_vector jsonb,p_mutation_digest text,p_created_at timestamptz
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE; v_chunk public.knowledge_chunk%ROWTYPE;
  v_migration_id text; v_epoch bigint; v_source_config bigint; v_mutation_id text;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_knowledge_version<1 OR
     length(btrim(p_chunk_id))=0 OR p_source_digest!~'^[0-9a-f]{64}$' OR p_content_digest!~'^[0-9a-f]{64}$' OR
     p_metadata_digest!~'^[0-9a-f]{64}$' OR length(btrim(p_embedding_profile_id))=0 OR p_embedding_version<1 OR
     length(btrim(p_vector_generation))=0 OR p_content IS NULL OR length(p_content)=0 OR
     p_metadata IS NULL OR p_vector IS NULL OR jsonb_typeof(p_metadata)<>'object' OR jsonb_typeof(p_vector)<>'array' OR jsonb_array_length(p_vector)<1 OR
     p_mutation_digest!~'^[0-9a-f]{64}$' OR
     p_created_at IS NULL THEN
    RAISE EXCEPTION 'knowledge chunk input is invalid' USING ERRCODE='22023';
  END IF;

  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_knowledge_version FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state <> 'staging' THEN
    RAISE EXCEPTION 'knowledge manifest is not staging' USING ERRCODE='23514';
  END IF;
  IF p_source_digest<>v_manifest.source_digest OR p_embedding_profile_id<>v_manifest.embedder_profile_id OR
     p_embedding_version<>v_manifest.embedder_version OR p_vector_generation<>v_manifest.vector_collection_generation OR
     EXISTS (
       SELECT 1 FROM jsonb_object_keys(p_metadata) AS metadata_key(key)
       WHERE NOT EXISTS (
         SELECT 1 FROM jsonb_array_elements_text(v_manifest.metadata_schema) AS allowed_key(value)
         WHERE allowed_key.value=metadata_key.key
       )
     ) THEN
    RAISE EXCEPTION 'knowledge chunk does not match manifest' USING ERRCODE='23514';
  END IF;

  INSERT INTO public.knowledge_chunk(tenant_id,knowledge_id,knowledge_version,chunk_id,
    source_digest,content_digest,metadata_digest,embedding_profile_id,embedding_version,vector_generation,
    image_digest,content,metadata,vector,created_at)
  VALUES(p_tenant_id,p_knowledge_id,p_knowledge_version,p_chunk_id,
    p_source_digest,p_content_digest,p_metadata_digest,p_embedding_profile_id,p_embedding_version,p_vector_generation,
    p_mutation_digest,p_content,p_metadata,p_vector,p_created_at)
  ON CONFLICT (tenant_id,knowledge_id,knowledge_version,chunk_id) DO NOTHING;
  SELECT * INTO v_chunk FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_knowledge_version AND chunk_id=p_chunk_id;
  IF (v_chunk.source_digest,v_chunk.content_digest,v_chunk.metadata_digest,v_chunk.embedding_profile_id,
      v_chunk.embedding_version,v_chunk.vector_generation,v_chunk.image_digest,v_chunk.content,v_chunk.metadata,v_chunk.vector,v_chunk.created_at)
     IS DISTINCT FROM
     (p_source_digest,p_content_digest,p_metadata_digest,p_embedding_profile_id,
      p_embedding_version,p_vector_generation,p_mutation_digest,p_content,p_metadata,p_vector,p_created_at) THEN
    RAISE EXCEPTION 'knowledge chunk id collision' USING ERRCODE='23505';
  END IF;

  SELECT migration_id,epoch,source_config_version INTO v_migration_id,v_epoch,v_source_config
    FROM public.backend_migration
    WHERE tenant_id=p_tenant_id AND domain='knowledge'
      AND state IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe')
    ORDER BY created_at DESC LIMIT 1;
  IF FOUND THEN
    v_mutation_id := 'ingest_' || encode(sha256(convert_to(
      v_migration_id || chr(31) || p_knowledge_id || chr(31) || p_knowledge_version::text || chr(31) || p_chunk_id,
      'UTF8')), 'hex');
    PERFORM public.record_knowledge_migration_mutation(
      p_tenant_id,v_migration_id,v_mutation_id,v_epoch,
      p_knowledge_id,p_knowledge_version,p_chunk_id,'upsert',
      1,p_mutation_digest,v_source_config,p_created_at);
  END IF;
END;
$$;

CREATE FUNCTION public.begin_knowledge_indexing(
  p_tenant_id text,p_knowledge_id text,p_version bigint,p_chunk_total bigint,p_at timestamptz
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE; v_count bigint;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_version<1 OR
     p_chunk_total<1 OR p_at IS NULL THEN
    RAISE EXCEPTION 'knowledge indexing input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state <> 'staging' THEN
    RAISE EXCEPTION 'knowledge manifest is not staging' USING ERRCODE='23514';
  END IF;
  SELECT count(*) INTO v_count FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version;
  IF v_count <> p_chunk_total THEN
    RAISE EXCEPTION 'knowledge chunk total does not match staged chunks' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_manifest SET state='indexing',chunk_total=p_chunk_total,updated_at=p_at,record_version=record_version+1
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version;
END;
$$;

CREATE FUNCTION public.mark_knowledge_chunk_indexed(
  p_tenant_id text,p_knowledge_id text,p_knowledge_version bigint,p_chunk_id text,p_at timestamptz
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_knowledge_version<1 OR
     length(btrim(p_chunk_id))=0 OR p_at IS NULL THEN
    RAISE EXCEPTION 'knowledge chunk index input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_knowledge_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state<>'indexing' THEN
    RAISE EXCEPTION 'knowledge manifest is not indexing' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_chunk SET indexed_at=COALESCE(indexed_at,p_at)
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_knowledge_version AND chunk_id=p_chunk_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge chunk does not exist' USING ERRCODE='P0002'; END IF;
END;
$$;

CREATE FUNCTION public.begin_knowledge_verifying(
  p_tenant_id text,p_knowledge_id text,p_version bigint,p_verification_digest text,p_at timestamptz
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE; v_indexed bigint; v_computed_digest text;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_version<1 OR
     p_verification_digest!~'^[0-9a-f]{64}$' OR p_at IS NULL THEN
    RAISE EXCEPTION 'knowledge verifying input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state <> 'indexing' THEN
    RAISE EXCEPTION 'knowledge manifest is not indexing' USING ERRCODE='23514';
  END IF;
  SELECT count(*) INTO v_indexed FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version AND indexed_at IS NOT NULL;
  IF v_indexed <> v_manifest.chunk_total THEN
    RAISE EXCEPTION 'knowledge indexing is incomplete' USING ERRCODE='23514';
  END IF;
  SELECT encode(sha256(convert_to(string_agg(length(image_digest)::text || ':' || image_digest,'' ORDER BY image_digest),'UTF8')),'hex')
    INTO v_computed_digest FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version;
  IF v_computed_digest IS NULL OR v_computed_digest<>p_verification_digest THEN
    RAISE EXCEPTION 'knowledge verification digest does not match chunk set' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_manifest SET state='verifying',verification_digest=p_verification_digest,
    updated_at=p_at,record_version=record_version+1
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version;
END;
$$;

CREATE FUNCTION public.record_knowledge_probe(
  p_tenant_id text,p_knowledge_id text,p_knowledge_version bigint,p_probe_id text,
  p_query text,p_expected_chunks jsonb,p_min_recall_ppm bigint,p_created_at timestamptz
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_existing public.knowledge_probe%ROWTYPE; v_manifest public.knowledge_manifest%ROWTYPE;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_knowledge_version<1 OR
     length(btrim(p_probe_id))=0 OR length(btrim(p_query))=0 OR jsonb_typeof(p_expected_chunks)<>'array' OR
     jsonb_array_length(p_expected_chunks)<1 OR
     EXISTS (SELECT 1 FROM jsonb_array_elements(p_expected_chunks) AS expected_chunk(value) WHERE jsonb_typeof(expected_chunk.value)<>'string' OR length(btrim(expected_chunk.value #>> '{}'))=0) OR
     p_min_recall_ppm<1 OR p_min_recall_ppm>1000000 OR p_created_at IS NULL THEN
    RAISE EXCEPTION 'knowledge probe input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_knowledge_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state<>'verifying' THEN
    RAISE EXCEPTION 'knowledge manifest is not verifying' USING ERRCODE='23514';
  END IF;
  IF (SELECT count(*) FROM jsonb_array_elements_text(p_expected_chunks)) <>
     (SELECT count(DISTINCT value) FROM jsonb_array_elements_text(p_expected_chunks) AS expected(value)) OR
     EXISTS (
       SELECT 1 FROM jsonb_array_elements_text(p_expected_chunks) AS expected(chunk_id)
       WHERE NOT EXISTS (
         SELECT 1 FROM public.knowledge_chunk
         WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_knowledge_version
           AND chunk_id=expected.chunk_id
       )
     ) THEN
    RAISE EXCEPTION 'knowledge probe expected chunks are invalid' USING ERRCODE='23514';
  END IF;
  INSERT INTO public.knowledge_probe(tenant_id,knowledge_id,knowledge_version,probe_id,
    query,expected_chunks,min_recall_ppm,created_at)
  VALUES(p_tenant_id,p_knowledge_id,p_knowledge_version,p_probe_id,
    p_query,p_expected_chunks,p_min_recall_ppm,p_created_at)
  ON CONFLICT (tenant_id,knowledge_id,knowledge_version,probe_id) DO NOTHING;
  SELECT * INTO v_existing FROM public.knowledge_probe
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_knowledge_version AND probe_id=p_probe_id;
  IF (v_existing.query,v_existing.expected_chunks,v_existing.min_recall_ppm,v_existing.created_at)
     IS DISTINCT FROM (p_query,p_expected_chunks,p_min_recall_ppm,p_created_at) THEN
    RAISE EXCEPTION 'knowledge probe id collision' USING ERRCODE='23505';
  END IF;
END;
$$;

CREATE FUNCTION public.mark_knowledge_probe_verified(
  p_tenant_id text,p_knowledge_id text,p_knowledge_version bigint,p_probe_id text
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_knowledge_version<1 OR
     length(btrim(p_probe_id))=0 THEN
    RAISE EXCEPTION 'knowledge probe verify input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_knowledge_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state<>'verifying' THEN
    RAISE EXCEPTION 'knowledge manifest is not verifying' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_probe SET verified=true
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_knowledge_version AND probe_id=p_probe_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge probe does not exist' USING ERRCODE='P0002'; END IF;
END;
$$;

CREATE FUNCTION public.publish_knowledge_version(
  p_tenant_id text,p_knowledge_id text,p_version bigint,p_at timestamptz
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE; v_unverified bigint; v_probe_count bigint; v_indexed bigint; v_computed_digest text;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_version<1 OR p_at IS NULL THEN
    RAISE EXCEPTION 'knowledge publish input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state <> 'verifying' THEN
    RAISE EXCEPTION 'knowledge manifest is not verifying' USING ERRCODE='23514';
  END IF;
  SELECT count(*) INTO v_indexed FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version AND indexed_at IS NOT NULL;
  IF v_indexed<>v_manifest.chunk_total THEN
    RAISE EXCEPTION 'knowledge indexing is incomplete' USING ERRCODE='23514';
  END IF;
  SELECT count(*),count(*) FILTER (WHERE verified=false) INTO v_probe_count,v_unverified FROM public.knowledge_probe
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version;
  IF v_probe_count=0 OR v_unverified > 0 THEN
    RAISE EXCEPTION 'knowledge sample verification is incomplete' USING ERRCODE='23514';
  END IF;
  SELECT encode(sha256(convert_to(string_agg(length(image_digest)::text || ':' || image_digest,'' ORDER BY image_digest),'UTF8')),'hex')
    INTO v_computed_digest FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version;
  IF v_computed_digest IS NULL OR v_computed_digest<>v_manifest.verification_digest THEN
    RAISE EXCEPTION 'knowledge verification digest does not match chunk set' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_manifest SET state='published',updated_at=p_at,record_version=record_version+1
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version;
END;
$$;

CREATE FUNCTION public.fail_knowledge_version(
  p_tenant_id text,p_knowledge_id text,p_version bigint,p_at timestamptz
) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_version<1 OR p_at IS NULL THEN
    RAISE EXCEPTION 'knowledge fail input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state NOT IN ('staging','indexing','verifying') THEN
    RAISE EXCEPTION 'knowledge manifest is not failable' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_manifest SET state='failed',updated_at=p_at,record_version=record_version+1
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version;
END;
$$;

CREATE FUNCTION public.guard_agent_app_revision_knowledge_published()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
  IF OLD.state<>'published' AND NEW.state='published' AND EXISTS (
    SELECT 1 FROM public.agent_app_revision_knowledge AS ref
    LEFT JOIN public.knowledge_manifest AS manifest
      ON manifest.tenant_id=ref.tenant_id AND manifest.knowledge_id=ref.knowledge_id AND manifest.version=ref.knowledge_version
    WHERE ref.tenant_id=NEW.tenant_id AND ref.agent_app_id=NEW.agent_app_id AND ref.revision=NEW.revision
      AND (manifest.state IS NULL OR manifest.state<>'published')
  ) THEN
    RAISE EXCEPTION 'agent app revision references unpublished knowledge' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_revision_knowledge_published_guard
BEFORE UPDATE OF state ON public.agent_app_revision
FOR EACH ROW EXECUTE FUNCTION public.guard_agent_app_revision_knowledge_published();

REVOKE ALL ON FUNCTION public.guard_knowledge_manifest_update() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.guard_agent_app_revision_knowledge_published() FROM PUBLIC;

COMMIT;
