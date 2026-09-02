BEGIN;

DROP FUNCTION IF EXISTS public.fail_knowledge_version(text,text,bigint,timestamptz);
DROP TRIGGER IF EXISTS agent_app_revision_knowledge_published_guard ON public.agent_app_revision;
DROP FUNCTION IF EXISTS public.guard_agent_app_revision_knowledge_published();
DROP FUNCTION IF EXISTS public.publish_knowledge_version(text,text,bigint,timestamptz);
DROP FUNCTION IF EXISTS public.mark_knowledge_probe_verified(text,text,bigint,text);
DROP FUNCTION IF EXISTS public.record_knowledge_probe(text,text,bigint,text,text,jsonb,bigint,timestamptz);
DROP FUNCTION IF EXISTS public.begin_knowledge_verifying(text,text,bigint,text,timestamptz);
DROP FUNCTION IF EXISTS public.mark_knowledge_chunk_indexed(text,text,bigint,text,timestamptz);
DROP FUNCTION IF EXISTS public.begin_knowledge_indexing(text,text,bigint,bigint,timestamptz);
DROP FUNCTION IF EXISTS public.stage_knowledge_chunk(text,text,bigint,text,text,text,text,text,bigint,text,text,jsonb,jsonb,text,timestamptz);
DROP FUNCTION IF EXISTS public.begin_knowledge_manifest(text,text,bigint,text,text,text,text,bigint,text,jsonb,text,timestamptz);

DROP TRIGGER IF EXISTS knowledge_manifest_guard ON public.knowledge_manifest;
DROP FUNCTION IF EXISTS public.guard_knowledge_manifest_update();

DROP TABLE IF EXISTS public.knowledge_probe;
DROP TABLE IF EXISTS public.knowledge_chunk;
DROP TABLE IF EXISTS public.knowledge_manifest;

COMMIT;
