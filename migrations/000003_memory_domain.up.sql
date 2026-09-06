BEGIN;

ALTER TABLE public.outbox DROP CONSTRAINT outbox_kind_check;
ALTER TABLE public.outbox ADD CONSTRAINT outbox_kind_check CHECK (kind = ANY (ARRAY[
  'audit'::text, 'tenant-control'::text, 'config-invalidation'::text,
  'memory-invalidation'::text, 'dispatch'::text, 'reply'::text, 'wakeup'::text,
  'execution-control'::text
]));

CREATE TABLE public.memory_watermark (
    tenant_id text PRIMARY KEY REFERENCES public.tenant(tenant_id),
    version bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT memory_watermark_version_check CHECK (version >= 0)
);

CREATE TABLE public.memory_entry (
    tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
    scope text NOT NULL,
    subject_id text NOT NULL DEFAULT '',
    memory_id text NOT NULL,
    version bigint NOT NULL,
    tenant_watermark bigint NOT NULL,
    content_ref text NOT NULL,
    content_digest text NOT NULL,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, scope, subject_id, memory_id),
    CONSTRAINT memory_entry_scope_check CHECK ((scope = 'tenant' AND subject_id = '') OR (scope = 'user' AND length(btrim(subject_id)) > 0)),
    CONSTRAINT memory_entry_version_check CHECK (version >= 1 AND tenant_watermark >= 1),
    CONSTRAINT memory_entry_content_check CHECK (length(btrim(content_ref)) > 0 AND content_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT memory_entry_attributes_check CHECK (jsonb_typeof(attributes) = 'object')
);

CREATE INDEX memory_entry_tenant_scope_idx ON public.memory_entry(tenant_id, scope, subject_id, memory_id);

CREATE TABLE public.memory_mutation (
    tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
    mutation_id text NOT NULL,
    scope text NOT NULL,
    subject_id text NOT NULL,
    memory_id text NOT NULL,
    entry_version bigint NOT NULL,
    tenant_watermark bigint NOT NULL,
    content_ref text NOT NULL,
    content_digest text NOT NULL,
    attributes jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, mutation_id),
    CONSTRAINT memory_mutation_scope_check CHECK ((scope = 'tenant' AND subject_id = '') OR (scope = 'user' AND length(btrim(subject_id)) > 0)),
    CONSTRAINT memory_mutation_version_check CHECK (entry_version >= 1 AND tenant_watermark >= 1),
    CONSTRAINT memory_mutation_content_check CHECK (length(btrim(mutation_id)) > 0 AND length(btrim(content_ref)) > 0 AND content_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT memory_mutation_attributes_check CHECK (jsonb_typeof(attributes) = 'object')
);

CREATE TABLE public.memory_index_intent (
    tenant_id text NOT NULL,
    mutation_id text NOT NULL,
    scope text NOT NULL,
    subject_id text NOT NULL,
    memory_id text NOT NULL,
    entry_version bigint NOT NULL,
    tenant_watermark bigint NOT NULL,
    content_ref text NOT NULL,
    content_digest text NOT NULL,
    state text NOT NULL DEFAULT 'pending',
    created_at timestamptz NOT NULL DEFAULT now(),
    indexed_at timestamptz,
    PRIMARY KEY (tenant_id, mutation_id),
    CONSTRAINT memory_index_intent_mutation_fk FOREIGN KEY (tenant_id, mutation_id) REFERENCES public.memory_mutation(tenant_id, mutation_id),
    CONSTRAINT memory_index_intent_state_check CHECK (state IN ('pending','indexed','failed')),
    CONSTRAINT memory_index_intent_version_check CHECK (entry_version >= 1 AND tenant_watermark >= 1)
);

CREATE INDEX memory_index_intent_pending_idx ON public.memory_index_intent(state, created_at, tenant_id);

COMMIT;
