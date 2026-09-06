BEGIN;

DROP TABLE public.memory_index_intent;
DROP TABLE public.memory_mutation;
DROP TABLE public.memory_entry;
DROP TABLE public.memory_watermark;

ALTER TABLE public.outbox DROP CONSTRAINT outbox_kind_check;
ALTER TABLE public.outbox ADD CONSTRAINT outbox_kind_check CHECK (kind = ANY (ARRAY[
  'audit'::text, 'tenant-control'::text, 'config-invalidation'::text,
  'dispatch'::text, 'reply'::text, 'wakeup'::text, 'execution-control'::text
]));

COMMIT;
