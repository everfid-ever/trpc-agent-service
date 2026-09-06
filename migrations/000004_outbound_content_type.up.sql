BEGIN;

ALTER TABLE public.result_payload
    ADD COLUMN content_type text NOT NULL DEFAULT 'text/plain',
    ADD CONSTRAINT result_payload_content_type_check CHECK (
        content_type IN ('text/plain', 'application/vnd.trpc.card+json')
        OR content_type ~ '^image/[a-z0-9.+-]+$'
    );

ALTER TABLE public.interaction_payload
    ADD COLUMN content_type text NOT NULL DEFAULT 'text/plain',
    ADD CONSTRAINT interaction_payload_content_type_check CHECK (
        content_type IN ('text/plain', 'application/vnd.trpc.card+json')
        OR content_type ~ '^image/[a-z0-9.+-]+$'
    );

COMMIT;
