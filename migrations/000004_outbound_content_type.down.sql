BEGIN;

ALTER TABLE public.interaction_payload DROP CONSTRAINT interaction_payload_content_type_check;
ALTER TABLE public.interaction_payload DROP COLUMN content_type;

ALTER TABLE public.result_payload DROP CONSTRAINT result_payload_content_type_check;
ALTER TABLE public.result_payload DROP COLUMN content_type;

COMMIT;
