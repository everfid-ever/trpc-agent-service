BEGIN;

DROP TABLE IF EXISTS public.channel_ingress_candidate;
DROP TABLE IF EXISTS public.channel_binding_locator;
DROP TABLE IF EXISTS public.channel_public_route;

DROP FUNCTION IF EXISTS public.claim_channel_inbox(
  text,text,text,text,text,text,text,text,text,text,text,bigint,text
);

ALTER TABLE public.inbox
  DROP COLUMN external_user_id,
  DROP COLUMN external_chat_id;

COMMIT;
