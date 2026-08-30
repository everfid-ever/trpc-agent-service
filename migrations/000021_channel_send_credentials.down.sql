BEGIN;

DROP TRIGGER IF EXISTS channel_binding_populate_send_secret ON public.channel_binding;
DROP FUNCTION IF EXISTS public.populate_channel_send_secret();
ALTER TABLE public.channel_binding
  DROP CONSTRAINT IF EXISTS channel_binding_send_secret_complete,
  DROP COLUMN IF EXISTS send_secret_version,
  DROP COLUMN IF EXISTS send_secret_ref;

COMMIT;
