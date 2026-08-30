BEGIN;

ALTER TABLE public.preprocess_job
  ADD COLUMN channel_binding_id text,
  ADD COLUMN config_version bigint;

UPDATE public.preprocess_job job
SET channel_binding_id = binding.binding_id,
    config_version = binding.config_version
FROM public.inbox inbox
JOIN public.tenant root ON root.tenant_id = inbox.tenant_id
JOIN public.channel_binding binding
  ON binding.tenant_id = inbox.tenant_id
 AND binding.config_version = root.active_config_version
 AND binding.channel = inbox.channel
 AND binding.external_account_id = inbox.external_account_id
 AND binding.agent_app_id = inbox.agent_app_id
WHERE job.tenant_id = inbox.tenant_id
  AND job.request_id = inbox.request_id;

ALTER TABLE public.preprocess_job
  ALTER COLUMN channel_binding_id SET NOT NULL,
  ALTER COLUMN config_version SET NOT NULL,
  ADD CONSTRAINT preprocess_job_config_version_check CHECK (config_version >= 1),
  ADD CONSTRAINT preprocess_job_channel_binding_fk
    FOREIGN KEY (tenant_id, config_version, channel_binding_id)
    REFERENCES public.channel_binding(tenant_id, config_version, binding_id);

COMMIT;
