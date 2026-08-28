BEGIN;

ALTER TABLE public.preprocess_job
  ADD COLUMN prepared_payload_ref text NOT NULL DEFAULT '';

CREATE TABLE public.prepared_payload (
  tenant_id text NOT NULL,
  request_id text NOT NULL,
  payload_ref text NOT NULL,
  source_payload_ref text NOT NULL,
  payload_ciphertext bytea NOT NULL,
  payload_nonce bytea NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  key_version bigint NOT NULL CHECK (key_version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, request_id),
  UNIQUE (tenant_id, payload_ref),
  FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id),
  FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id)
);

ALTER TABLE public.preprocess_job
  ADD CONSTRAINT preprocess_job_prepared_ref_complete CHECK (
    (state <> 'ready' AND prepared_payload_ref = '') OR
    (state = 'ready' AND (prepared_payload_ref = '' OR length(btrim(prepared_payload_ref)) > 0))
  );

COMMIT;
