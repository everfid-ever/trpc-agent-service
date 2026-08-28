BEGIN;

CREATE TABLE public.media_artifact (
  tenant_id text NOT NULL,
  request_id text NOT NULL,
  artifact_id text NOT NULL,
  artifact_ref text NOT NULL,
  ordinal integer NOT NULL CHECK (ordinal >= 0),
  source_digest text NOT NULL CHECK (source_digest ~ '^[0-9a-f]{64}$'),
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  media_type text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('image','file')),
  content bytea NOT NULL CHECK (octet_length(content) > 0),
  malware_scan_version text NOT NULL,
  dlp_version text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, artifact_id),
  UNIQUE (tenant_id, artifact_ref),
  FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id),
  FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id)
);

CREATE INDEX media_artifact_request_idx
  ON public.media_artifact(tenant_id, request_id, ordinal);

COMMIT;
