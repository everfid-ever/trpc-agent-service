BEGIN;

CREATE TABLE inbound_payload (
  tenant_id text NOT NULL,
  request_id text NOT NULL,
  payload_ref text NOT NULL,
  content bytea NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, request_id),
  UNIQUE (tenant_id, payload_ref),
  FOREIGN KEY (tenant_id) REFERENCES tenant(tenant_id)
);

COMMIT;
