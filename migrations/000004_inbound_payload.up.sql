BEGIN;

CREATE TABLE inbound_payload (
  tenant_id text NOT NULL,
  request_id text NOT NULL,
  payload_ref text NOT NULL,
  payload_ciphertext bytea NOT NULL,
  payload_nonce bytea NOT NULL,
  content_digest text NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  key_version bigint NOT NULL CHECK (key_version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, request_id),
  UNIQUE (tenant_id, payload_ref),
  FOREIGN KEY (tenant_id) REFERENCES tenant(tenant_id)
);

COMMIT;
