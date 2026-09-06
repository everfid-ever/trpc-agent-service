BEGIN;

CREATE TABLE public.config_release (
    release_id text PRIMARY KEY,
    state text NOT NULL,
    percentage integer NOT NULL,
    salt text NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    actor_id text NOT NULL,
    reason_code text NOT NULL,
    correlation_id text NOT NULL,
    trace_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT config_release_state_check CHECK (state IN ('active','rolled_back')),
    CONSTRAINT config_release_percentage_check CHECK (percentage BETWEEN 0 AND 100),
    CONSTRAINT config_release_version_check CHECK (version >= 1),
    CONSTRAINT config_release_identifier_check CHECK (length(btrim(release_id)) > 0 AND length(release_id) <= 256),
    CONSTRAINT config_release_salt_check CHECK (length(btrim(salt)) >= 16 AND length(salt) <= 512),
    CONSTRAINT config_release_metadata_check CHECK (length(btrim(actor_id)) > 0 AND length(btrim(reason_code)) > 0 AND length(btrim(correlation_id)) > 0 AND length(btrim(trace_id)) > 0)
);

CREATE TABLE public.config_release_target (
    release_id text NOT NULL REFERENCES public.config_release(release_id),
    tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
    baseline_config_version bigint NOT NULL,
    candidate_config_version bigint NOT NULL,
    allowlisted boolean NOT NULL DEFAULT false,
    PRIMARY KEY (release_id, tenant_id),
    CONSTRAINT config_release_target_versions_check CHECK (baseline_config_version >= 1 AND candidate_config_version >= 1 AND baseline_config_version <> candidate_config_version),
    CONSTRAINT config_release_target_baseline_fk FOREIGN KEY (tenant_id, baseline_config_version) REFERENCES public.config_snapshot(tenant_id, config_version),
    CONSTRAINT config_release_target_candidate_fk FOREIGN KEY (tenant_id, candidate_config_version) REFERENCES public.config_snapshot(tenant_id, config_version)
);

CREATE INDEX config_release_target_effective_idx ON public.config_release_target(tenant_id, baseline_config_version, release_id);

COMMIT;
