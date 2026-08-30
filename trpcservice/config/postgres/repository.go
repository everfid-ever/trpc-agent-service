// Package postgres implements immutable ConfigSnapshot publication using the
// transaction functions installed by the control-plane migration.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Repository struct {
	db      *sql.DB
	tenants tenant.Repository
}

func New(db *sql.DB, tenants tenant.Repository) *Repository {
	return &Repository{db: db, tenants: tenants}
}

func (r *Repository) Validate(ctx context.Context, in config.ValidateInput) error {
	if in.TenantID == "" || in.Payload.SchemaVersion != config.CurrentSchemaVersion || in.Payload.PolicyVersion < 1 || in.Payload.DefaultAgentAppID == "" {
		return config.ErrInvalid
	}
	apps := map[string]struct{}{in.Payload.DefaultAgentAppID: {}}
	channels := map[string]struct{}{}
	for _, binding := range in.Payload.ChannelBindings {
		if binding.BindingID == "" || binding.Channel == "" || binding.ExternalAccountID == "" || binding.AgentAppID == "" || binding.SecretRef.Ref == "" || binding.SecretRef.Version < 1 || !config.ValidSendSecret(binding) {
			return config.ErrInvalid
		}
		key := binding.Channel + "\x00" + binding.ExternalAccountID
		if _, ok := channels[key]; ok {
			return config.ErrInvalid
		}
		channels[key] = struct{}{}
		apps[binding.AgentAppID] = struct{}{}
	}
	domains := map[string]struct{}{}
	for _, binding := range in.Payload.BackendBindings {
		if binding.Domain == "" || binding.BackendProfileID == "" || binding.BackendVersion < 1 {
			return config.ErrInvalid
		}
		if _, ok := domains[binding.Domain]; ok {
			return config.ErrInvalid
		}
		domains[binding.Domain] = struct{}{}
		var status string
		var digest string
		var capabilitiesSatisfied bool
		if err := r.db.QueryRowContext(ctx, `SELECT p.status,v.content_digest,
NOT EXISTS (SELECT capability FROM unnest($4::text[]) capability EXCEPT SELECT capability FROM unnest(v.capabilities) capability)
FROM backend_profile p JOIN backend_profile_revision v USING (tenant_id,backend_profile_id)
WHERE p.tenant_id=$1 AND p.backend_profile_id=$2 AND v.profile_version=$3`, in.TenantID, binding.BackendProfileID, binding.BackendVersion, binding.Required).Scan(&status, &digest, &capabilitiesSatisfied); err != nil {
			return classify(err)
		}
		if status != "active" || len(digest) != 64 || !capabilitiesSatisfied {
			return config.ErrInvalid
		}
	}
	for appID := range apps {
		var status string
		var revision sql.NullInt64
		err := r.db.QueryRowContext(ctx, `SELECT status,current_revision FROM agent_app WHERE tenant_id=$1 AND agent_app_id=$2`, in.TenantID, appID).Scan(&status, &revision)
		if err != nil {
			return classify(err)
		}
		if status != string(agentapp.StatusActive) || !revision.Valid {
			return config.ErrInvalid
		}
		var state, digest string
		if err = r.db.QueryRowContext(ctx, `SELECT state,content_digest FROM agent_app_revision WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3`, in.TenantID, appID, revision.Int64).Scan(&state, &digest); err != nil {
			return classify(err)
		}
		if state != string(agentapp.RevisionPublished) || digest == "" {
			return config.ErrInvalid
		}
	}
	return nil
}

func (r *Repository) Publish(ctx context.Context, in config.PublishInput) (config.PublishResult, error) {
	if err := in.Metadata.Validate(); err != nil {
		return config.PublishResult{}, err
	}
	if err := r.Validate(ctx, config.ValidateInput{TenantID: in.TenantID, Payload: in.Payload}); err != nil {
		return config.PublishResult{}, err
	}
	digest, data, err := config.ContentDigest(in.Payload)
	if err != nil {
		return config.PublishResult{}, err
	}
	var configVersion, tenantVersion int64
	err = r.db.QueryRowContext(ctx, `SELECT config_version,tenant_version FROM publish_config_snapshot($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9,$10,$11)`, in.TenantID, in.ExpectedTenantVersion, config.CurrentSchemaVersion, string(data), digest, in.Payload.DefaultAgentAppID, in.Metadata.ActorID, in.Metadata.ReasonCode, in.Metadata.CorrelationID, in.Metadata.TraceID, nil).Scan(&configVersion, &tenantVersion)
	if err != nil {
		return config.PublishResult{}, classify(err)
	}
	snapshot, err := r.Get(ctx, in.TenantID, configVersion)
	if err != nil {
		return config.PublishResult{}, err
	}
	current, err := r.tenants.Get(ctx, in.TenantID)
	if err != nil {
		return config.PublishResult{}, err
	}
	if current.Version != tenantVersion {
		return config.PublishResult{}, config.ErrVersionConflict
	}
	return config.PublishResult{Snapshot: snapshot, Tenant: current}, nil
}
func (r *Repository) Get(ctx context.Context, tenantID string, version int64) (config.Snapshot, error) {
	var result config.Snapshot
	var payload []byte
	err := r.db.QueryRowContext(ctx, `SELECT tenant_id,config_version,schema_version,payload,content_digest,state,published_at,created_at FROM config_snapshot WHERE tenant_id=$1 AND config_version=$2`, tenantID, version).Scan(&result.TenantID, &result.ConfigVersion, &result.SchemaVersion, &payload, &result.ContentDigest, &result.State, &result.PublishedAt, &result.CreatedAt)
	if err != nil {
		return config.Snapshot{}, classify(err)
	}
	decoded, err := config.DecodeV1(payload)
	if err != nil {
		return config.Snapshot{}, config.ErrInvalid
	}
	result.Payload = decoded
	return result, nil
}
func (r *Repository) GetCurrent(ctx context.Context, tenantID string) (config.Snapshot, error) {
	current, err := r.tenants.Get(ctx, tenantID)
	if err != nil {
		return config.Snapshot{}, err
	}
	if current.ActiveConfigVersion < 1 {
		return config.Snapshot{}, config.ErrNotFound
	}
	return r.Get(ctx, tenantID, current.ActiveConfigVersion)
}
func (r *Repository) Rollback(ctx context.Context, in config.RollbackInput) (config.PublishResult, error) {
	target, err := r.Get(ctx, in.TenantID, in.TargetVersion)
	if err != nil {
		return config.PublishResult{}, err
	}
	return r.Publish(ctx, config.PublishInput{TenantID: in.TenantID, ExpectedTenantVersion: in.ExpectedTenantVersion, Payload: target.Payload, Metadata: in.Metadata})
}
func (r *Repository) ResolveExecutionBinding(ctx context.Context, tc tenant.Context) (tenant.ExecutionBinding, error) {
	if err := tc.Validate(); err != nil {
		return tenant.ExecutionBinding{}, err
	}
	current, err := r.tenants.Get(ctx, tc.TenantID)
	if err != nil {
		return tenant.ExecutionBinding{}, err
	}
	if current.Status != tenant.StatusActive || current.Version != tc.TenantVersion {
		return tenant.ExecutionBinding{}, runtime.ErrVersionMismatch
	}
	snapshot, err := r.Get(ctx, tc.TenantID, current.ActiveConfigVersion)
	if err != nil {
		return tenant.ExecutionBinding{}, err
	}
	allowed := tc.AgentAppID == snapshot.Payload.DefaultAgentAppID && tc.TrustedSource == "authenticated_api"
	for _, binding := range snapshot.Payload.ChannelBindings {
		if binding.AgentAppID == tc.AgentAppID && binding.Channel == tc.Channel && tc.TrustedSource == "channel_binding:"+binding.BindingID {
			allowed = true
			break
		}
	}
	if !allowed {
		return tenant.ExecutionBinding{}, config.ErrTenantScope
	}
	var appVersion, revision int64
	var status string
	if err = r.db.QueryRowContext(ctx, `SELECT version,current_revision,status FROM agent_app WHERE tenant_id=$1 AND agent_app_id=$2`, tc.TenantID, tc.AgentAppID).Scan(&appVersion, &revision, &status); err != nil {
		return tenant.ExecutionBinding{}, classify(err)
	}
	if status != string(agentapp.StatusActive) {
		return tenant.ExecutionBinding{}, config.ErrInvalid
	}
	var digest, state string
	if err = r.db.QueryRowContext(ctx, `SELECT content_digest,state FROM agent_app_revision WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3`, tc.TenantID, tc.AgentAppID, revision).Scan(&digest, &state); err != nil {
		return tenant.ExecutionBinding{}, classify(err)
	}
	if state != string(agentapp.RevisionPublished) {
		return tenant.ExecutionBinding{}, config.ErrInvalid
	}
	result := tenant.ExecutionBinding{AgentAppVersion: appVersion, AgentAppRevision: revision, AgentContentDigest: digest, ConfigVersion: snapshot.ConfigVersion, PolicyVersion: snapshot.Payload.PolicyVersion}
	return result, result.Validate()
}

type sqlStater interface{ SQLState() string }

func classify(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return config.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001":
			return fmt.Errorf("%w: %v", config.ErrVersionConflict, err)
		case "P0002":
			return fmt.Errorf("%w: %v", config.ErrNotFound, err)
		case "23503":
			return fmt.Errorf("%w: %v", config.ErrTenantScope, err)
		case "22023", "23514", "23505", "55000":
			return fmt.Errorf("%w: %v", config.ErrInvalid, err)
		}
	}
	return err
}

var _ config.Repository = (*Repository)(nil)
