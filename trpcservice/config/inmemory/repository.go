// Package inmemory implements immutable ConfigSnapshot publication for local
// contract tests.
package inmemory

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Repository struct {
	mu        sync.RWMutex
	tenants   tenant.Repository
	apps      agentapp.Repository
	snapshots map[string]map[int64]config.Snapshot
	next      map[string]int64
}

func New(tenants tenant.Repository, apps agentapp.Repository) *Repository {
	return &Repository{tenants: tenants, apps: apps, snapshots: make(map[string]map[int64]config.Snapshot), next: make(map[string]int64)}
}

func (r *Repository) Validate(ctx context.Context, in config.ValidateInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.TenantID == "" || in.Payload.SchemaVersion != config.CurrentSchemaVersion || in.Payload.PolicyVersion < 1 || in.Payload.DefaultAgentAppID == "" {
		return config.ErrInvalid
	}
	if err := r.validateApp(ctx, in.TenantID, in.Payload.DefaultAgentAppID); err != nil {
		return err
	}
	seenChannels := map[string]struct{}{}
	for _, binding := range in.Payload.ChannelBindings {
		if binding.BindingID == "" || binding.Channel == "" || binding.ExternalAccountID == "" || binding.AgentAppID == "" || binding.SecretRef.Ref == "" || binding.SecretRef.Version < 1 {
			return config.ErrInvalid
		}
		key := binding.Channel + "\x00" + binding.ExternalAccountID
		if _, ok := seenChannels[key]; ok {
			return config.ErrInvalid
		}
		seenChannels[key] = struct{}{}
		if err := r.validateApp(ctx, in.TenantID, binding.AgentAppID); err != nil {
			return err
		}
	}
	seenDomains := map[string]struct{}{}
	for _, binding := range in.Payload.BackendBindings {
		if binding.Domain == "" || binding.BackendProfileID == "" || binding.BackendVersion < 1 {
			return config.ErrInvalid
		}
		if _, ok := seenDomains[binding.Domain]; ok {
			return config.ErrInvalid
		}
		seenDomains[binding.Domain] = struct{}{}
	}
	return nil
}
func (r *Repository) validateApp(ctx context.Context, tenantID, appID string) error {
	app, err := r.apps.Get(ctx, tenantID, appID)
	if err != nil {
		return err
	}
	if app.TenantID != tenantID || app.Status != agentapp.StatusActive || app.CurrentRevision < 1 {
		return config.ErrInvalid
	}
	rev, err := r.apps.GetRevision(ctx, tenantID, appID, app.CurrentRevision)
	if err != nil {
		return err
	}
	if rev.State != agentapp.RevisionPublished || rev.ContentDigest == "" {
		return config.ErrInvalid
	}
	return nil
}
func (r *Repository) Publish(ctx context.Context, in config.PublishInput) (config.PublishResult, error) {
	if err := r.Validate(ctx, config.ValidateInput{TenantID: in.TenantID, Payload: in.Payload}); err != nil {
		return config.PublishResult{}, err
	}
	current, err := r.tenants.Get(ctx, in.TenantID)
	if err != nil {
		return config.PublishResult{}, err
	}
	if current.Version != in.ExpectedTenantVersion {
		return config.PublishResult{}, tenant.ErrVersionConflict
	}
	if current.Status == tenant.StatusDisabled {
		return config.PublishResult{}, tenant.ErrStatusConflict
	}
	snapshot, err := r.stage(in.TenantID, in.Payload)
	if err != nil {
		return config.PublishResult{}, err
	}
	nextTenant := current
	nextTenant.DefaultAgentAppID = in.Payload.DefaultAgentAppID
	nextTenant.ActiveConfigVersion = snapshot.ConfigVersion
	changed, err := r.tenants.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{Tenant: nextTenant, ExpectedVersion: in.ExpectedTenantVersion, ChangeMetadata: in.Metadata})
	if err != nil {
		r.removeStaged(in.TenantID, snapshot.ConfigVersion)
		return config.PublishResult{}, err
	}
	r.activate(in.TenantID, snapshot.ConfigVersion)
	snapshot, _ = r.Get(ctx, in.TenantID, snapshot.ConfigVersion)
	return config.PublishResult{Snapshot: snapshot, Tenant: changed.Tenant}, nil
}
func (r *Repository) stage(tenantID string, payload config.ConfigV1) (config.Snapshot, error) {
	normalized := config.NormalizeV1(payload)
	digest, _, err := config.ContentDigest(normalized)
	if err != nil {
		return config.Snapshot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	version := r.next[tenantID] + 1
	r.next[tenantID] = version
	if r.snapshots[tenantID] == nil {
		r.snapshots[tenantID] = make(map[int64]config.Snapshot)
	}
	snapshot := config.Snapshot{TenantID: tenantID, ConfigVersion: version, SchemaVersion: config.CurrentSchemaVersion, Payload: normalized, ContentDigest: digest, State: config.StateStaged, CreatedAt: time.Now().UTC()}
	r.snapshots[tenantID][version] = snapshot
	return cloneSnapshot(snapshot), nil
}
func (r *Repository) activate(tenantID string, version int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := r.snapshots[tenantID][version]
	snapshot.State = config.StatePublished
	snapshot.PublishedAt = time.Now().UTC()
	r.snapshots[tenantID][version] = snapshot
}
func (r *Repository) removeStaged(tenantID string, version int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if snapshot, ok := r.snapshots[tenantID][version]; ok && snapshot.State == config.StateStaged {
		delete(r.snapshots[tenantID], version)
	}
}
func (r *Repository) Get(ctx context.Context, tenantID string, version int64) (config.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return config.Snapshot{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot, ok := r.snapshots[tenantID][version]
	if !ok || snapshot.State != config.StatePublished {
		return config.Snapshot{}, config.ErrNotFound
	}
	return cloneSnapshot(snapshot), nil
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
	if current.Version != tc.TenantVersion || current.Status != tenant.StatusActive {
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
	app, err := r.apps.Get(ctx, tc.TenantID, tc.AgentAppID)
	if err != nil {
		return tenant.ExecutionBinding{}, err
	}
	if app.Status != agentapp.StatusActive || app.CurrentRevision < 1 {
		return tenant.ExecutionBinding{}, config.ErrInvalid
	}
	revision, err := r.apps.GetRevision(ctx, tc.TenantID, tc.AgentAppID, app.CurrentRevision)
	if err != nil {
		return tenant.ExecutionBinding{}, err
	}
	result := tenant.ExecutionBinding{AgentAppVersion: app.Version, AgentAppRevision: revision.Revision, AgentContentDigest: revision.ContentDigest, ConfigVersion: snapshot.ConfigVersion, PolicyVersion: snapshot.Payload.PolicyVersion}
	return result, result.Validate()
}
func clonePayload(in config.ConfigV1) config.ConfigV1 {
	return config.NormalizeV1(in)
}
func cloneSnapshot(in config.Snapshot) config.Snapshot {
	out := in
	out.Payload = clonePayload(in.Payload)
	return out
}

var _ config.Repository = (*Repository)(nil)
