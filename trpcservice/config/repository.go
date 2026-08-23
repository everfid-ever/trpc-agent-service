package config

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrNotFound        = errors.New("config snapshot not found")
	ErrInvalid         = errors.New("invalid config snapshot")
	ErrTenantScope     = errors.New("config tenant scope mismatch")
	ErrVersionConflict = errors.New("config version conflict")
)

type ValidateInput struct {
	TenantID string
	Payload  ConfigV1
}
type PublishInput struct {
	TenantID              string
	ExpectedTenantVersion int64
	Payload               ConfigV1
	Metadata              tenant.ChangeMetadata
}
type PublishResult struct {
	Snapshot Snapshot
	Tenant   tenant.Tenant
}
type RollbackInput struct {
	TenantID                             string
	ExpectedTenantVersion, TargetVersion int64
	Metadata                             tenant.ChangeMetadata
}

type Repository interface {
	Validate(context.Context, ValidateInput) error
	Publish(context.Context, PublishInput) (PublishResult, error)
	Get(context.Context, string, int64) (Snapshot, error)
	GetCurrent(context.Context, string) (Snapshot, error)
	Rollback(context.Context, RollbackInput) (PublishResult, error)
	ResolveExecutionBinding(context.Context, tenant.Context) (tenant.ExecutionBinding, error)
}
