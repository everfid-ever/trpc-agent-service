// Package admin exposes the authenticated minimum control-plane operations.
package admin

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrUnauthenticated = errors.New("admin principal is not authenticated")
	ErrForbidden       = errors.New("admin principal is not authorized for tenant")
)

type Principal struct {
	Authenticated bool
	TenantID      string
	SubjectID     string
	CanManage     bool
}

type Service struct{ Configs config.Repository }

func authorize(principal Principal, pathTenant string) error {
	if !principal.Authenticated {
		return ErrUnauthenticated
	}
	if !principal.CanManage || principal.TenantID == "" || principal.TenantID != pathTenant {
		return ErrForbidden
	}
	return nil
}

func (s Service) Validate(ctx context.Context, principal Principal, pathTenant string, payload config.ConfigV1) error {
	if err := authorize(principal, pathTenant); err != nil {
		return err
	}
	return s.Configs.Validate(ctx, config.ValidateInput{TenantID: pathTenant, Payload: payload})
}

func (s Service) Publish(ctx context.Context, principal Principal, pathTenant string, expectedVersion int64, payload config.ConfigV1, metadata tenant.ChangeMetadata) (config.PublishResult, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return config.PublishResult{}, err
	}
	return s.Configs.Publish(ctx, config.PublishInput{TenantID: pathTenant, ExpectedTenantVersion: expectedVersion, Payload: payload, Metadata: metadata})
}

func (s Service) Current(ctx context.Context, principal Principal, pathTenant string) (config.Snapshot, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return config.Snapshot{}, err
	}
	return s.Configs.GetCurrent(ctx, pathTenant)
}

func (s Service) Get(ctx context.Context, principal Principal, pathTenant string, version int64) (config.Snapshot, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return config.Snapshot{}, err
	}
	return s.Configs.Get(ctx, pathTenant, version)
}

func (s Service) Rollback(ctx context.Context, principal Principal, pathTenant string, expectedVersion, targetVersion int64, metadata tenant.ChangeMetadata) (config.PublishResult, error) {
	if err := authorize(principal, pathTenant); err != nil {
		return config.PublishResult{}, err
	}
	return s.Configs.Rollback(ctx, config.RollbackInput{TenantID: pathTenant, ExpectedTenantVersion: expectedVersion, TargetVersion: targetVersion, Metadata: metadata})
}
