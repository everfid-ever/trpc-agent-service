package inmemory

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func meta() tenant.ChangeMetadata {
	return tenant.ChangeMetadata{ActorType: "admin", ActorID: "operator", ReasonCode: "test", CorrelationID: "correlation", TraceID: "trace"}
}

func TestStatusCASAndAtomicFacts(t *testing.T) {
	ctx := context.Background()
	repo := New()
	created, err := repo.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: "tenant-a", TenantKey: "tenant-a", DisplayName: "A"}, ChangeMetadata: meta()})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := repo.TransitionStatus(ctx, tenant.TransitionStatusInput{TenantID: "tenant-a", ExpectedVersion: created.Version, NextStatus: tenant.StatusSuspended, ChangeMetadata: meta()})
	if err != nil {
		t.Fatal(err)
	}
	if changed.Tenant.Version != 2 {
		t.Fatalf("version=%d", changed.Tenant.Version)
	}
	_, err = repo.TransitionStatus(ctx, tenant.TransitionStatusInput{TenantID: "tenant-a", ExpectedVersion: 1, NextStatus: tenant.StatusDisabled, ChangeMetadata: meta()})
	if !errors.Is(err, tenant.ErrVersionConflict) {
		t.Fatalf("got %v", err)
	}
	changes, outbox := repo.Facts("tenant-a")
	if len(changes) != 2 || len(outbox) != 4 {
		t.Fatalf("changes=%d outbox=%d", len(changes), len(outbox))
	}
}
