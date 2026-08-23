package inmemory

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestTaskStoreContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewTaskStore().PrepareDispatch(ctx, gateway.PrepareDispatchRequest{Tenant: tenant.Context{}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
