package inmemory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestBundleManagerBuildsOnceAndClosesAfterLastLease(t *testing.T) {
	var builds, closes atomic.Int32
	manager := NewBundleManager(func(context.Context, profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		builds.Add(1)
		return "bundle", func(context.Context) error { closes.Add(1); return nil }, nil
	})
	key := profile.ExecutionProfileKey{TenantID: "t", AgentAppID: "a", AgentAppRevision: 1, ContentDigest: "d", ConfigVersion: 1, PolicyVersion: 1}
	var wg sync.WaitGroup
	leases := make([]profile.BundleLease, 20)
	for i := range leases {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lease, err := manager.Acquire(context.Background(), key)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			leases[i] = lease
		}(i)
	}
	wg.Wait()
	if builds.Load() != 1 {
		t.Fatalf("builds=%d", builds.Load())
	}
	manager.Retire(key)
	if _, err := manager.Acquire(context.Background(), key); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("retired acquire=%v", err)
	}
	for _, lease := range leases {
		lease.Release()
	}
	if closes.Load() != 1 {
		t.Fatalf("closes=%d", closes.Load())
	}
}
