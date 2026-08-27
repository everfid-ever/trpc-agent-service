package relay

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// ParkedInputReconciler repairs a lost Wakeup Stream event and enforces park
// deadlines even when no further session commit occurs.
type ParkedInputReconciler struct {
	Activator    WakeupActivator
	BatchSize    int
	PollInterval time.Duration
	OnBlocked    func(context.Context, gateway.ExecutionKey)
}

func (r ParkedInputReconciler) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	interval := r.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r ParkedInputReconciler) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	limit := r.BatchSize
	if limit <= 0 {
		limit = 100
	}
	keys, err := r.Activator.Store.ListActionableParkedInputs(ctx, time.Now().UTC(), limit)
	if err != nil {
		return 0, err
	}
	handled := 0
	for _, key := range keys {
		disposition, err := r.Activator.Activate(ctx, key)
		if err != nil {
			return handled, err
		}
		if disposition == WakeupBlocked && r.OnBlocked != nil {
			r.OnBlocked(ctx, key)
		}
		handled++
	}
	return handled, nil
}

func (r ParkedInputReconciler) validate() error {
	if r.Activator.Store == nil || r.Activator.Dispatch == nil || r.Activator.ShardCount == 0 {
		return runtime.ErrInvariantViolation
	}
	return nil
}
