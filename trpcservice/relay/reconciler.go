package relay

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

// ReconciliationHandler must call the same typed transition used by the
// normal path. It must not update Session or Outbox tables directly.
type ReconciliationHandler interface {
	Reconcile(context.Context, messaging.ReconciliationIssue) error
}

type Reconciler struct {
	Store        messaging.ReconciliationStore
	Handler      ReconciliationHandler
	StuckAfter   time.Duration
	BatchSize    int
	PollInterval time.Duration
}

func (r Reconciler) Run(ctx context.Context) error {
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
		_, _ = r.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r Reconciler) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	stuckAfter := r.StuckAfter
	if stuckAfter <= 0 {
		stuckAfter = time.Minute
	}
	limit := r.BatchSize
	if limit <= 0 {
		limit = 100
	}
	issues, err := r.Store.FindReconciliationIssues(ctx, time.Now().Add(-stuckAfter), limit)
	if err != nil {
		return 0, err
	}
	handled := 0
	for _, issue := range issues {
		if err := r.Handler.Reconcile(ctx, issue); err != nil {
			return handled, err
		}
		handled++
	}
	return handled, nil
}

func (r Reconciler) validate() error {
	if r.Store == nil || r.Handler == nil {
		return runtime.ErrInvariantViolation
	}
	return nil
}
