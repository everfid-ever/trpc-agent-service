package relay

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type reconciliationStoreStub struct {
	issues []messaging.ReconciliationIssue
}

func (s reconciliationStoreStub) FindReconciliationIssues(context.Context, time.Time, int) ([]messaging.ReconciliationIssue, error) {
	return s.issues, nil
}

type reconciliationHandlerStub struct {
	issues []messaging.ReconciliationIssue
}

func (s *reconciliationHandlerStub) Reconcile(_ context.Context, issue messaging.ReconciliationIssue) error {
	s.issues = append(s.issues, issue)
	return nil
}

func TestReconcilerDelegatesToTypedTransitions(t *testing.T) {
	issues := []messaging.ReconciliationIssue{{Kind: messaging.IssueExpiredOutboxClaim, TenantID: "tenant", AggregateID: "request", RefID: "outbox", Version: 2}, {Kind: messaging.IssueMissingReplyOutbox, TenantID: "tenant", AggregateID: "request", RefID: "commit", Version: 3}}
	handler := &reconciliationHandlerStub{}
	r := Reconciler{Store: reconciliationStoreStub{issues: issues}, Handler: handler}
	count, err := r.RunOnce(context.Background())
	if err != nil || count != len(issues) || len(handler.issues) != len(issues) {
		t.Fatalf("count=%d err=%v handled=%#v", count, err, handler.issues)
	}
}
