package inmemory

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

func TestCommitIdempotencyAndStaleFence(t *testing.T) {
	ctx := context.Background()
	s := New()
	key := sessionstore.SessionKey{TenantID: "t", AgentAppID: "a", SessionID: "s"}
	head, err := s.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "r1", InputSeq: 1, Fence: 41})
	if err != nil {
		t.Fatal(err)
	}
	commit := sessionstore.CommitTurnRequest{SessionKey: key, RequestID: "r1", CommitID: "c1", Stage: "terminal", InputSeq: 1, Fence: 41, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded, ResultRef: "result"}
	first, err := s.CommitTurn(ctx, commit)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.CommitTurn(ctx, commit)
	if err != nil || again != first {
		t.Fatalf("idempotent result=%#v err=%v", again, err)
	}
	head, err = s.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "r2", InputSeq: 2, Fence: 42})
	if err != nil {
		t.Fatal(err)
	}
	commit2 := sessionstore.CommitTurnRequest{SessionKey: key, RequestID: "r2", CommitID: "c2", Stage: "terminal", InputSeq: 2, Fence: 40, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded}
	if _, err = s.CommitTurn(ctx, commit2); !errors.Is(err, runtime.ErrStaleFence) {
		t.Fatalf("got %v", err)
	}
}
