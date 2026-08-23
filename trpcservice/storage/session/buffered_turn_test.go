package session_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	agentevent "trpc.group/trpc-go/trpc-agent-go/event"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestBufferedTurnDoesNotPersistPartialEffects(t *testing.T) {
	ctx := context.Background()
	backing := agentmemory.NewSessionService()
	key := agentsession.Key{AppName: "app", UserID: "user", SessionID: "session"}
	if _, err := backing.CreateSession(ctx, key, agentsession.StateMap{}); err != nil {
		t.Fatal(err)
	}
	atomic := sessionmemory.New()
	storageKey := sessionstore.SessionKey{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}
	head, err := atomic.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: storageKey, RequestID: "request", InputSeq: 1, Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := sessionstore.NewBufferedTurn(atomic, backing, storageKey, "user", func(_ context.Context, value *agentevent.Event) (string, string, error) {
		return "agent_event", "event://" + value.ID, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := turn.SessionService().GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	event := &agentevent.Event{ID: "event-1", StateDelta: map[string][]byte{"answer": []byte(`"ready"`)}}
	if err := turn.SessionService().AppendEvent(ctx, session, event); err != nil {
		t.Fatal(err)
	}
	base, err := backing.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(base.Events) != 0 || len(base.State) != 0 {
		t.Fatalf("backing mutated before commit: %#v", base)
	}
	_, err = turn.Commit(ctx, sessionstore.CommitTurnRequest{RequestID: "request", CommitID: "request:terminal:0", Stage: "terminal", InputSeq: 1, Fence: 1, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded})
	if err != nil {
		t.Fatal(err)
	}
	events, _, _ := atomic.SnapshotEffects(storageKey)
	if len(events) != 1 || events[0].EventID != "event-1" {
		t.Fatalf("events=%#v", events)
	}
}

func TestBufferedTurnRollbackDropsEffects(t *testing.T) {
	ctx := context.Background()
	backing := agentmemory.NewSessionService()
	key := agentsession.Key{AppName: "app", UserID: "user", SessionID: "session"}
	if _, err := backing.CreateSession(ctx, key, agentsession.StateMap{}); err != nil {
		t.Fatal(err)
	}
	atomic := sessionmemory.New()
	storageKey := sessionstore.SessionKey{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}
	turn, err := sessionstore.NewBufferedTurn(atomic, backing, storageKey, "user", func(context.Context, *agentevent.Event) (string, string, error) {
		return "event", "event://1", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := turn.SessionService().GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := turn.SessionService().AppendEvent(ctx, session, &agentevent.Event{ID: "event-1"}); err != nil {
		t.Fatal(err)
	}
	if err := turn.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if len(turn.Events()) != 0 {
		t.Fatal("rollback retained events")
	}
}
