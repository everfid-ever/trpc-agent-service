package session_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	agentevent "trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestBufferedTurnPersistsSessionEffectsThroughOfficialService(t *testing.T) {
	ctx := context.Background()
	backing := agentmemory.NewSessionService()
	key := agentsession.Key{AppName: "tenant/app", UserID: "user", SessionID: "session"}
	if _, err := backing.CreateSession(ctx, key, agentsession.StateMap{}); err != nil {
		t.Fatal(err)
	}
	atomic := sessionmemory.New()
	storageKey := sessionstore.SessionKey{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}
	head, err := atomic.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: storageKey, RequestID: "request", InputSeq: 1, Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := sessionstore.NewBufferedTurn(atomic, backing, storageKey, "user")
	if err != nil {
		t.Fatal(err)
	}
	session, err := turn.SessionService().GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := turn.SessionService().AppendEvent(ctx, session, durableEvent("event-0", model.RoleUser, "input", nil)); err != nil {
		t.Fatal(err)
	}
	event := durableEvent("event-1", model.RoleAssistant, "ready", map[string][]byte{"answer": []byte(`"ready"`)})
	if err := turn.SessionService().AppendEvent(ctx, session, event); err != nil {
		t.Fatal(err)
	}
	base, err := backing.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(base.Events) != 2 || string(base.State["answer"]) != `"ready"` {
		t.Fatalf("SDK session effects not persisted: %#v", base)
	}
	_, err = turn.Commit(ctx, sessionstore.CommitTurnRequest{RequestID: "request", CommitID: "request:terminal:0", Stage: "terminal", InputSeq: 1, Fence: 1, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded})
	if err != nil {
		t.Fatal(err)
	}
	events, _, _ := atomic.SnapshotEffects(storageKey)
	if len(events) != 0 {
		t.Fatalf("coordination store duplicated SDK events=%#v", events)
	}
}

func TestBufferedTurnRollbackOnlyDropsCoordinationMetadata(t *testing.T) {
	ctx := context.Background()
	backing := agentmemory.NewSessionService()
	key := agentsession.Key{AppName: "tenant/app", UserID: "user", SessionID: "session"}
	if _, err := backing.CreateSession(ctx, key, agentsession.StateMap{}); err != nil {
		t.Fatal(err)
	}
	atomic := sessionmemory.New()
	storageKey := sessionstore.SessionKey{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}
	turn, err := sessionstore.NewBufferedTurn(atomic, backing, storageKey, "user")
	if err != nil {
		t.Fatal(err)
	}
	session, err := turn.SessionService().GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := turn.SessionService().AppendEvent(ctx, session, durableEvent("event-0", model.RoleUser, "input", nil)); err != nil {
		t.Fatal(err)
	}
	if err := turn.SessionService().AppendEvent(ctx, session, durableEvent("event-1", model.RoleAssistant, "ready", nil)); err != nil {
		t.Fatal(err)
	}
	if err := turn.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	base, err := backing.GetSession(ctx, key)
	if err != nil || len(base.Events) != 2 {
		t.Fatalf("SDK event unexpectedly rolled back: session=%#v err=%v", base, err)
	}
}

func TestDurableBufferedTurnRestoresCommittedHistory(t *testing.T) {
	ctx := context.Background()
	atomic := sessionmemory.New()
	backing := agentmemory.NewSessionService()
	storageKey := sessionstore.SessionKey{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}
	head, err := atomic.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: storageKey, RequestID: "request-1", InputSeq: 1, Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	first, err := sessionstore.NewDurableBufferedTurnScoped(atomic, backing, storageKey, "tenant/app", "user")
	if err != nil {
		t.Fatal(err)
	}
	session, err := first.SessionService().GetSession(ctx, agentsession.Key{AppName: "tenant/app", UserID: "user", SessionID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SessionService().AppendEvent(ctx, session, durableEvent("event-0", model.RoleUser, "input", nil)); err != nil {
		t.Fatal(err)
	}
	if err := first.SessionService().AppendEvent(ctx, session, durableEvent("event-1", model.RoleAssistant, "ready", map[string][]byte{"answer": []byte(`"ready"`)})); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Commit(ctx, sessionstore.CommitTurnRequest{RequestID: "request-1", CommitID: "request-1:terminal:0", Stage: "terminal", InputSeq: 1, Fence: 1, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded}); err != nil {
		t.Fatal(err)
	}
	if _, err := atomic.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: storageKey, RequestID: "request-2", InputSeq: 2, Fence: 2}); err != nil {
		t.Fatal(err)
	}
	second, err := sessionstore.NewDurableBufferedTurnScoped(atomic, backing, storageKey, "tenant/app", "user")
	if err != nil {
		t.Fatal(err)
	}
	restored, err := second.SessionService().GetSession(ctx, agentsession.Key{AppName: "tenant/app", UserID: "user", SessionID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Events) != 2 || restored.Events[1].ID != "event-1" || string(restored.State["answer"]) != `"ready"` {
		t.Fatalf("restored=%#v", restored)
	}
}

// durableEvent mirrors the event shape the official trpc-agent-go Session
// services persist: a completed response plus any StateDelta. Metadata-only
// events intentionally update state without entering the transcript.
func durableEvent(id string, role model.Role, content string, delta map[string][]byte) *agentevent.Event {
	return &agentevent.Event{ID: id, Response: &model.Response{Choices: []model.Choice{{Message: model.Message{
		Role: role, Content: content}}}}, StateDelta: delta}
}
