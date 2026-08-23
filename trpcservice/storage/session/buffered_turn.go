package session

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agentevent "trpc.group/trpc-go/trpc-agent-go/event"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
)

// EventRefEncoder externalizes an upstream event and returns the durable event
// type and payload reference used by CommitTurn.
type EventRefEncoder func(context.Context, *agentevent.Event) (string, string, error)

// BufferedTurn is the service-owned transaction bridge used until upstream
// exposes a native atomic commit hook.
type BufferedTurn struct {
	mu           sync.Mutex
	store        AtomicSessionStore
	backing      agentsession.Service
	key          SessionKey
	userID       string
	encode       EventRefEncoder
	events       []BufferedEvent
	agentEvents  []agentevent.Event
	eventSeqBase uint64
	state        StateDelta
	summary      *SummaryCandidate
	closed       bool
	committed    bool
	service      *bufferedSessionService
}

func NewBufferedTurn(store AtomicSessionStore, backing agentsession.Service, key SessionKey, userID string, encode EventRefEncoder) (*BufferedTurn, error) {
	if store == nil || backing == nil || encode == nil || key.TenantID == "" || key.AgentAppID == "" || key.SessionID == "" || userID == "" {
		return nil, runtime.ErrCapabilityUnsupported
	}
	turn := &BufferedTurn{store: store, backing: backing, key: key, userID: userID, encode: encode, state: StateDelta{}}
	turn.service = &bufferedSessionService{turn: turn}
	return turn, nil
}

func (t *BufferedTurn) SessionService() agentsession.Service { return t.service }

func (t *BufferedTurn) Events() []BufferedEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]BufferedEvent(nil), t.events...)
}

func (t *BufferedTurn) StateDelta() StateDelta {
	t.mu.Lock()
	defer t.mu.Unlock()
	return cloneStateDelta(t.state)
}

func (t *BufferedTurn) SummaryCandidate() *SummaryCandidate {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.summary == nil {
		return nil
	}
	copy := *t.summary
	return &copy
}

func (t *BufferedTurn) SetSummaryCandidate(candidate SummaryCandidate) error {
	if candidate.SummaryID == "" || candidate.BaseSessionSeq < 1 || candidate.LastEventID == "" || candidate.CutoffAt.IsZero() || candidate.ContentRef == "" {
		return runtime.ErrCommitConflict
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return runtime.ErrCommitConflict
	}
	copy := candidate
	t.summary = &copy
	return nil
}

// SetEventSeqBase assigns the stable request-level cursor used by a later
// continuation stage. It must be called before the first buffered event.
func (t *BufferedTurn) SetEventSeqBase(base uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || len(t.events) != 0 {
		return runtime.ErrCommitConflict
	}
	t.eventSeqBase = base
	return nil
}

func (t *BufferedTurn) Commit(ctx context.Context, request CommitTurnRequest) (CommitTurnResult, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return CommitTurnResult{}, runtime.ErrCommitConflict
	}
	request.SessionKey = t.key
	request.Events = append([]BufferedEvent(nil), t.events...)
	request.StateDelta = cloneStateDelta(t.state)
	if t.summary != nil {
		copy := *t.summary
		request.SummaryCandidate = &copy
	}
	t.mu.Unlock()
	result, err := t.store.CommitTurn(ctx, request)
	if err != nil {
		return CommitTurnResult{}, err
	}
	t.mu.Lock()
	t.closed, t.committed = true, true
	t.mu.Unlock()
	return result, nil
}

func (t *BufferedTurn) Rollback(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return runtime.ErrCommitConflict
	}
	t.closed = true
	t.events = nil
	t.agentEvents = nil
	t.state = nil
	t.summary = nil
	return nil
}

func cloneStateDelta(in StateDelta) StateDelta {
	out := make(StateDelta, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type bufferedSessionService struct{ turn *BufferedTurn }

func (s *bufferedSessionService) validateKey(key agentsession.Key) error {
	if key.AppName != s.turn.key.AgentAppID || key.UserID != s.turn.userID || key.SessionID != s.turn.key.SessionID {
		return runtime.ErrTenantScope
	}
	return nil
}

func (s *bufferedSessionService) CreateSession(ctx context.Context, key agentsession.Key, state agentsession.StateMap, _ ...agentsession.Option) (*agentsession.Session, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}
	if err := s.UpdateSessionState(ctx, key, state); err != nil {
		return nil, err
	}
	return &agentsession.Session{ID: key.SessionID, AppName: key.AppName, UserID: key.UserID, State: cloneAgentState(state)}, nil
}

func (s *bufferedSessionService) GetSession(ctx context.Context, key agentsession.Key, options ...agentsession.Option) (*agentsession.Session, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}
	session, err := s.turn.backing.GetSession(ctx, key, options...)
	if err != nil {
		return nil, err
	}
	copy := session.Clone()
	s.turn.mu.Lock()
	defer s.turn.mu.Unlock()
	if copy.State == nil {
		copy.State = agentsession.StateMap{}
	}
	for key, value := range s.turn.state {
		if encoded, ok := value.(json.RawMessage); ok {
			copy.State[key] = append([]byte(nil), encoded...)
		}
	}
	for _, value := range s.turn.agentEvents {
		copy.Events = append(copy.Events, *value.Clone())
	}
	return copy, nil
}

func (s *bufferedSessionService) ListSessions(ctx context.Context, key agentsession.UserKey, options ...agentsession.Option) ([]*agentsession.Session, error) {
	if key.AppName != s.turn.key.AgentAppID || key.UserID != s.turn.userID {
		return nil, runtime.ErrTenantScope
	}
	return s.turn.backing.ListSessions(ctx, key, options...)
}

func (s *bufferedSessionService) DeleteSession(context.Context, agentsession.Key, ...agentsession.Option) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) UpdateAppState(context.Context, string, agentsession.StateMap) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) DeleteAppState(context.Context, string, string) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) ListAppStates(ctx context.Context, app string) (agentsession.StateMap, error) {
	if app != s.turn.key.AgentAppID {
		return nil, runtime.ErrTenantScope
	}
	return s.turn.backing.ListAppStates(ctx, app)
}
func (s *bufferedSessionService) UpdateUserState(context.Context, agentsession.UserKey, agentsession.StateMap) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) ListUserStates(ctx context.Context, key agentsession.UserKey) (agentsession.StateMap, error) {
	if key.AppName != s.turn.key.AgentAppID || key.UserID != s.turn.userID {
		return nil, runtime.ErrTenantScope
	}
	return s.turn.backing.ListUserStates(ctx, key)
}
func (s *bufferedSessionService) DeleteUserState(context.Context, agentsession.UserKey, string) error {
	return runtime.ErrCapabilityUnsupported
}

func (s *bufferedSessionService) UpdateSessionState(ctx context.Context, key agentsession.Key, state agentsession.StateMap) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.validateKey(key); err != nil {
		return err
	}
	s.turn.mu.Lock()
	defer s.turn.mu.Unlock()
	if s.turn.closed {
		return runtime.ErrCommitConflict
	}
	for name, value := range state {
		s.turn.state[name] = json.RawMessage(append([]byte(nil), value...))
	}
	return nil
}

func (s *bufferedSessionService) AppendEvent(ctx context.Context, session *agentsession.Session, value *agentevent.Event, _ ...agentsession.Option) error {
	if session == nil || value == nil {
		return runtime.ErrCommitConflict
	}
	if err := s.validateKey(agentsession.Key{AppName: session.AppName, UserID: session.UserID, SessionID: session.ID}); err != nil {
		return err
	}
	eventType, payloadRef, err := s.turn.encode(ctx, value)
	if err != nil {
		return err
	}
	if value.ID == "" || eventType == "" || payloadRef == "" {
		return runtime.ErrCommitConflict
	}
	s.turn.mu.Lock()
	defer s.turn.mu.Unlock()
	if s.turn.closed {
		return runtime.ErrCommitConflict
	}
	eventSeq := s.turn.eventSeqBase + uint64(len(s.turn.events)+1)
	s.turn.events = append(s.turn.events, BufferedEvent{EventID: value.ID, EventType: eventType, PayloadRef: payloadRef, EventSeq: eventSeq})
	s.turn.agentEvents = append(s.turn.agentEvents, *value.Clone())
	for name, delta := range value.StateDelta {
		s.turn.state[name] = json.RawMessage(append([]byte(nil), delta...))
	}
	session.EventMu.Lock()
	session.Events = append(session.Events, *value.Clone())
	session.EventMu.Unlock()
	if session.State == nil {
		session.State = agentsession.StateMap{}
	}
	for name, delta := range value.StateDelta {
		session.State[name] = append([]byte(nil), delta...)
	}
	return nil
}

func (s *bufferedSessionService) CreateSessionSummary(context.Context, *agentsession.Session, string, bool) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) EnqueueSummaryJob(context.Context, *agentsession.Session, string, bool) error {
	return runtime.ErrCapabilityUnsupported
}
func (s *bufferedSessionService) GetSessionSummaryText(ctx context.Context, session *agentsession.Session, options ...agentsession.SummaryOption) (string, bool) {
	return s.turn.backing.GetSessionSummaryText(ctx, session, options...)
}
func (s *bufferedSessionService) Close() error { return nil }

func cloneAgentState(in agentsession.StateMap) agentsession.StateMap {
	out := make(agentsession.StateMap, len(in))
	for key, value := range in {
		out[key] = append([]byte(nil), value...)
	}
	return out
}

var _ TurnTx = (*BufferedTurn)(nil)
var _ agentsession.Service = (*bufferedSessionService)(nil)
