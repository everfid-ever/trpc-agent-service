// Package inmemory is the local AtomicSessionStore contract implementation.
package inmemory

import (
	"context"
	"reflect"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

type Store struct {
	mu       sync.Mutex
	heads    map[sessionstore.SessionKey]sessionstore.SessionHead
	commits  map[string]sessionstore.CommitTurnRequest
	results  map[string]sessionstore.CommitTurnResult
	terminal map[sessionstore.TerminalKey]sessionstore.CommitTurnResult
}

func New() *Store {
	return &Store{heads: make(map[sessionstore.SessionKey]sessionstore.SessionHead), commits: make(map[string]sessionstore.CommitTurnRequest), results: make(map[string]sessionstore.CommitTurnResult), terminal: make(map[sessionstore.TerminalKey]sessionstore.CommitTurnResult)}
}
func commitKey(k sessionstore.SessionKey, id string) string {
	return k.TenantID + "\x00" + k.AgentAppID + "\x00" + k.SessionID + "\x00" + id
}

func (s *Store) OpenForRun(ctx context.Context, in sessionstore.OpenForRunRequest) (sessionstore.SessionHead, error) {
	if err := ctx.Err(); err != nil {
		return sessionstore.SessionHead{}, err
	}
	if in.TenantID == "" || in.AgentAppID == "" || in.SessionID == "" {
		return sessionstore.SessionHead{}, runtime.ErrTenantScope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	head, ok := s.heads[in.SessionKey]
	if !ok {
		head = sessionstore.SessionHead{SessionKey: in.SessionKey, NextInputSeq: 1, State: map[string]any{}}
		s.heads[in.SessionKey] = head
	}
	if in.InputSeq < head.NextInputSeq {
		return head, runtime.ErrAlreadyTerminal
	}
	if in.InputSeq > head.NextInputSeq {
		return head, runtime.ErrInputNotReady
	}
	if in.Fence < head.LastFence {
		return head, runtime.ErrStaleFence
	}
	return cloneHead(head), nil
}

func (s *Store) CommitTurn(ctx context.Context, in sessionstore.CommitTurnRequest) (sessionstore.CommitTurnResult, error) {
	if err := ctx.Err(); err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ck := commitKey(in.SessionKey, in.CommitID)
	if old, ok := s.commits[ck]; ok {
		if !reflect.DeepEqual(old, in) {
			return sessionstore.CommitTurnResult{}, runtime.ErrCommitConflict
		}
		return s.results[ck], nil
	}
	head, ok := s.heads[in.SessionKey]
	if !ok {
		return sessionstore.CommitTurnResult{}, runtime.ErrNotFound
	}
	if in.InputSeq < head.NextInputSeq {
		if result, ok := s.terminal[sessionstore.TerminalKey{SessionKey: in.SessionKey, InputSeq: in.InputSeq}]; ok {
			return result, runtime.ErrAlreadyTerminal
		}
		return sessionstore.CommitTurnResult{}, runtime.ErrCommitConflict
	}
	if in.InputSeq > head.NextInputSeq {
		return sessionstore.CommitTurnResult{}, runtime.ErrInputNotReady
	}
	if in.ExpectedVersion != head.Version {
		return sessionstore.CommitTurnResult{}, runtime.ErrVersionConflict
	}
	if in.Fence < head.LastFence {
		return sessionstore.CommitTurnResult{}, runtime.ErrStaleFence
	}
	head.Version++
	head.LastFence = in.Fence
	head.LastSessionSeq += uint64(len(in.Events))
	if head.State == nil {
		head.State = map[string]any{}
	}
	for k, v := range in.StateDelta {
		head.State[k] = v
	}
	if in.Outcome.Terminal() {
		head.NextInputSeq++
	}
	s.heads[in.SessionKey] = head
	result := sessionstore.CommitTurnResult{CommitID: in.CommitID, Outcome: in.Outcome, SessionVersion: head.Version, ResultRef: in.ResultRef, ReplyCursor: in.ReplyCursor}
	s.commits[ck] = cloneCommit(in)
	s.results[ck] = result
	if in.Outcome.Terminal() {
		s.terminal[sessionstore.TerminalKey{SessionKey: in.SessionKey, InputSeq: in.InputSeq}] = result
	}
	return result, nil
}
func (s *Store) GetTerminalByInputSeq(ctx context.Context, key sessionstore.TerminalKey) (sessionstore.CommitTurnResult, error) {
	if err := ctx.Err(); err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.terminal[key]
	if !ok {
		return sessionstore.CommitTurnResult{}, runtime.ErrNotFound
	}
	return result, nil
}
func (s *Store) ReadLastFence(ctx context.Context, key sessionstore.SessionKey) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	head, ok := s.heads[key]
	if !ok {
		return 0, nil
	}
	return head.LastFence, nil
}
func cloneHead(in sessionstore.SessionHead) sessionstore.SessionHead {
	out := in
	out.State = make(map[string]any, len(in.State))
	for k, v := range in.State {
		out.State[k] = v
	}
	return out
}

func cloneCommit(in sessionstore.CommitTurnRequest) sessionstore.CommitTurnRequest {
	out := in
	out.Events = append([]sessionstore.BufferedEvent(nil), in.Events...)
	if in.StateDelta != nil {
		out.StateDelta = make(sessionstore.StateDelta, len(in.StateDelta))
		for k, v := range in.StateDelta {
			out.StateDelta[k] = v
		}
	}
	if in.SummaryCandidate != nil {
		candidate := *in.SummaryCandidate
		out.SummaryCandidate = &candidate
	}
	return out
}
