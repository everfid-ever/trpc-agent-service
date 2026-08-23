// Package postgres implements AtomicSessionStore with one PostgreSQL
// commit_turn transaction function as the business-effect boundary.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

type DB interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type Store struct{ db DB }

func New(db DB) *Store { return &Store{db: db} }

func (s *Store) OpenForRun(ctx context.Context, in sessionstore.OpenForRunRequest) (sessionstore.SessionHead, error) {
	if in.TenantID == "" || in.AgentAppID == "" || in.SessionID == "" {
		return sessionstore.SessionHead{}, runtime.ErrTenantScope
	}
	if in.RequestID == "" || in.InputSeq < 1 || in.Fence < 1 {
		return sessionstore.SessionHead{}, runtime.ErrCommitConflict
	}
	var head sessionstore.SessionHead
	var state []byte
	head.SessionKey = in.SessionKey
	err := s.db.QueryRowContext(ctx, `SELECT version,last_fence,last_session_seq,next_input_seq,state_json
FROM session_head WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3`, in.TenantID, in.AgentAppID, in.SessionID).
		Scan(&head.Version, &head.LastFence, &head.LastSessionSeq, &head.NextInputSeq, &state)
	if err != nil {
		return sessionstore.SessionHead{}, translate(err)
	}
	if err := json.Unmarshal(state, &head.State); err != nil {
		return sessionstore.SessionHead{}, runtime.ErrInvariantViolation
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
	return head, nil
}

type eventJSON struct {
	EventID, EventType, PayloadRef string
}
type summaryJSON struct {
	SummaryID      string `json:"summary_id"`
	BaseSessionSeq uint64 `json:"base_session_seq"`
	LastEventID    string `json:"last_event_id"`
	CutoffAt       string `json:"cutoff_at"`
	ContentRef     string `json:"content_ref"`
}
type outboxJSON struct {
	Kind, IdempotencyKey, PayloadRef, TraceParent string
	EventSeq                                      uint64
}

func (s *Store) CommitTurn(ctx context.Context, in sessionstore.CommitTurnRequest) (sessionstore.CommitTurnResult, error) {
	if err := sessionstore.ValidateCommit(in); err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	digest, err := sessionstore.CommitDigest(in)
	if err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	events := make([]map[string]any, 0, len(in.Events))
	for _, event := range in.Events {
		events = append(events, map[string]any{"event_id": event.EventID, "event_type": event.EventType, "payload_ref": event.PayloadRef, "event_seq": event.EventSeq})
	}
	outboxes := make([]map[string]any, 0, len(in.Outbox))
	for _, event := range in.Outbox {
		outboxes = append(outboxes, map[string]any{"kind": event.Kind, "idempotency_key": event.IdempotencyKey, "payload_ref": event.PayloadRef, "traceparent": event.TraceParent, "event_seq": event.EventSeq})
	}
	eventBytes, err := json.Marshal(events)
	if err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	stateDelta := in.StateDelta
	if stateDelta == nil {
		stateDelta = sessionstore.StateDelta{}
	}
	stateBytes, err := json.Marshal(stateDelta)
	if err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	summaryBytes := []byte("null")
	if in.SummaryCandidate != nil {
		summaryBytes, err = json.Marshal(summaryJSON{SummaryID: in.SummaryCandidate.SummaryID, BaseSessionSeq: in.SummaryCandidate.BaseSessionSeq, LastEventID: in.SummaryCandidate.LastEventID, CutoffAt: in.SummaryCandidate.CutoffAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"), ContentRef: in.SummaryCandidate.ContentRef})
		if err != nil {
			return sessionstore.CommitTurnResult{}, err
		}
	}
	outboxBytes, err := json.Marshal(outboxes)
	if err != nil {
		return sessionstore.CommitTurnResult{}, err
	}
	var result sessionstore.CommitTurnResult
	var resultRef, replyCursor sql.NullString
	err = s.db.QueryRowContext(ctx, `SELECT commit_id,outcome,input_seq,session_version,result_ref,reply_cursor
FROM commit_turn($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14::jsonb,$15,$16,$17::jsonb)`,
		in.TenantID, in.AgentAppID, in.SessionID, in.RequestID, in.CommitID, digest, in.Stage,
		in.InputSeq, in.Fence, in.ExpectedVersion, string(in.Outcome), eventBytes, stateBytes,
		summaryBytes, nullable(in.ResultRef), nullable(in.ReplyCursor), outboxBytes).
		Scan(&result.CommitID, &result.Outcome, &result.InputSeq, &result.SessionVersion, &resultRef, &replyCursor)
	if err != nil {
		return sessionstore.CommitTurnResult{}, translate(err)
	}
	result.ResultRef, result.ReplyCursor = resultRef.String, replyCursor.String
	return result, nil
}

func (s *Store) GetTerminalByInputSeq(ctx context.Context, key sessionstore.TerminalKey) (sessionstore.CommitTurnResult, error) {
	var result sessionstore.CommitTurnResult
	err := s.db.QueryRowContext(ctx, `SELECT commit_id,outcome,input_seq,session_version,COALESCE(result_ref,''),COALESCE(reply_cursor,'')
FROM session_commit WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 AND input_seq=$4
AND outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout')`,
		key.TenantID, key.AgentAppID, key.SessionID, key.InputSeq).
		Scan(&result.CommitID, &result.Outcome, &result.InputSeq, &result.SessionVersion, &result.ResultRef, &result.ReplyCursor)
	if err != nil {
		return sessionstore.CommitTurnResult{}, translate(err)
	}
	return result, nil
}

func (s *Store) ReadLastFence(ctx context.Context, key sessionstore.SessionKey) (uint64, error) {
	var fence uint64
	err := s.db.QueryRowContext(ctx, `SELECT last_fence FROM session_head WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3`, key.TenantID, key.AgentAppID, key.SessionID).Scan(&fence)
	if err != nil {
		return 0, translate(err)
	}
	return fence, nil
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type sqlStater interface{ SQLState() string }

func translate(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001":
			if strings.Contains(strings.ToLower(err.Error()), "stale fence") {
				return runtime.ErrStaleFence
			}
			return runtime.ErrVersionConflict
		case "23505":
			return runtime.ErrCommitConflict
		case "XX001":
			return runtime.ErrInvariantViolation
		case "55000":
			return runtime.ErrInputNotReady
		case "42501":
			return runtime.ErrTenantScope
		}
	}
	return err
}

var _ sessionstore.AtomicSessionStore = (*Store)(nil)
