// Package postgres implements the shared PostgreSQL TaskStore.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type TaskStore struct{ db *sql.DB }

func NewTaskStore(db *sql.DB) *TaskStore { return &TaskStore{db: db} }

func (s *TaskStore) PrepareDispatch(ctx context.Context, in gateway.PrepareDispatchRequest) (gateway.PreparedDispatch, error) {
	if err := in.Tenant.Validate(); err != nil {
		return gateway.PreparedDispatch{}, err
	}
	if err := in.Binding.Validate(); err != nil {
		return gateway.PreparedDispatch{}, err
	}
	var input sql.NullInt64
	var accepted bool
	var reason sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT input_seq,accepted,terminal_reason FROM prepare_dispatch(
$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, in.Tenant.TenantID, in.Tenant.TenantVersion,
		in.Tenant.AgentAppID, in.Binding.AgentAppVersion, in.Binding.AgentAppRevision,
		in.Binding.AgentContentDigest, in.Binding.ConfigVersion, in.Binding.PolicyVersion,
		in.RequestID, in.SessionID, in.UserID, in.Tenant.Channel, in.PayloadRef, nullable(in.TraceParent)).
		Scan(&input, &accepted, &reason)
	if err != nil {
		return gateway.PreparedDispatch{}, translate(err)
	}
	if !accepted {
		return gateway.PreparedDispatch{Accepted: false, TerminalReason: reason.String}, nil
	}
	status, err := s.GetExecution(ctx, gateway.ExecutionKey{TenantID: in.Tenant.TenantID, RequestID: in.RequestID})
	if err != nil {
		return gateway.PreparedDispatch{}, err
	}
	if status.Envelope.InputSeq != uint64(input.Int64) {
		return gateway.PreparedDispatch{}, runtime.ErrInvariantViolation
	}
	return gateway.PreparedDispatch{Envelope: status.Envelope, Accepted: true}, nil
}

func (s *TaskStore) GetExecution(ctx context.Context, key gateway.ExecutionKey) (gateway.ExecutionStatus, error) {
	var result gateway.ExecutionStatus
	var created time.Time
	err := s.db.QueryRowContext(ctx, `SELECT tenant_id,tenant_version,agent_app_id,agent_app_version,
agent_app_revision,agent_content_digest,config_version,policy_version,request_id,session_id,user_id,channel,
input_seq,payload_ref,COALESCE(traceparent,''),outcome,COALESCE(result_ref,''),created_at
FROM execution_record WHERE tenant_id=$1 AND request_id=$2`, key.TenantID, key.RequestID).
		Scan(&result.Envelope.TenantID, &result.Envelope.TenantVersion, &result.Envelope.AgentAppID,
			&result.Envelope.AgentAppVersion, &result.Envelope.AgentAppRevision, &result.Envelope.AgentContentDigest,
			&result.Envelope.ConfigVersion, &result.Envelope.PolicyVersion, &result.Envelope.RequestID,
			&result.Envelope.SessionID, &result.Envelope.UserID, &result.Envelope.Channel, &result.Envelope.InputSeq,
			&result.Envelope.PayloadRef, &result.Envelope.TraceParent, &result.Outcome, &result.ResultRef, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return gateway.ExecutionStatus{}, runtime.ErrNotFound
	}
	if err != nil {
		return gateway.ExecutionStatus{}, err
	}
	result.Envelope.SchemaVersion = runtime.CurrentEnvelopeSchemaVersion
	result.Envelope.CreatedAt = created
	return result, nil
}

func (s *TaskStore) RequestCancel(ctx context.Context, in gateway.CancelRequest) (gateway.CancelResult, error) {
	var result gateway.CancelResult
	err := s.db.QueryRowContext(ctx, `SELECT accepted,execution_version FROM request_cancel_execution($1,$2,$3,$4)`,
		in.TenantID, in.RequestID, in.ExpectedVersion, nil).Scan(&result.Accepted, &result.Version)
	return result, translate(err)
}

func (s *TaskStore) ParkInput(ctx context.Context, in gateway.ParkRequest) error {
	_, err := s.db.ExecContext(ctx, `SELECT park_execution($1,$2,$3,$4)`, in.TenantID, in.RequestID, in.InputSeq, in.Attempt)
	return translate(err)
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type sqlStater interface{ SQLState() string }

func translate(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001":
			return runtime.ErrVersionConflict
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "42501":
			return runtime.ErrTenantScope
		case "P0002":
			return runtime.ErrNotFound
		case "55000":
			if strings.Contains(strings.ToLower(err.Error()), "park") {
				return runtime.ErrCommitConflict
			}
			return runtime.ErrVersionMismatch
		}
	}
	return err
}

var _ gateway.TaskStore = (*TaskStore)(nil)
