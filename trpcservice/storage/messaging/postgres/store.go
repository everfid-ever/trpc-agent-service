// Package postgres implements durable Inbox, Outbox and delivery stores.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) ClaimInbox(ctx context.Context, in messaging.ClaimInboxRequest) (messaging.InboxRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT tenant_id,channel,external_account_id,external_message_id,
request_id,agent_app_id,COALESCE(session_id,''),COALESCE(input_seq,0),state,payload_ref,payload_digest,
key_version,version,COALESCE(terminal_reason,''),COALESCE(result_ref,''),created_at,updated_at
FROM claim_inbox($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		in.TenantID, in.Channel, in.ExternalAccountID, in.ExternalMessageID, in.RequestID,
		in.AgentAppID, nullable(in.SessionID), in.PayloadRef, in.PayloadDigest, in.KeyVersion, string(in.InitialState))
	record, err := scanInbox(row)
	return record, translate(err)
}

func (s *Store) GetInbox(ctx context.Context, key messaging.InboxKey) (messaging.InboxRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT tenant_id,channel,external_account_id,external_message_id,
request_id,agent_app_id,COALESCE(session_id,''),COALESCE(input_seq,0),state,payload_ref,payload_digest,
key_version,version,COALESCE(terminal_reason,''),COALESCE(result_ref,''),created_at,updated_at
FROM inbox WHERE tenant_id=$1 AND channel=$2 AND external_account_id=$3 AND external_message_id=$4`,
		key.TenantID, key.Channel, key.ExternalAccountID, key.ExternalMessageID)
	return scanInbox(row)
}

type rowScanner interface{ Scan(...any) error }

func scanInbox(row rowScanner) (messaging.InboxRecord, error) {
	var record messaging.InboxRecord
	err := row.Scan(&record.TenantID, &record.Channel, &record.ExternalAccountID, &record.ExternalMessageID,
		&record.RequestID, &record.AgentAppID, &record.SessionID, &record.InputSeq, &record.State,
		&record.PayloadRef, &record.PayloadDigest, &record.KeyVersion, &record.Version,
		&record.TerminalReason, &record.ResultRef, &record.CreatedAt, &record.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.InboxRecord{}, runtime.ErrNotFound
	}
	return record, err
}

func (s *Store) ClaimOutbox(ctx context.Context, kind string, limit int, owner string, until time.Time) ([]messaging.OutboxRecord, error) {
	if kind == "" || limit < 1 || owner == "" || until.IsZero() {
		return nil, runtime.ErrCommitConflict
	}
	rows, err := s.db.QueryContext(ctx, `WITH candidates AS (
  SELECT tenant_id,outbox_id FROM outbox
  WHERE kind=$1 AND ((state IN ('pending','retry_wait') AND next_attempt_at<=now()) OR (state='claimed' AND claim_until<now()))
  ORDER BY next_attempt_at,created_at FOR UPDATE SKIP LOCKED LIMIT $2
)
UPDATE outbox o SET state='claimed',claim_owner=$3,claim_until=$4,version=o.version+1,attempt=o.attempt+1
FROM candidates c WHERE o.tenant_id=c.tenant_id AND o.outbox_id=c.outbox_id
RETURNING o.tenant_id,o.outbox_id,o.kind,o.aggregate_id,o.event_seq,o.idempotency_key,o.payload_ref,
o.traceparent,o.state,o.version,o.attempt,o.next_attempt_at,o.claim_owner,o.claim_until,o.created_at`, kind, limit, owner, until)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []messaging.OutboxRecord
	for rows.Next() {
		var record messaging.OutboxRecord
		var trace sql.NullString
		if err := rows.Scan(&record.TenantID, &record.OutboxID, &record.Kind, &record.AggregateID,
			&record.EventSeq, &record.IdempotencyKey, &record.PayloadRef, &trace, &record.State,
			&record.Version, &record.Attempt, &record.NextAttemptAt, &record.ClaimOwner, &record.ClaimUntil, &record.CreatedAt); err != nil {
			return nil, err
		}
		record.TraceParent = trace.String
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) MarkPublished(ctx context.Context, tenantID, outboxID string, expectedVersion uint64) error {
	result, err := s.db.ExecContext(ctx, `UPDATE outbox SET state='published',published_at=now(),claim_owner=NULL,claim_until=NULL,version=version+1
WHERE tenant_id=$1 AND outbox_id=$2 AND version=$3 AND state='claimed'`, tenantID, outboxID, expectedVersion)
	return casResult(result, err)
}

func (s *Store) MarkRetry(ctx context.Context, tenantID, outboxID string, expectedVersion uint64, next time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE outbox SET state='retry_wait',next_attempt_at=$4,claim_owner=NULL,claim_until=NULL,version=version+1
WHERE tenant_id=$1 AND outbox_id=$2 AND version=$3 AND state='claimed'`, tenantID, outboxID, expectedVersion, next)
	return casResult(result, err)
}

func (s *Store) GetDelivery(ctx context.Context, key messaging.DeliveryKey) (messaging.DeliveryRecord, error) {
	var record messaging.DeliveryRecord
	record.DeliveryKey = key
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(provider_message_id,''),state,version,updated_at FROM delivery_ledger
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3`, key.TenantID, key.DeliveryKey, key.SegmentNo).
		Scan(&record.ProviderMessageID, &record.State, &record.Version, &record.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, runtime.ErrNotFound
	}
	return record, err
}

func (s *Store) RecordDelivery(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	err := s.db.QueryRowContext(ctx, `INSERT INTO delivery_ledger(tenant_id,delivery_key,segment_no,provider_message_id,state,version)
SELECT $1,$2,$3,$4,$5,1 WHERE $6=0
ON CONFLICT (tenant_id,delivery_key,segment_no) DO UPDATE SET provider_message_id=EXCLUDED.provider_message_id,
state=EXCLUDED.state,version=delivery_ledger.version+1,updated_at=now()
WHERE delivery_ledger.version=$6
RETURNING version,updated_at`, in.TenantID, in.DeliveryKey.DeliveryKey, in.SegmentNo, nullable(in.ProviderMessageID), in.State, expectedVersion).
		Scan(&in.Version, &in.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	return in, err
}

func casResult(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return runtime.ErrVersionConflict
	}
	return nil
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
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "42501":
			return runtime.ErrTenantScope
		case "P0002":
			return runtime.ErrNotFound
		case "40001":
			return runtime.ErrVersionConflict
		}
	}
	return err
}

var _ messaging.InboxClaimer = (*Store)(nil)
var _ messaging.OutboxStore = (*Store)(nil)
var _ messaging.DeliveryLedger = (*Store)(nil)
