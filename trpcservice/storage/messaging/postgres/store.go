// Package postgres implements durable Inbox, Outbox and delivery stores.
package postgres

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	cryptorand "crypto/rand"
	"database/sql"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type Store struct {
	db                *sql.DB
	payloadKey        []byte
	payloadKeyVersion int64
}

func New(db *sql.DB) *Store { return &Store{db: db} }

func NewWithPayloadKey(db *sql.DB, key []byte, keyVersion int64) *Store {
	return &Store{db: db, payloadKey: append([]byte(nil), key...), payloadKeyVersion: keyVersion}
}

func (s *Store) PutPayload(ctx context.Context, in messaging.PayloadRecord) error {
	if in.TenantID == "" || in.RequestID == "" || in.PayloadRef == "" || in.ContentDigest == "" || len(in.Content) == 0 {
		return runtime.ErrCommitConflict
	}
	if len(s.payloadKey) == 0 || s.payloadKeyVersion < 1 {
		return runtime.ErrCapabilityUnsupported
	}
	if in.KeyVersion != 0 && in.KeyVersion != s.payloadKeyVersion {
		return runtime.ErrVersionMismatch
	}
	ciphertext, nonce, err := encryptPayload(s.payloadKey, payloadAAD(in), in.Content)
	if err != nil {
		return err
	}
	var storedRef, storedDigest string
	var storedCiphertext, storedNonce []byte
	var storedKeyVersion int64
	err = s.db.QueryRowContext(ctx, `INSERT INTO inbound_payload(tenant_id,request_id,payload_ref,payload_ciphertext,payload_nonce,content_digest,key_version)
VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (tenant_id,request_id) DO UPDATE SET request_id=EXCLUDED.request_id
RETURNING payload_ref,content_digest,payload_ciphertext,payload_nonce,key_version`, in.TenantID, in.RequestID, in.PayloadRef, ciphertext, nonce, in.ContentDigest, s.payloadKeyVersion).
		Scan(&storedRef, &storedDigest, &storedCiphertext, &storedNonce, &storedKeyVersion)
	if err != nil {
		return translate(err)
	}
	if storedRef != in.PayloadRef || storedDigest != in.ContentDigest || storedKeyVersion != s.payloadKeyVersion {
		return runtime.ErrIdempotencyCollision
	}
	storedContent, err := decryptPayload(s.payloadKey, payloadAAD(in), storedCiphertext, storedNonce)
	if err != nil || string(storedContent) != string(in.Content) {
		return runtime.ErrIdempotencyCollision
	}
	return nil
}

func (s *Store) GetPayload(ctx context.Context, tenantID, requestID string) (messaging.PayloadRecord, error) {
	if len(s.payloadKey) == 0 || s.payloadKeyVersion < 1 {
		return messaging.PayloadRecord{}, runtime.ErrCapabilityUnsupported
	}
	var record messaging.PayloadRecord
	record.TenantID, record.RequestID = tenantID, requestID
	var ciphertext, nonce []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload_ref,content_digest,payload_ciphertext,payload_nonce,key_version,created_at FROM inbound_payload
WHERE tenant_id=$1 AND request_id=$2`, tenantID, requestID).
		Scan(&record.PayloadRef, &record.ContentDigest, &ciphertext, &nonce, &record.KeyVersion, &record.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.PayloadRecord{}, runtime.ErrNotFound
	}
	if err != nil {
		return messaging.PayloadRecord{}, err
	}
	if record.KeyVersion != s.payloadKeyVersion {
		return messaging.PayloadRecord{}, runtime.ErrVersionMismatch
	}
	record.Content, err = decryptPayload(s.payloadKey, payloadAAD(record), ciphertext, nonce)
	return record, err
}

func (s *Store) PutResult(ctx context.Context, in messaging.ResultRecord) error {
	if in.TenantID == "" || in.RequestID == "" || in.ResultRef == "" || in.ContentDigest == "" || len(in.Content) == 0 {
		return runtime.ErrCommitConflict
	}
	if len(s.payloadKey) == 0 || s.payloadKeyVersion < 1 {
		return runtime.ErrCapabilityUnsupported
	}
	if in.KeyVersion != 0 && in.KeyVersion != s.payloadKeyVersion {
		return runtime.ErrVersionMismatch
	}
	aad := resultAAD(in)
	ciphertext, nonce, err := encryptPayload(s.payloadKey, aad, in.Content)
	if err != nil {
		return err
	}
	var ref, digest string
	var encrypted, storedNonce []byte
	var version int64
	err = s.db.QueryRowContext(ctx, `INSERT INTO result_payload(tenant_id,request_id,result_ref,result_ciphertext,result_nonce,content_digest,key_version)
VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (tenant_id,request_id) DO UPDATE SET request_id=EXCLUDED.request_id
RETURNING result_ref,content_digest,result_ciphertext,result_nonce,key_version`, in.TenantID, in.RequestID, in.ResultRef, ciphertext, nonce, in.ContentDigest, s.payloadKeyVersion).
		Scan(&ref, &digest, &encrypted, &storedNonce, &version)
	if err != nil {
		return translate(err)
	}
	if ref != in.ResultRef || digest != in.ContentDigest || version != s.payloadKeyVersion {
		return runtime.ErrIdempotencyCollision
	}
	content, err := decryptPayload(s.payloadKey, aad, encrypted, storedNonce)
	if err != nil || string(content) != string(in.Content) {
		return runtime.ErrIdempotencyCollision
	}
	return nil
}

func (s *Store) GetResult(ctx context.Context, tenantID, requestID string) (messaging.ResultRecord, error) {
	if len(s.payloadKey) == 0 || s.payloadKeyVersion < 1 {
		return messaging.ResultRecord{}, runtime.ErrCapabilityUnsupported
	}
	record := messaging.ResultRecord{TenantID: tenantID, RequestID: requestID}
	var ciphertext, nonce []byte
	err := s.db.QueryRowContext(ctx, `SELECT result_ref,content_digest,result_ciphertext,result_nonce,key_version,created_at FROM result_payload WHERE tenant_id=$1 AND request_id=$2`, tenantID, requestID).
		Scan(&record.ResultRef, &record.ContentDigest, &ciphertext, &nonce, &record.KeyVersion, &record.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.ResultRecord{}, runtime.ErrNotFound
	}
	if err != nil {
		return messaging.ResultRecord{}, err
	}
	if record.KeyVersion != s.payloadKeyVersion {
		return messaging.ResultRecord{}, runtime.ErrVersionMismatch
	}
	record.Content, err = decryptPayload(s.payloadKey, resultAAD(record), ciphertext, nonce)
	return record, err
}

func resultAAD(record messaging.ResultRecord) []byte {
	return []byte(record.TenantID + "\x00" + record.RequestID + "\x00" + record.ResultRef + "\x00" + record.ContentDigest)
}

func payloadAAD(record messaging.PayloadRecord) []byte {
	return []byte(record.TenantID + "\x00" + record.RequestID + "\x00" + record.PayloadRef + "\x00" + record.ContentDigest)
}

func encryptPayload(key, aad, plaintext []byte) ([]byte, []byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, runtime.ErrCapabilityUnsupported
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := cryptorand.Read(nonce); err != nil {
		return nil, nil, err
	}
	return aead.Seal(nil, nonce, plaintext, aad), nonce, nil
}

func decryptPayload(key, aad, ciphertext, nonce []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, runtime.ErrVersionMismatch
	}
	return plaintext, nil
}

func (s *Store) ClaimInbox(ctx context.Context, in messaging.ClaimInboxRequest) (messaging.InboxRecord, error) {
	if in.TenantID == "" || in.Channel == "" || in.ExternalAccountID == "" || in.ExternalMessageID == "" {
		return messaging.InboxRecord{}, runtime.ErrTenantScope
	}
	if in.AgentAppID == "" || in.PayloadDigest == "" || in.KeyVersion < 1 {
		return messaging.InboxRecord{}, runtime.ErrCommitConflict
	}
	requestID, payloadRef := messaging.StableInboxIdentity(in.InboxKey)
	row := s.db.QueryRowContext(ctx, `SELECT tenant_id,channel,external_account_id,external_message_id,
request_id,agent_app_id,COALESCE(session_id,''),COALESCE(input_seq,0),state,payload_ref,payload_digest,
key_version,version,COALESCE(terminal_reason,''),COALESCE(result_ref,''),created_at,updated_at
FROM claim_inbox($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		in.TenantID, in.Channel, in.ExternalAccountID, in.ExternalMessageID, requestID,
		in.AgentAppID, nullable(in.SessionID), payloadRef, in.PayloadDigest, in.KeyVersion, string(in.InitialState))
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

func (s *Store) RenewOutboxClaim(ctx context.Context, tenantID, outboxID string, expectedVersion uint64, owner string, until time.Time) (uint64, error) {
	if owner == "" || until.IsZero() {
		return 0, runtime.ErrCommitConflict
	}
	var version uint64
	err := s.db.QueryRowContext(ctx, `UPDATE outbox SET claim_until=$5,version=version+1
WHERE tenant_id=$1 AND outbox_id=$2 AND version=$3 AND state='claimed' AND claim_owner=$4
RETURNING version`, tenantID, outboxID, expectedVersion, owner, until).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, runtime.ErrVersionConflict
	}
	return version, err
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
var _ messaging.PayloadStore = (*Store)(nil)
var _ messaging.ResultStore = (*Store)(nil)
var _ messaging.OutboxStore = (*Store)(nil)
var _ messaging.DeliveryLedger = (*Store)(nil)
