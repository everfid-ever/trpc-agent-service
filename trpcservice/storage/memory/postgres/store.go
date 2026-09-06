// Package postgres implements the PostgreSQL durable Memory authority.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/memory"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) Put(ctx context.Context, request memory.PutRequest) (memory.Entry, error) {
	if s == nil || s.db == nil {
		return memory.Entry{}, memory.ErrInvalid
	}
	if err := validatePut(request); err != nil {
		return memory.Entry{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return memory.Entry{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var mutation memory.Entry
	var attributes []byte
	err = tx.QueryRowContext(ctx, `SELECT scope,subject_id,memory_id,entry_version,tenant_watermark,content_ref,content_digest,attributes,created_at,updated_at
FROM memory_mutation WHERE tenant_id=$1 AND mutation_id=$2`, request.TenantID, request.MutationID).
		Scan(&mutation.Scope, &mutation.SubjectID, &mutation.MemoryID, &mutation.Version, &mutation.TenantWatermark, &mutation.ContentRef, &mutation.ContentDigest, &attributes, &mutation.CreatedAt, &mutation.UpdatedAt)
	if err == nil {
		mutation.TenantID = request.TenantID
		mutation.Attributes, err = decodeAttrs(attributes)
		if err != nil {
			return memory.Entry{}, memory.ErrInvalid
		}
		if !sameMutation(mutation, request) {
			return memory.Entry{}, memory.ErrVersionConflict
		}
		if err = tx.Commit(); err != nil {
			return memory.Entry{}, err
		}
		return mutation, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return memory.Entry{}, classify(err)
	}
	var currentVersion int64
	err = tx.QueryRowContext(ctx, `SELECT version FROM memory_entry WHERE tenant_id=$1 AND scope=$2 AND subject_id=$3 AND memory_id=$4 FOR UPDATE`, request.TenantID, request.Scope, request.SubjectID, request.MemoryID).Scan(&currentVersion)
	if errors.Is(err, sql.ErrNoRows) {
		if request.ExpectedVersion != 0 {
			return memory.Entry{}, memory.ErrVersionConflict
		}
		currentVersion = 0
	} else if err != nil {
		return memory.Entry{}, classify(err)
	} else if currentVersion != request.ExpectedVersion {
		return memory.Entry{}, memory.ErrVersionConflict
	}
	attrs, err := json.Marshal(request.Attributes)
	if err != nil {
		return memory.Entry{}, memory.ErrInvalid
	}
	var watermark int64
	if err = tx.QueryRowContext(ctx, `INSERT INTO memory_watermark(tenant_id,version) VALUES($1,1) ON CONFLICT(tenant_id) DO UPDATE SET version=memory_watermark.version+1,updated_at=now() RETURNING version`, request.TenantID).Scan(&watermark); err != nil {
		return memory.Entry{}, classify(err)
	}
	entry := request.Entry
	entry.Version = currentVersion + 1
	entry.TenantWatermark = watermark
	if currentVersion == 0 {
		err = tx.QueryRowContext(ctx, `INSERT INTO memory_entry(tenant_id,scope,subject_id,memory_id,version,tenant_watermark,content_ref,content_digest,attributes)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb) RETURNING created_at,updated_at`, entry.TenantID, entry.Scope, entry.SubjectID, entry.MemoryID, entry.Version, entry.TenantWatermark, entry.ContentRef, entry.ContentDigest, string(attrs)).Scan(&entry.CreatedAt, &entry.UpdatedAt)
	} else {
		err = tx.QueryRowContext(ctx, `UPDATE memory_entry SET version=$5,tenant_watermark=$6,content_ref=$7,content_digest=$8,attributes=$9::jsonb,updated_at=now()
WHERE tenant_id=$1 AND scope=$2 AND subject_id=$3 AND memory_id=$4 AND version=$10 RETURNING created_at,updated_at`, entry.TenantID, entry.Scope, entry.SubjectID, entry.MemoryID, entry.Version, entry.TenantWatermark, entry.ContentRef, entry.ContentDigest, string(attrs), request.ExpectedVersion).Scan(&entry.CreatedAt, &entry.UpdatedAt)
	}
	if err != nil {
		return memory.Entry{}, classify(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO memory_mutation(tenant_id,mutation_id,scope,subject_id,memory_id,entry_version,tenant_watermark,content_ref,content_digest,attributes,created_at,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12)`, entry.TenantID, request.MutationID, entry.Scope, entry.SubjectID, entry.MemoryID, entry.Version, entry.TenantWatermark, entry.ContentRef, entry.ContentDigest, string(attrs), entry.CreatedAt, entry.UpdatedAt); err != nil {
		return memory.Entry{}, classify(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO memory_index_intent(tenant_id,mutation_id,scope,subject_id,memory_id,entry_version,tenant_watermark,content_ref,content_digest,state)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending')`, entry.TenantID, request.MutationID, entry.Scope, entry.SubjectID, entry.MemoryID, entry.Version, entry.TenantWatermark, entry.ContentRef, entry.ContentDigest); err != nil {
		return memory.Entry{}, classify(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
VALUES($1,$2,'memory-invalidation',$3,$4,$5,$6)`, entry.TenantID, fmt.Sprintf("memory-invalidation:%s:%d", entry.TenantID, entry.TenantWatermark), entry.TenantID, entry.TenantWatermark, fmt.Sprintf("memory:%s:%d:invalidate", entry.TenantID, entry.TenantWatermark), fmt.Sprintf("memory://%s/%d", entry.TenantID, entry.TenantWatermark)); err != nil {
		return memory.Entry{}, classify(err)
	}
	if err = tx.Commit(); err != nil {
		return memory.Entry{}, err
	}
	return entry, nil
}

func (s *Store) Get(ctx context.Context, key memory.Key) (memory.Entry, error) {
	if s == nil || s.db == nil {
		return memory.Entry{}, memory.ErrInvalid
	}
	if err := key.Validate(); err != nil {
		return memory.Entry{}, err
	}
	return scan(s.db.QueryRowContext(ctx, `SELECT version,tenant_watermark,content_ref,content_digest,attributes,created_at,updated_at FROM memory_entry WHERE tenant_id=$1 AND scope=$2 AND subject_id=$3 AND memory_id=$4`, key.TenantID, key.Scope, key.SubjectID, key.MemoryID), key)
}
func (s *Store) List(ctx context.Context, q memory.Query) ([]memory.Entry, error) {
	if s == nil || s.db == nil || q.TenantID == "" || q.Scope == "" || (q.Scope == memory.ScopeUser && q.SubjectID == "") || (q.Scope == memory.ScopeTenant && q.SubjectID != "") {
		return nil, memory.ErrInvalid
	}
	limit := q.Limit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT memory_id,version,tenant_watermark,content_ref,content_digest,attributes,created_at,updated_at FROM memory_entry WHERE tenant_id=$1 AND scope=$2 AND subject_id=$3 ORDER BY memory_id LIMIT $4`, q.TenantID, q.Scope, q.SubjectID, limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	result := []memory.Entry{}
	for rows.Next() {
		var e memory.Entry
		var attrs []byte
		e.Key = memory.Key{TenantID: q.TenantID, Scope: q.Scope, SubjectID: q.SubjectID}
		if err = rows.Scan(&e.MemoryID, &e.Version, &e.TenantWatermark, &e.ContentRef, &e.ContentDigest, &attrs, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		e.Attributes, err = decodeAttrs(attrs)
		if err != nil {
			return nil, memory.ErrInvalid
		}
		result = append(result, e)
	}
	return result, rows.Err()
}
func (s *Store) TenantWatermark(ctx context.Context, tenantID string) (int64, error) {
	if s == nil || s.db == nil || tenantID == "" {
		return 0, memory.ErrInvalid
	}
	var v int64
	err := s.db.QueryRowContext(ctx, `SELECT version FROM memory_watermark WHERE tenant_id=$1`, tenantID).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, classify(err)
}

type scanner interface{ Scan(...any) error }

func scan(row scanner, key memory.Key) (memory.Entry, error) {
	var e memory.Entry
	var attrs []byte
	e.Key = key
	err := row.Scan(&e.Version, &e.TenantWatermark, &e.ContentRef, &e.ContentDigest, &attrs, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		return memory.Entry{}, classify(err)
	}
	e.Attributes, err = decodeAttrs(attrs)
	if err != nil {
		return memory.Entry{}, memory.ErrInvalid
	}
	return e, nil
}
func validatePut(r memory.PutRequest) error {
	if r.Key.Validate() != nil || r.MutationID == "" || r.ExpectedVersion < 0 || r.ContentRef == "" || len(r.ContentDigest) != 64 {
		return memory.ErrInvalid
	}
	return nil
}
func sameMutation(e memory.Entry, r memory.PutRequest) bool {
	if e.Key != r.Key || e.ContentRef != r.ContentRef || e.ContentDigest != r.ContentDigest {
		return false
	}
	return equalAttrs(e.Attributes, r.Attributes)
}
func equalAttrs(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
func decodeAttrs(data []byte) (map[string]string, error) {
	out := map[string]string{}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}
func classify(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return memory.ErrNotFound
	}
	return err
}

var _ memory.Store = (*Store)(nil)
