package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) Create(ctx context.Context, in migration.CreateRequest) (migration.Migration, error) {
	created, err := migration.NewMigration(in)
	if err != nil {
		return migration.Migration{}, err
	}
	if s == nil || s.db == nil {
		return migration.Migration{}, runtime.ErrBackendUnavailable
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO public.backend_migration(
tenant_id,migration_id,domain,epoch,source_config_version,source_backend_profile_id,source_backend_version,
target_config_version,target_backend_profile_id,target_backend_version,state,created_at,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'planned',$11,$11)
ON CONFLICT (tenant_id,migration_id) DO NOTHING`, in.TenantID, in.MigrationID, in.Domain, in.Epoch,
		in.Source.ConfigVersion, in.Source.BackendProfileID, in.Source.BackendVersion,
		in.Target.ConfigVersion, in.Target.BackendProfileID, in.Target.BackendVersion, in.CreatedAt.UTC())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return migration.Migration{}, runtime.ErrVersionConflict
		}
		return migration.Migration{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return migration.Migration{}, err
	}
	if rows == 0 {
		existing, err := s.Get(ctx, in.TenantID, in.MigrationID)
		if err != nil {
			return migration.Migration{}, err
		}
		if !sameCreation(existing, created) {
			return migration.Migration{}, runtime.ErrIdempotencyCollision
		}
		return existing, nil
	}
	return s.Get(ctx, in.TenantID, in.MigrationID)
}

func (s *Store) Get(ctx context.Context, tenantID, migrationID string) (migration.Migration, error) {
	if s == nil || s.db == nil {
		return migration.Migration{}, runtime.ErrBackendUnavailable
	}
	return scanMigration(s.db.QueryRowContext(ctx, selectMigration+` WHERE tenant_id=$1 AND migration_id=$2`, tenantID, migrationID))
}

func (s *Store) Transition(ctx context.Context, in migration.TransitionRequest) (migration.Migration, error) {
	if s == nil || s.db == nil {
		return migration.Migration{}, runtime.ErrBackendUnavailable
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return migration.Migration{}, err
	}
	defer tx.Rollback()
	current, err := scanMigration(tx.QueryRowContext(ctx, selectMigration+` WHERE tenant_id=$1 AND migration_id=$2 FOR UPDATE`, in.TenantID, in.MigrationID))
	if err != nil {
		return migration.Migration{}, err
	}
	next, err := migration.ApplyTransition(current, in)
	if err != nil {
		return migration.Migration{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE public.backend_migration SET state=$3,snapshot_watermark=$4,dual_write_ref=$5,
verify_source_count=$6,verify_target_count=$7,verify_source_digest=$8,verify_target_digest=$9,
verify_source_watermark=$10,verify_target_watermark=$11,verify_sample_digest=$12,cutover_config_version=$13,
cutover_at=$14,observe_until=$15,rollback_sync_watermark=$16,version=$17,updated_at=$18
WHERE tenant_id=$1 AND migration_id=$2 AND version=$19`, next.TenantID, next.MigrationID, next.State,
		next.SnapshotWatermark, next.DualWriteRef, nullableCount(next.Verification.SourceCount, next.State),
		nullableCount(next.Verification.TargetCount, next.State), next.Verification.SourceDigest, next.Verification.TargetDigest,
		next.Verification.SourceWatermark, next.Verification.TargetWatermark, next.Verification.SampleDigest,
		nullableInt64(next.CutoverConfigVersion), nullableTime(next.CutoverAt), nullableTime(next.ObserveUntil),
		next.RollbackSyncWatermark, next.Version, next.UpdatedAt, current.Version)
	if err != nil {
		return migration.Migration{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return migration.Migration{}, runtime.ErrVersionConflict
	}
	if err := tx.Commit(); err != nil {
		return migration.Migration{}, err
	}
	return next, nil
}

func (s *Store) CommitBatch(ctx context.Context, in migration.BatchRequest) (migration.BatchResult, error) {
	if s == nil || s.db == nil {
		return migration.BatchResult{}, runtime.ErrBackendUnavailable
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return migration.BatchResult{}, err
	}
	defer tx.Rollback()
	current, err := scanMigration(tx.QueryRowContext(ctx, selectMigration+` WHERE tenant_id=$1 AND migration_id=$2 FOR UPDATE`, in.TenantID, in.MigrationID))
	if err != nil {
		return migration.BatchResult{}, err
	}
	existing, exists, err := readBatch(ctx, tx, in.TenantID, in.MigrationID, in.BatchID)
	if err != nil {
		return migration.BatchResult{}, err
	}
	if exists {
		if !migration.SameBatch(existing, in) {
			return migration.BatchResult{}, runtime.ErrIdempotencyCollision
		}
		return migration.BatchResult{Migration: current, Batch: existing}, nil
	}
	next, batch, err := migration.ApplyBatch(current, in)
	if err != nil {
		return migration.BatchResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO public.backend_migration_batch(
tenant_id,migration_id,batch_seq,batch_id,epoch,from_checkpoint,to_checkpoint,record_count,content_digest,complete,result_version,committed_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, in.TenantID, in.MigrationID, in.BatchSeq, in.BatchID,
		in.Epoch, in.FromCheckpoint, in.ToCheckpoint, in.RecordCount, in.Digest, in.Complete, batch.ResultVersion, in.CommittedAt.UTC()); err != nil {
		return migration.BatchResult{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE public.backend_migration SET backfill_checkpoint=$3,next_batch_seq=$4,
backfill_count=$5,backfill_complete=$6,version=$7,updated_at=$8
WHERE tenant_id=$1 AND migration_id=$2 AND version=$9`, next.TenantID, next.MigrationID, next.BackfillCheckpoint,
		next.NextBatchSeq, next.BackfillCount, next.BackfillComplete, next.Version, next.UpdatedAt, current.Version)
	if err != nil {
		return migration.BatchResult{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return migration.BatchResult{}, runtime.ErrVersionConflict
	}
	if err := tx.Commit(); err != nil {
		return migration.BatchResult{}, err
	}
	return migration.BatchResult{Migration: next, Batch: batch}, nil
}

const selectMigration = `SELECT tenant_id,migration_id,domain,epoch,
source_config_version,source_backend_profile_id,source_backend_version,
target_config_version,target_backend_profile_id,target_backend_version,state,
snapshot_watermark,dual_write_ref,backfill_checkpoint,next_batch_seq,backfill_count,backfill_complete,
verify_source_count,verify_target_count,verify_source_digest,verify_target_digest,
verify_source_watermark,verify_target_watermark,verify_sample_digest,cutover_config_version,cutover_at,
observe_until,rollback_sync_watermark,created_at,updated_at,version FROM public.backend_migration`

type scanner interface{ Scan(...any) error }

func scanMigration(row scanner) (migration.Migration, error) {
	var value migration.Migration
	var sourceCount, targetCount, cutoverConfig sql.NullInt64
	var cutoverAt, observeUntil sql.NullTime
	err := row.Scan(&value.TenantID, &value.MigrationID, &value.Domain, &value.Epoch,
		&value.Source.ConfigVersion, &value.Source.BackendProfileID, &value.Source.BackendVersion,
		&value.Target.ConfigVersion, &value.Target.BackendProfileID, &value.Target.BackendVersion, &value.State,
		&value.SnapshotWatermark, &value.DualWriteRef, &value.BackfillCheckpoint, &value.NextBatchSeq, &value.BackfillCount,
		&value.BackfillComplete, &sourceCount, &targetCount, &value.Verification.SourceDigest, &value.Verification.TargetDigest,
		&value.Verification.SourceWatermark, &value.Verification.TargetWatermark, &value.Verification.SampleDigest, &cutoverConfig,
		&cutoverAt, &observeUntil, &value.RollbackSyncWatermark, &value.CreatedAt, &value.UpdatedAt, &value.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return migration.Migration{}, runtime.ErrNotFound
	}
	if err != nil {
		return migration.Migration{}, err
	}
	if sourceCount.Valid {
		value.Verification.SourceCount = sourceCount.Int64
	}
	if targetCount.Valid {
		value.Verification.TargetCount = targetCount.Int64
	}
	if cutoverConfig.Valid {
		value.CutoverConfigVersion = cutoverConfig.Int64
	}
	if cutoverAt.Valid {
		value.CutoverAt = cutoverAt.Time.UTC()
	}
	if observeUntil.Valid {
		value.ObserveUntil = observeUntil.Time.UTC()
	}
	value.CreatedAt, value.UpdatedAt = value.CreatedAt.UTC(), value.UpdatedAt.UTC()
	return value, nil
}

func readBatch(ctx context.Context, tx *sql.Tx, tenantID, migrationID, batchID string) (migration.Batch, bool, error) {
	var value migration.Batch
	err := tx.QueryRowContext(ctx, `SELECT tenant_id,migration_id,batch_id,epoch,batch_seq,from_checkpoint,to_checkpoint,
content_digest,record_count,complete,result_version,committed_at FROM public.backend_migration_batch
WHERE tenant_id=$1 AND migration_id=$2 AND batch_id=$3`, tenantID, migrationID, batchID).Scan(
		&value.TenantID, &value.MigrationID, &value.BatchID, &value.Epoch, &value.BatchSeq, &value.FromCheckpoint,
		&value.ToCheckpoint, &value.Digest, &value.RecordCount, &value.Complete, &value.ResultVersion, &value.CommittedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return migration.Batch{}, false, nil
	}
	return value, err == nil, err
}

func nullableInt64(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

func nullableCount(value int64, state migration.State) any {
	if state != migration.StateCutover && state != migration.StateObserve && state != migration.StateCleanup {
		return nil
	}
	return value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func sameCreation(left, right migration.Migration) bool {
	return left.TenantID == right.TenantID && left.MigrationID == right.MigrationID && left.Domain == right.Domain &&
		left.Epoch == right.Epoch && left.Source == right.Source && left.Target == right.Target && left.CreatedAt.Equal(right.CreatedAt)
}

var _ migration.Repository = (*Store)(nil)
