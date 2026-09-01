// Package migration coordinates durable online tenant backend migration.
package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type State string

const (
	StatePlanned   State = "planned"
	StateSnapshot  State = "snapshot"
	StateDualWrite State = "dual_write"
	StateBackfill  State = "backfill"
	StateVerify    State = "verify"
	StateCutover   State = "cutover"
	StateObserve   State = "observe"
	StateCleanup   State = "cleanup"
)

type Binding struct {
	ConfigVersion    int64
	BackendProfileID string
	BackendVersion   int64
}

type Verification struct {
	SourceCount, TargetCount                       int64
	SourceDigest, TargetDigest                     string
	SourceWatermark, TargetWatermark, SampleDigest string
}

type Migration struct {
	TenantID, MigrationID, Domain string
	Epoch                         int64
	Source, Target                Binding
	State                         State
	SnapshotWatermark             string
	DualWriteRef                  string
	BackfillCheckpoint            string
	NextBatchSeq, BackfillCount   int64
	BackfillComplete              bool
	Verification                  Verification
	CutoverConfigVersion          int64
	CutoverAt, ObserveUntil       time.Time
	RollbackSyncWatermark         string
	CreatedAt, UpdatedAt          time.Time
	Version                       int64
}

type CreateRequest struct {
	TenantID, MigrationID, Domain string
	Epoch                         int64
	Source, Target                Binding
	CreatedAt                     time.Time
}

type TransitionRequest struct {
	TenantID, MigrationID string
	ExpectedVersion       int64
	To                    State
	At                    time.Time
	SnapshotWatermark     string
	DualWriteRef          string
	Verification          Verification
	CutoverConfigVersion  int64
	ObserveUntil          time.Time
	RollbackSyncWatermark string
}

type BatchRequest struct {
	TenantID, MigrationID, BatchID       string
	Epoch, ExpectedVersion, BatchSeq     int64
	FromCheckpoint, ToCheckpoint, Digest string
	RecordCount                          int64
	Complete                             bool
	CommittedAt                          time.Time
}

type Batch struct {
	BatchRequest
	ResultVersion int64
}

type BatchResult struct {
	Migration Migration
	Batch     Batch
}

type Repository interface {
	Create(context.Context, CreateRequest) (Migration, error)
	Get(context.Context, string, string) (Migration, error)
	Transition(context.Context, TransitionRequest) (Migration, error)
	CommitBatch(context.Context, BatchRequest) (BatchResult, error)
}

func NewMigration(in CreateRequest) (Migration, error) {
	if !validText(in.TenantID, 128) || !validText(in.MigrationID, 128) || !validText(in.Domain, 32) || in.Epoch < 1 ||
		!validBinding(in.Source) || !validBinding(in.Target) || in.Source == in.Target || in.CreatedAt.IsZero() {
		return Migration{}, runtime.ErrInvariantViolation
	}
	return Migration{TenantID: in.TenantID, MigrationID: in.MigrationID, Domain: in.Domain, Epoch: in.Epoch,
		Source: in.Source, Target: in.Target, State: StatePlanned, NextBatchSeq: 1,
		CreatedAt: in.CreatedAt.UTC(), UpdatedAt: in.CreatedAt.UTC(), Version: 1}, nil
}

func ApplyTransition(current Migration, in TransitionRequest) (Migration, error) {
	if in.TenantID != current.TenantID || in.MigrationID != current.MigrationID {
		return Migration{}, runtime.ErrTenantScope
	}
	if in.ExpectedVersion != current.Version {
		return Migration{}, runtime.ErrVersionConflict
	}
	if in.At.IsZero() || in.At.Before(current.UpdatedAt) || nextState(current.State) != in.To {
		return Migration{}, runtime.ErrInvariantViolation
	}
	next := current
	switch in.To {
	case StateSnapshot:
		if !validText(in.SnapshotWatermark, 512) {
			return Migration{}, runtime.ErrInvariantViolation
		}
		next.SnapshotWatermark = in.SnapshotWatermark
	case StateDualWrite:
		if !validText(in.DualWriteRef, 512) {
			return Migration{}, runtime.ErrInvariantViolation
		}
		next.DualWriteRef = in.DualWriteRef
	case StateBackfill:
		// Backfill starts at the empty checkpoint and advances only via CommitBatch.
	case StateVerify:
		if !current.BackfillComplete {
			return Migration{}, runtime.ErrInvariantViolation
		}
	case StateCutover:
		if !validVerification(in.Verification) || in.CutoverConfigVersion != current.Target.ConfigVersion {
			return Migration{}, runtime.ErrInvariantViolation
		}
		next.Verification = in.Verification
		next.CutoverConfigVersion = in.CutoverConfigVersion
		next.CutoverAt = in.At.UTC()
	case StateObserve:
		if in.ObserveUntil.IsZero() || !in.ObserveUntil.After(in.At) {
			return Migration{}, runtime.ErrInvariantViolation
		}
		next.ObserveUntil = in.ObserveUntil.UTC()
	case StateCleanup:
		if current.ObserveUntil.IsZero() || in.At.Before(current.ObserveUntil) || in.RollbackSyncWatermark != current.Verification.TargetWatermark {
			return Migration{}, runtime.ErrInvariantViolation
		}
		next.RollbackSyncWatermark = in.RollbackSyncWatermark
	default:
		return Migration{}, runtime.ErrInvariantViolation
	}
	next.State = in.To
	next.Version++
	next.UpdatedAt = in.At.UTC()
	return next, nil
}

func ApplyBatch(current Migration, in BatchRequest) (Migration, Batch, error) {
	if in.TenantID != current.TenantID || in.MigrationID != current.MigrationID {
		return Migration{}, Batch{}, runtime.ErrTenantScope
	}
	if current.State != StateBackfill || current.BackfillComplete || in.Epoch != current.Epoch || in.ExpectedVersion != current.Version ||
		in.BatchSeq != current.NextBatchSeq || in.FromCheckpoint != current.BackfillCheckpoint ||
		!validText(in.BatchID, 128) || !validText(in.ToCheckpoint, 512) || in.ToCheckpoint == in.FromCheckpoint ||
		!validDigest(in.Digest) || in.RecordCount < 0 || (in.RecordCount == 0 && !in.Complete) || in.CommittedAt.IsZero() || in.CommittedAt.Before(current.UpdatedAt) {
		return Migration{}, Batch{}, runtime.ErrInvariantViolation
	}
	next := current
	next.BackfillCheckpoint = in.ToCheckpoint
	next.NextBatchSeq++
	next.BackfillCount += in.RecordCount
	next.BackfillComplete = in.Complete
	next.Version++
	next.UpdatedAt = in.CommittedAt.UTC()
	return next, Batch{BatchRequest: in, ResultVersion: next.Version}, nil
}

func BatchDigest(in BatchRequest) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{in.TenantID, in.MigrationID, in.BatchID, in.FromCheckpoint,
		in.ToCheckpoint, in.Digest, strconv.FormatInt(in.Epoch, 10), strconv.FormatInt(in.BatchSeq, 10),
		strconv.FormatInt(in.RecordCount, 10), strconv.FormatBool(in.Complete)}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func SameBatch(left Batch, right BatchRequest) bool {
	return left.TenantID == right.TenantID && left.MigrationID == right.MigrationID && left.BatchID == right.BatchID &&
		left.Epoch == right.Epoch && left.BatchSeq == right.BatchSeq && left.FromCheckpoint == right.FromCheckpoint &&
		left.ToCheckpoint == right.ToCheckpoint && left.Digest == right.Digest && left.RecordCount == right.RecordCount && left.Complete == right.Complete
}

func nextState(state State) State {
	switch state {
	case StatePlanned:
		return StateSnapshot
	case StateSnapshot:
		return StateDualWrite
	case StateDualWrite:
		return StateBackfill
	case StateBackfill:
		return StateVerify
	case StateVerify:
		return StateCutover
	case StateCutover:
		return StateObserve
	case StateObserve:
		return StateCleanup
	default:
		return ""
	}
}

func validBinding(value Binding) bool {
	return value.ConfigVersion >= 1 && value.BackendVersion >= 1 && validText(value.BackendProfileID, 128)
}

func validVerification(value Verification) bool {
	return value.SourceCount >= 0 && value.SourceCount == value.TargetCount && validDigest(value.SourceDigest) &&
		value.SourceDigest == value.TargetDigest && validText(value.SourceWatermark, 512) &&
		value.SourceWatermark == value.TargetWatermark && validDigest(value.SampleDigest)
}

func validDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}
