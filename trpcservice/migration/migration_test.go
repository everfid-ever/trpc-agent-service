package migration

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestTransitionGatesFailClosed(t *testing.T) {
	clock := time.Date(2026, 9, 8, 8, 0, 0, 0, time.UTC)
	current, err := NewMigration(CreateRequest{TenantID: "tenant-a", MigrationID: "migration-1", Domain: "session", Epoch: 1,
		Source: Binding{ConfigVersion: 1, BackendProfileID: "source", BackendVersion: 1},
		Target: Binding{ConfigVersion: 2, BackendProfileID: "target", BackendVersion: 1}, CreatedAt: clock})
	if err != nil {
		t.Fatal(err)
	}
	request := TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		To: StateDualWrite, At: clock.Add(time.Minute), DualWriteRef: "log://migration"}
	if _, err := ApplyTransition(current, request); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("skipped state accepted: %v", err)
	}
	request.To, request.SnapshotWatermark = StateSnapshot, "snapshot:1"
	current, err = ApplyTransition(current, request)
	if err != nil {
		t.Fatal(err)
	}
	request = TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		To: StateDualWrite, At: clock.Add(2 * time.Minute), DualWriteRef: "log://migration"}
	current, err = ApplyTransition(current, request)
	if err != nil {
		t.Fatal(err)
	}
	request = TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		To: StateBackfill, At: clock.Add(3 * time.Minute)}
	current, err = ApplyTransition(current, request)
	if err != nil {
		t.Fatal(err)
	}
	request = TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		To: StateVerify, At: clock.Add(4 * time.Minute)}
	if _, err := ApplyTransition(current, request); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("verify before complete accepted: %v", err)
	}
}

func TestDigestCanonicalForm(t *testing.T) {
	if validDigest(strings.Repeat("A", 64)) {
		t.Fatal("uppercase digest accepted")
	}
	if !validDigest(strings.Repeat("a", 64)) {
		t.Fatal("lowercase digest rejected")
	}
}

func TestBatchAndCutoverEvidenceFailClosed(t *testing.T) {
	clock := time.Date(2026, 9, 8, 8, 0, 0, 0, time.UTC)
	current := Migration{TenantID: "tenant-a", MigrationID: "migration-1", Domain: "session", Epoch: 3,
		Source: Binding{ConfigVersion: 1, BackendProfileID: "source", BackendVersion: 1},
		Target: Binding{ConfigVersion: 2, BackendProfileID: "target", BackendVersion: 1}, State: StateBackfill,
		BackfillCheckpoint: "cursor:10", NextBatchSeq: 2, Version: 4, CreatedAt: clock, UpdatedAt: clock}
	batch := BatchRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, BatchID: "batch-2", Epoch: 3,
		ExpectedVersion: 4, BatchSeq: 2, FromCheckpoint: "wrong", ToCheckpoint: "cursor:20",
		Digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RecordCount: 10, CommittedAt: clock.Add(time.Minute)}
	if _, _, err := ApplyBatch(current, batch); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("discontinuous checkpoint accepted: %v", err)
	}
	current.State, current.BackfillComplete, current.Version = StateVerify, true, 6
	verification := Verification{SourceCount: 10, TargetCount: 9,
		SourceDigest:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetDigest:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SourceWatermark: "watermark:10", TargetWatermark: "watermark:10",
		SampleDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	request := TransitionRequest{TenantID: current.TenantID, MigrationID: current.MigrationID, ExpectedVersion: current.Version,
		To: StateCutover, At: clock.Add(2 * time.Minute), Verification: verification, CutoverConfigVersion: 2}
	if _, err := ApplyTransition(current, request); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("divergent verification accepted: %v", err)
	}
}
