package artifact_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	objectmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/inmemory"
)

func TestRetentionReconcilerDeletesByteaAndObjectArtifacts(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	content := []byte("object")
	_, object := lifecycleFixture(t, "tenant-a", "request-a", 0, content, now)
	objects := objectmemory.New()
	if _, err := objects.PutObject(context.Background(), object); err != nil {
		t.Fatal(err)
	}
	store := &retentionStoreStub{claimed: []artifact.RetainedArtifact{
		{TenantID: "tenant-a", ArtifactID: "bytea", Backend: artifact.RetentionBackendPostgres, State: artifact.RetentionDeleteClaimed, ClaimOwner: "cleaner", Version: 2},
		{TenantID: object.TenantID, ArtifactID: "object", Backend: artifact.RetentionBackendObject, ObjectKey: object.ObjectKey,
			ContentDigest: object.ContentDigest, State: artifact.RetentionDeleteClaimed, ClaimOwner: "cleaner", Version: 2},
	}}
	reconciler := artifact.RetentionReconciler{Store: store, Objects: objects, Owner: "cleaner", OrphanGrace: time.Hour, Now: func() time.Time { return now }}
	handled, err := reconciler.RunOnce(context.Background())
	if err != nil || handled != 2 || len(store.finished) != 2 {
		t.Fatalf("handled=%d finished=%d err=%v", handled, len(store.finished), err)
	}
	if _, err := objects.GetObject(context.Background(), object.TenantID, object.ObjectKey); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("object remains err=%v", err)
	}
	if !store.orphanBefore.Equal(now.Add(-time.Hour)) {
		t.Fatalf("orphan_before=%v", store.orphanBefore)
	}
}

func TestRetentionReconcilerDefersAndQuarantines(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	value := artifact.RetainedArtifact{TenantID: "tenant-a", ArtifactID: "object", Backend: artifact.RetentionBackendObject,
		ObjectKey: "key", ContentDigest: objectstoreDigestPlaceholder, State: artifact.RetentionDeleteClaimed,
		ClaimOwner: "cleaner", Version: 2}
	store := &retentionStoreStub{claimed: []artifact.RetainedArtifact{value}}
	reconciler := artifact.RetentionReconciler{Store: store, Objects: failingObjectStore{}, Owner: "cleaner", OrphanGrace: time.Hour,
		Now: func() time.Time { return now }, RetryBackoff: 5 * time.Minute}
	if handled, err := reconciler.RunOnce(context.Background()); handled != 0 || !errors.Is(err, runtime.ErrBackendUnavailable) ||
		len(store.deferred) != 1 || !store.deferred[0].Equal(now.Add(5*time.Minute)) {
		t.Fatalf("handled=%d deferred=%v err=%v", handled, store.deferred, err)
	}
	value.DeleteAttempt = 7
	store = &retentionStoreStub{claimed: []artifact.RetainedArtifact{value}}
	reconciler.Store = store
	if handled, err := reconciler.RunOnce(context.Background()); handled != 1 || !errors.Is(err, runtime.ErrBackendUnavailable) || len(store.quarantined) != 1 {
		t.Fatalf("handled=%d quarantined=%v err=%v", handled, store.quarantined, err)
	}
}

const objectstoreDigestPlaceholder = "0000000000000000000000000000000000000000000000000000000000000000"

type retentionStoreStub struct {
	claimed      []artifact.RetainedArtifact
	orphanBefore time.Time
	finished     []artifact.RetainedArtifact
	deferred     []time.Time
	quarantined  []time.Time
}

func (s *retentionStoreStub) ClaimExpiredArtifacts(_ context.Context, _ time.Time, orphanBefore time.Time, _ string, _ time.Duration, _ int) ([]artifact.RetainedArtifact, error) {
	s.orphanBefore = orphanBefore
	return append([]artifact.RetainedArtifact(nil), s.claimed...), nil
}
func (s *retentionStoreStub) FinishArtifactDeletion(_ context.Context, value artifact.RetainedArtifact) error {
	s.finished = append(s.finished, value)
	return nil
}
func (s *retentionStoreStub) DeferArtifactDeletion(_ context.Context, _ artifact.RetainedArtifact, until time.Time, _ string) error {
	s.deferred = append(s.deferred, until)
	return nil
}
func (s *retentionStoreStub) QuarantineArtifactDeletion(_ context.Context, _ artifact.RetainedArtifact, _ string, at time.Time) error {
	s.quarantined = append(s.quarantined, at)
	return nil
}
