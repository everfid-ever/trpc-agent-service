package governance

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestCanonicalArgumentsStableAndRejectsDuplicateKeys(t *testing.T) {
	a, digestA, err := CanonicalArguments([]byte(`{"b":2,"a":{"z":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, digestB, err := CanonicalArguments([]byte(` { "a" : {"z":1}, "b":2 } `))
	if err != nil || !bytes.Equal(a, b) || digestA != digestB {
		t.Fatalf("a=%s b=%s digests=%q/%q err=%v", a, b, digestA, digestB, err)
	}
	if _, _, err := CanonicalArguments([]byte(`{"a":1,"a":2}`)); !errors.Is(err, runtime.ErrInvalidEnvelope) {
		t.Fatalf("duplicate key error=%v", err)
	}
}

type expiryCoordinator struct {
	ConfirmationCoordinator
	now   time.Time
	limit int
}

func (c *expiryCoordinator) ExpireDue(_ context.Context, now time.Time, limit int) ([]Confirmation, error) {
	c.now, c.limit = now, limit
	return []Confirmation{{State: ConfirmationExpired}}, nil
}

func TestConfirmationExpiryReconcilerUsesBoundedAuthorityBatch(t *testing.T) {
	now := time.Date(2026, 9, 7, 1, 2, 3, 0, time.FixedZone("test", 8*60*60))
	coordinator := &expiryCoordinator{}
	count, err := (ConfirmationExpiryReconciler{Coordinator: coordinator, Now: func() time.Time { return now }, BatchSize: 7}).RunOnce(context.Background())
	if err != nil || count != 1 || coordinator.limit != 7 || !coordinator.now.Equal(now.UTC()) || coordinator.now.Location() != time.UTC {
		t.Fatalf("count=%d now=%s limit=%d err=%v", count, coordinator.now, coordinator.limit, err)
	}
}

func TestDangerousToolAsksOnlyWithDurableConfirmationCapability(t *testing.T) {
	policy := PolicySnapshot{TenantID: "tenant", Version: 1, Policy: PolicyV1{Tools: []ToolRule{{ToolID: "danger", Version: 1, Dangerous: true}}}}
	decision := ToolDecision(policy, VersionedRef{ID: "danger", Version: 1})
	if decision.Action != ActionDeny {
		t.Fatalf("unsupported dangerous action=%s", decision.Action)
	}
	policy.Policy.Tools[0].ConfirmationSupported = true
	decision = ToolDecision(policy, VersionedRef{ID: "danger", Version: 1})
	if decision.Action != ActionAsk || decision.ReasonCode != ReasonConfirmationRequired {
		t.Fatalf("decision=%#v", decision)
	}
}
