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

type actionCoordinator struct {
	ConfirmationCoordinator
	value    Confirmation
	decision ConfirmationDecision
}

func (c *actionCoordinator) GetConfirmation(context.Context, string, string) (Confirmation, error) {
	return c.value, nil
}
func (c *actionCoordinator) Decide(_ context.Context, in ConfirmationDecision) (Confirmation, error) {
	c.decision = in
	c.value.State = ConfirmationApproved
	return c.value, nil
}

func TestConfirmationActionServiceBindsVerifiedActorAndSession(t *testing.T) {
	now := time.Now().UTC()
	coordinator := &actionCoordinator{value: Confirmation{SuspensionRequest: SuspensionRequest{ConfirmationID: "confirmation", TenantID: "tenant",
		SubjectID: "subject", ChannelBindingID: "binding", SessionID: "session"}, State: ConfirmationPending, Version: 1}}
	service := ConfirmationActionService{Coordinator: coordinator}
	action := ConfirmationAction{TenantID: "tenant", ConfirmationID: "confirmation", SubjectID: "subject", ChannelBindingID: "binding",
		SessionID: "session", Approve: true, ExpectedVersion: 1, DecidedAt: now}
	if _, err := service.DecideAction(context.Background(), action); err != nil {
		t.Fatal(err)
	}
	if !coordinator.decision.Approve || coordinator.decision.SubjectID != "subject" {
		t.Fatalf("decision=%#v", coordinator.decision)
	}
	action.SessionID = "forged"
	if _, err := service.DecideAction(context.Background(), action); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("forged session=%v", err)
	}
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
