package governance_test

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancememory "github.com/liuzengh/trpc-agent-service/trpcservice/governance/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestServicePinsPolicyReservesSettlesAndFailsClosed(t *testing.T) {
	now := time.Now().UTC()
	store := governancememory.New(1_000_000, 1_000)
	pricingValue := governance.PricingV1{SchemaVersion: 1, Currency: "USD", ConversionRef: "fx", ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), Prices: []governance.Price{{Provider: "deepseek", Model: "chat", ModelProfileID: "model", ModelProfileVersion: 1, InputMicrosPerMillion: 1_000_000, OutputMicrosPerMillion: 1_000_000}}}
	pricingDigest, _, _ := governance.PricingDigest(pricingValue)
	if err := store.PublishPricing(governance.PricingSnapshot{TenantID: "tenant", Version: 3, SchemaVersion: 1, Pricing: pricingValue, ContentDigest: pricingDigest, PublishedAt: now}); err != nil {
		t.Fatal(err)
	}
	policyValue := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, AllowedModels: []governance.VersionedRef{{ID: "model", Version: 1}}, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled, Budget: governance.BudgetPolicy{MaxInputTokens: 100, MaxOutputTokens: 100, MaxCostMicrosPerRun: 500}, PricingVersion: 3}
	policyDigest, _, _ := governance.PolicyDigest(policyValue)
	if err := store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 7, SchemaVersion: 1, Policy: policyValue, ContentDigest: policyDigest, PublishedAt: now}); err != nil {
		t.Fatal(err)
	}
	service := governance.Service{Repository: store, Ledger: store, Decisions: store, Now: func() time.Time { return now }}
	envelope := runtime.ExecutionEnvelope{TenantID: "tenant", RequestID: "request", UserID: "user", PolicyVersion: 7}
	newPolicy := policyValue
	newPolicy.DefaultAction = governance.ActionDeny
	newDigest, _, _ := governance.PolicyDigest(newPolicy)
	if err := store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 9, SchemaVersion: 1, Policy: newPolicy, ContentDigest: newDigest, PublishedAt: now}); err != nil {
		t.Fatal(err)
	}
	permit, err := service.Begin(context.Background(), envelope, governance.VersionedRef{ID: "model", Version: 1}, []byte("hello"))
	if err != nil || permit.Decision.Action != governance.ActionAllow || permit.Reservation.State != governance.ReservationReserved {
		t.Fatalf("permit=%#v err=%v", permit, err)
	}
	decision, err := service.Finish(context.Background(), permit, governance.Usage{InputTokens: 10, OutputTokens: 5}, []byte("answer"))
	if err != nil || decision.Action != governance.ActionAllow {
		t.Fatalf("decision=%#v err=%v", decision, err)
	}
	recovered, err := service.Begin(context.Background(), envelope, governance.VersionedRef{ID: "model", Version: 1}, []byte("hello"))
	if err != nil || recovered.Decision.Action != governance.ActionDeny || recovered.Decision.ReasonCode != governance.ReasonReservationClosed {
		t.Fatalf("closed reservation recovery=%#v err=%v", recovered, err)
	}
	strict := policyValue
	strict.InputDLP = governance.DLPRequired
	strictDigest, _, _ := governance.PolicyDigest(strict)
	if err := store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 8, SchemaVersion: 1, Policy: strict, ContentDigest: strictDigest, PublishedAt: now}); err != nil {
		t.Fatal(err)
	}
	envelope.RequestID = "strict"
	envelope.PolicyVersion = 8
	denied, err := service.Begin(context.Background(), envelope, governance.VersionedRef{ID: "model", Version: 1}, []byte("secret"))
	if err != nil || denied.Decision.Action != governance.ActionDeny || denied.Reservation.ReservationID != "" {
		t.Fatalf("strict=%#v err=%v", denied, err)
	}
}
