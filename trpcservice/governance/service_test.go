package governance_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancememory "github.com/liuzengh/trpc-agent-service/trpcservice/governance/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
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

func TestServiceEmitsLowCardinalityBudgetAndUsageMetrics(t *testing.T) {
	now := time.Now().UTC()
	policy := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow,
		AllowedModels: []governance.VersionedRef{{ID: "model", Version: 1}}, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled,
		Budget: governance.BudgetPolicy{MaxInputTokens: 100, MaxOutputTokens: 100}}
	envelope := runtime.ExecutionEnvelope{TenantID: "tenant", RequestID: "request", UserID: "user", PolicyVersion: 1}

	budgetStore := governancememory.New(0, 1)
	publishTestPolicy(t, budgetStore, now, policy)
	budgetMetrics := &governanceMetrics{Provider: telemetry.Noop()}
	budgetService := governance.Service{Repository: budgetStore, Ledger: budgetStore, Decisions: budgetStore, Telemetry: budgetMetrics, Now: func() time.Time { return now }}
	denied, err := budgetService.Begin(context.Background(), envelope, governance.VersionedRef{ID: "model", Version: 1}, []byte("input"))
	if err != nil || denied.Decision.Action != governance.ActionDeny || denied.Decision.ReasonCode != governance.ReasonBudgetExceeded {
		t.Fatalf("budget decision=%#v err=%v", denied.Decision, err)
	}
	if got := budgetMetrics.value(telemetry.MetricGovernanceBudgetDenied); got != 1 {
		t.Fatalf("budget denial metric=%d", got)
	}

	usageStore := governancememory.New(0, 1_000)
	publishTestPolicy(t, usageStore, now, policy)
	usageMetrics := &governanceMetrics{Provider: telemetry.Noop()}
	usageService := governance.Service{Repository: usageStore, Ledger: usageStore, Decisions: usageStore, Telemetry: usageMetrics, Now: func() time.Time { return now }}
	permit, err := usageService.Begin(context.Background(), envelope, governance.VersionedRef{ID: "model", Version: 1}, []byte("input"))
	if err != nil || permit.Decision.Action != governance.ActionAllow {
		t.Fatalf("usage permit=%#v err=%v", permit, err)
	}
	if _, err := usageService.Finish(context.Background(), permit, governance.Usage{}, []byte("output")); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("missing usage err=%v", err)
	}
	if got := usageMetrics.value(telemetry.MetricGovernanceUsageMissing); got != 1 {
		t.Fatalf("usage missing metric=%d", got)
	}
}

func publishTestPolicy(t *testing.T, store *governancememory.Store, now time.Time, value governance.PolicyV1) {
	t.Helper()
	digest, _, err := governance.PolicyDigest(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PublishPolicy(governance.PolicySnapshot{TenantID: "tenant", Version: 1, SchemaVersion: 1, Policy: value, ContentDigest: digest, PublishedAt: now}); err != nil {
		t.Fatal(err)
	}
}

type governanceMetrics struct {
	telemetry.Provider
	mu     sync.Mutex
	values map[telemetry.MetricDescriptor]int64
}

func (m *governanceMetrics) Counter(descriptor telemetry.MetricDescriptor) telemetry.Counter {
	return governanceCounter{metrics: m, descriptor: descriptor}
}

func (m *governanceMetrics) value(descriptor telemetry.MetricDescriptor) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.values[descriptor]
}

type governanceCounter struct {
	metrics    *governanceMetrics
	descriptor telemetry.MetricDescriptor
}

func (c governanceCounter) Add(_ context.Context, value int64, attributes ...telemetry.Attribute) {
	if len(attributes) != 1 || attributes[0].Key() != "component" || attributes[0].Value() != "worker" {
		panic("governance metric labels must remain fixed and low-cardinality")
	}
	c.metrics.mu.Lock()
	defer c.metrics.mu.Unlock()
	if c.metrics.values == nil {
		c.metrics.values = make(map[telemetry.MetricDescriptor]int64)
	}
	c.metrics.values[c.descriptor] += value
}
