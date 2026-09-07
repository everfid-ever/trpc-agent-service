package postgres

import (
	"context"
	"sync"
	"testing"

	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type histogramRecord struct {
	value      float64
	attributes map[string]string
}

type recordingHistogram struct {
	mu      sync.Mutex
	records []histogramRecord
}

func (h *recordingHistogram) Record(_ context.Context, value float64, attributes ...telemetry.Attribute) {
	record := histogramRecord{value: value, attributes: make(map[string]string, len(attributes))}
	for _, attribute := range attributes {
		record.attributes[attribute.Key()] = attribute.Value()
	}
	h.mu.Lock()
	h.records = append(h.records, record)
	h.mu.Unlock()
}

type recordingProvider struct {
	telemetry.Provider
	histogram *recordingHistogram
}

func (p recordingProvider) Histogram(descriptor telemetry.MetricDescriptor) telemetry.Histogram {
	if descriptor == telemetry.MetricSessionBackendDuration {
		return p.histogram
	}
	return p.Provider.Histogram(descriptor)
}

func TestOpenForRunRecordsFixedBackendLatencyLabels(t *testing.T) {
	histogram := &recordingHistogram{}
	store := NewWithTelemetry(nil, recordingProvider{Provider: telemetry.Noop(), histogram: histogram})
	if _, err := store.OpenForRun(context.Background(), sessionstore.OpenForRunRequest{}); err == nil {
		t.Fatal("invalid request unexpectedly succeeded")
	}
	histogram.mu.Lock()
	defer histogram.mu.Unlock()
	if len(histogram.records) != 1 {
		t.Fatalf("records=%d", len(histogram.records))
	}
	record := histogram.records[0]
	if record.value < 0 || record.attributes["destination"] != "postgres" || record.attributes["operation"] != "session.open" || record.attributes["outcome"] != "error" {
		t.Fatalf("record=%#v", record)
	}
	if _, exists := record.attributes["tenant_id"]; exists {
		t.Fatalf("tenant label must not be emitted: %#v", record.attributes)
	}
}
