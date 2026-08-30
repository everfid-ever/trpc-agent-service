package worker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/event"
)

func TestDurableEventRefIsStableAndRejectsIncompleteEvents(t *testing.T) {
	value := &event.Event{ID: "event-1", RequestID: "request-1", Author: "assistant"}
	kind, first, err := DurableEventRef(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := DurableEventRef(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	if kind != "agent.event.v1" || first != second || !strings.HasPrefix(first, "event://sha256/") {
		t.Fatalf("kind=%q first=%q second=%q", kind, first, second)
	}
	if _, _, err := DurableEventRef(context.Background(), &event.Event{}); !errors.Is(err, runtime.ErrInvalidEnvelope) {
		t.Fatalf("incomplete: %v", err)
	}
}
