// Package inmemory stores audit events for local contract tests.
package inmemory

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

type Sink struct {
	mu     sync.RWMutex
	events map[string]audit.Event
}

func New() *Sink { return &Sink{events: make(map[string]audit.Event)} }
func (s *Sink) Emit(ctx context.Context, event audit.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[event.TenantID+"\x00"+event.AuditID] = clone(event)
	return nil
}
func (s *Sink) Events(tenantID string) []audit.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]audit.Event, 0)
	for _, event := range s.events {
		if event.TenantID == tenantID {
			out = append(out, clone(event))
		}
	}
	return out
}
func clone(in audit.Event) audit.Event {
	b, _ := json.Marshal(in)
	var out audit.Event
	_ = json.Unmarshal(b, &out)
	return out
}
