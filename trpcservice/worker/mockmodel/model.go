// Package mockmodel provides a deterministic model for the P0 local slice.
package mockmodel

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Model struct {
	mu    sync.Mutex
	calls map[string]int
}

func New() *Model { return &Model{calls: make(map[string]int)} }
func (m *Model) Generate(ctx context.Context, e runtime.ExecutionEnvelope, _ profile.ExecutionProfileSnapshot) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := e.TenantID + "\x00" + e.RequestID
	m.calls[key]++
	return "mock-result://" + e.TenantID + "/" + e.RequestID, nil
}
func (m *Model) Calls(tenantID, requestID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[tenantID+"\x00"+requestID]
}
