// Package inmemory is a local SecretProvider that always returns defensive
// copies and exact versions.
package inmemory

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type Provider struct {
	mu     sync.RWMutex
	values map[secrets.SecretRef][]byte
}

func New() *Provider { return &Provider{values: make(map[secrets.SecretRef][]byte)} }
func (p *Provider) Put(ref secrets.SecretRef, value []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.values[ref] = append([]byte(nil), value...)
}
func (p *Provider) Resolve(ctx context.Context, ref secrets.SecretRef) (secrets.SecretValue, error) {
	if err := ctx.Err(); err != nil {
		return secrets.SecretValue{}, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	value, ok := p.values[ref]
	if !ok {
		return secrets.SecretValue{}, runtime.ErrNotFound
	}
	return secrets.SecretValue{Bytes: append([]byte(nil), value...), Version: ref.Version}, nil
}
