// Package inmemory provides an at-least-once broker for local contract tests.
package inmemory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Broker struct {
	mu           sync.Mutex
	nextID       uint64
	queued       []broker.Delivery
	pending      map[string]broker.Delivery
	pendingSince map[string]time.Time
	reclaimIdle  time.Duration
	wakeup       chan struct{}
}

func New() *Broker {
	return NewWithReclaimIdle(30 * time.Second)
}

func NewWithReclaimIdle(idle time.Duration) *Broker {
	if idle <= 0 {
		idle = 30 * time.Second
	}
	return &Broker{pending: make(map[string]broker.Delivery), pendingSince: make(map[string]time.Time), reclaimIdle: idle, wakeup: make(chan struct{}, 1)}
}

func (b *Broker) Publish(ctx context.Context, shard broker.Shard, envelope runtime.ExecutionEnvelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := envelope.Validate(); err != nil {
		return err
	}
	b.mu.Lock()
	b.nextID++
	delivery := broker.Delivery{ID: fmt.Sprintf("local-%d", b.nextID), Shard: shard, Envelope: envelope}
	b.queued = append(b.queued, delivery)
	b.mu.Unlock()
	select {
	case b.wakeup <- struct{}{}:
	default:
	}
	return nil
}

func (b *Broker) Consume(ctx context.Context, _ broker.ConsumerOptions, handle func(context.Context, broker.Delivery) error) error {
	for {
		delivery, ok := b.take()
		if ok {
			if err := handle(ctx, delivery); err != nil {
				return err
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.wakeup:
		}
	}
}
func (b *Broker) take() (broker.Delivery, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.queued) == 0 {
		return broker.Delivery{}, false
	}
	d := b.queued[0]
	b.queued = b.queued[1:]
	b.pending[d.ID] = d
	b.pendingSince[d.ID] = time.Now()
	return d, true
}
func (b *Broker) Ack(ctx context.Context, d broker.Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.pending, d.ID)
	delete(b.pendingSince, d.ID)
	return nil
}
func (b *Broker) Reclaim(ctx context.Context, opts broker.ReclaimOptions) ([]broker.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	limit := opts.Limit
	if limit <= 0 {
		limit = len(b.pending)
	}
	out := make([]broker.Delivery, 0, limit)
	now := time.Now()
	for id, d := range b.pending {
		if now.Sub(b.pendingSince[id]) < b.reclaimIdle {
			continue
		}
		out = append(out, d)
		b.pendingSince[id] = now
		if len(out) == limit {
			break
		}
	}
	return out, nil
}
