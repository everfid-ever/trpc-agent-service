// Package broker defines at-least-once execution delivery.
package broker

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Shard uint32
type ConsumerOptions struct {
	ConsumerID string
	Shards     []Shard
}
type ReclaimOptions struct {
	ConsumerID string
	Limit      int
}
type Delivery struct {
	ID       string
	Shard    Shard
	Envelope runtime.ExecutionEnvelope
}

type Broker interface {
	Publish(context.Context, Shard, runtime.ExecutionEnvelope) error
	Consume(context.Context, ConsumerOptions, func(context.Context, Delivery) error) error
	Ack(context.Context, Delivery) error
	Reclaim(context.Context, ReclaimOptions) ([]Delivery, error)
}
