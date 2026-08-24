// Package redis implements the Redis Streams execution broker.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
)

type Config struct {
	Environment string
	Group       string
	ShardCount  uint32
	ReadBlock   time.Duration
	ReclaimIdle time.Duration
}

type Broker struct {
	client redisclient.UniversalClient
	config Config
}

func New(client redisclient.UniversalClient, config Config) (*Broker, error) {
	if client == nil || !validSegment(config.Environment) || !validSegment(config.Group) || config.ShardCount == 0 {
		return nil, runtime.ErrInvalidEnvelope
	}
	if config.ReadBlock <= 0 {
		config.ReadBlock = 250 * time.Millisecond
	}
	if config.ReclaimIdle <= 0 {
		config.ReclaimIdle = 30 * time.Second
	}
	return &Broker{client: client, config: config}, nil
}

func (b *Broker) Publish(ctx context.Context, shard broker.Shard, envelope runtime.ExecutionEnvelope) error {
	if uint32(shard) >= b.config.ShardCount {
		return runtime.ErrInvalidEnvelope
	}
	payload, err := runtime.MarshalEnvelope(envelope)
	if err != nil {
		return err
	}
	return b.client.XAdd(ctx, &redisclient.XAddArgs{
		Stream: b.stream(shard),
		Values: map[string]any{
			"schema_version": strconv.FormatUint(uint64(envelope.SchemaVersion), 10),
			"envelope":       payload,
			"request_id":     envelope.RequestID,
			"session_hash":   sessionHash(envelope),
			"traceparent":    envelope.TraceParent,
			"published_at":   time.Now().UTC().Format(time.RFC3339Nano),
		},
	}).Err()
}

func (b *Broker) Consume(ctx context.Context, opts broker.ConsumerOptions, handle func(context.Context, broker.Delivery) error) error {
	if opts.ConsumerID == "" || handle == nil || len(opts.Shards) == 0 {
		return runtime.ErrInvalidEnvelope
	}
	streams := make([]string, 0, len(opts.Shards)*2)
	for _, shard := range opts.Shards {
		if uint32(shard) >= b.config.ShardCount {
			return runtime.ErrInvalidEnvelope
		}
		stream := b.stream(shard)
		if err := b.ensureGroup(ctx, stream); err != nil {
			return err
		}
		streams = append(streams, stream)
	}
	for range opts.Shards {
		streams = append(streams, ">")
	}
	for {
		result, err := b.client.XReadGroup(ctx, &redisclient.XReadGroupArgs{
			Group: b.config.Group, Consumer: opts.ConsumerID, Streams: streams,
			Count: 1, Block: b.config.ReadBlock,
		}).Result()
		if errors.Is(err, redisclient.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		for _, stream := range result {
			shard, err := b.parseStream(stream.Stream)
			if err != nil {
				return err
			}
			for _, message := range stream.Messages {
				delivery, err := decodeDelivery(shard, message)
				if err != nil {
					return err
				}
				if err := handle(ctx, delivery); err != nil {
					return err
				}
			}
		}
	}
}

func (b *Broker) Ack(ctx context.Context, delivery broker.Delivery) error {
	if delivery.ID == "" || uint32(delivery.Shard) >= b.config.ShardCount {
		return runtime.ErrInvalidEnvelope
	}
	return b.client.XAck(ctx, b.stream(delivery.Shard), b.config.Group, delivery.ID).Err()
}

func (b *Broker) Reclaim(ctx context.Context, opts broker.ReclaimOptions) ([]broker.Delivery, error) {
	if opts.ConsumerID == "" {
		return nil, runtime.ErrInvalidEnvelope
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	result := make([]broker.Delivery, 0, limit)
	for shard := uint32(0); shard < b.config.ShardCount && len(result) < limit; shard++ {
		if err := b.ensureGroup(ctx, b.stream(broker.Shard(shard))); err != nil {
			return nil, err
		}
		messages, _, err := b.client.XAutoClaim(ctx, &redisclient.XAutoClaimArgs{
			Stream: b.stream(broker.Shard(shard)), Group: b.config.Group,
			Consumer: opts.ConsumerID, MinIdle: b.config.ReclaimIdle, Start: "0-0",
			Count: int64(limit - len(result)),
		}).Result()
		if errors.Is(err, redisclient.Nil) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, message := range messages {
			delivery, err := decodeDelivery(broker.Shard(shard), message)
			if err != nil {
				return nil, err
			}
			result = append(result, delivery)
		}
	}
	return result, nil
}

func (b *Broker) ensureGroup(ctx context.Context, stream string) error {
	err := b.client.XGroupCreateMkStream(ctx, stream, b.config.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (b *Broker) stream(shard broker.Shard) string {
	return fmt.Sprintf("trpc:{%s}:%d:dispatch", b.config.Environment, shard)
}

func (b *Broker) parseStream(stream string) (broker.Shard, error) {
	prefix := fmt.Sprintf("trpc:{%s}:", b.config.Environment)
	if !strings.HasPrefix(stream, prefix) || !strings.HasSuffix(stream, ":dispatch") {
		return 0, runtime.ErrInvalidEnvelope
	}
	value := strings.TrimSuffix(strings.TrimPrefix(stream, prefix), ":dispatch")
	shard, err := strconv.ParseUint(value, 10, 32)
	if err != nil || shard >= uint64(b.config.ShardCount) {
		return 0, runtime.ErrInvalidEnvelope
	}
	return broker.Shard(shard), nil
}

func decodeDelivery(shard broker.Shard, message redisclient.XMessage) (broker.Delivery, error) {
	payload, ok := valueString(message.Values["envelope"])
	if !ok {
		return broker.Delivery{}, runtime.ErrInvalidEnvelope
	}
	envelope, err := runtime.UnmarshalEnvelope([]byte(payload))
	if err != nil {
		return broker.Delivery{}, err
	}
	requestID, ok := valueString(message.Values["request_id"])
	if !ok || requestID != envelope.RequestID {
		return broker.Delivery{}, runtime.ErrVersionMismatch
	}
	return broker.Delivery{ID: message.ID, Shard: shard, Envelope: envelope}, nil
}

func valueString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}

func sessionHash(envelope runtime.ExecutionEnvelope) string {
	digest := sha256.Sum256([]byte(envelope.TenantID + "\x00" + envelope.AgentAppID + "\x00" + envelope.SessionID))
	return hex.EncodeToString(digest[:])
}

func validSegment(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

var _ broker.Broker = (*Broker)(nil)
