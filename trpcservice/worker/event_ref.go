package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/event"
)

// DurableEventRef derives a content-addressed reference for the event bytes
// that AtomicSessionStore persists in the same CommitTurn transaction.
func DurableEventRef(ctx context.Context, value *event.Event) (string, string, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if value == nil || value.ID == "" {
		return "", "", runtime.ErrInvalidEnvelope
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", "", runtime.ErrInvariantViolation
	}
	digest := sha256.Sum256(payload)
	return "agent.event.v1", "event://sha256/" + hex.EncodeToString(digest[:]), nil
}
