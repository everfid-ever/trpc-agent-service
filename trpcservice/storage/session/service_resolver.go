package session

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
)

// ServiceResolver resolves the official trpc-agent-go Session service for an
// exact, immutable execution profile. Implementations must not consult a
// mutable tenant "current" pointer: the ExecutionEnvelope's ConfigVersion is
// the routing authority for the lifetime of a turn.
type ServiceResolver interface {
	Resolve(context.Context, profile.ExecutionProfileSnapshot) (agentsession.Service, error)
}
