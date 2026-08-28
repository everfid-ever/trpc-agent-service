package gateway

import (
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// ProtocolInvocationResolver authenticates a façade request and resolves its
// server-owned tenant, app, principal, user, session and idempotency scope.
// Implementations may use verified tokens, route bindings and trusted headers;
// they must not trust tenant identity from the protocol payload.
type ProtocolInvocationResolver interface {
	ResolveProtocolInvocation(*http.Request) (ServerInvocationContext, error)
}

// ProtocolInvocationMiddleware is the mandatory boundary around an upstream
// OpenAI, A2A or tRPC-Agent handler configured with GatewayRunnerBridge.
type ProtocolInvocationMiddleware struct {
	Resolver ProtocolInvocationResolver
	Next     http.Handler
}

func (m ProtocolInvocationMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.Resolver == nil || m.Next == nil {
		writeControlError(w, runtime.ErrCapabilityUnsupported)
		return
	}
	trusted, err := m.Resolver.ResolveProtocolInvocation(r)
	if err != nil || trusted.Tenant.Validate() != nil || trusted.PrincipalID == "" ||
		trusted.UserID == "" || trusted.SessionID == "" || trusted.Protocol == "" || trusted.IdempotencyKey == "" {
		writeControlError(w, ErrUnauthenticated)
		return
	}
	m.Next.ServeHTTP(w, r.WithContext(WithServerInvocationContext(r.Context(), trusted)))
}
