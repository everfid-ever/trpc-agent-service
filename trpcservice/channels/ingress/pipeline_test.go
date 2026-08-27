package ingress_test

import (
	"context"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	preprocessmemory "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/inmemory"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

func TestPipelinePersistsNormalizedPayloadAndSchedulesExactlyOnce(t *testing.T) {
	store := preprocessmemory.New()
	payloads := messagingmemory.New()
	verified := ingress.VerifiedIngress{Binding: channel.VerifiedBinding{TenantID: "tenant", TenantVersion: 2, AgentAppID: "app",
		ChannelBindingID: "binding", Channel: "fake", ExternalAccountID: "account", BindingVersion: 1},
		Events: []channel.ProviderEvent{{SchemaVersion: 1, Channel: "fake", ExternalAccountID: "account", ExternalMessageID: "message",
			ExternalUserID: "external-user", ConversationType: "dm", MessageType: "text", Text: "hello", OccurredAt: time.Now(), TraceParent: "trace"}}}
	pipeline := ingress.Pipeline{Verification: staticVerifier{verified}, Identity: staticIdentity{}, Intake: store, Payloads: payloads}
	first, err := pipeline.Accept(context.Background(), channel.CallbackRequest{})
	if err != nil || len(first) != 1 {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	second, err := pipeline.Accept(context.Background(), channel.CallbackRequest{})
	if err != nil || len(second) != 1 || first[0] != second[0] {
		t.Fatalf("second=%#v err=%v", second, err)
	}
	payload, err := payloads.GetPayload(context.Background(), "tenant", first[0].RequestID)
	if err != nil || string(payload.Content) != `{"external_message_id":"message","external_user_id":"external-user","external_chat_id":"","text":"hello"}` {
		t.Fatalf("payload=%s err=%v", payload.Content, err)
	}
}

type staticVerifier struct{ ingress.VerifiedIngress }

func (v staticVerifier) VerifyAndDecode(context.Context, channel.CallbackRequest) (ingress.VerifiedIngress, error) {
	return v.VerifiedIngress, nil
}

type staticIdentity struct{}

func (staticIdentity) Map(context.Context, channel.VerifiedBinding, channel.ProviderEvent) (identity.Result, error) {
	return identity.Result{UserID: "user", SessionID: "session"}, nil
}
