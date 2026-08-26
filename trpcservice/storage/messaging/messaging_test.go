package messaging_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

func TestStableReplyIDUsesOnlyLogicalCoordinate(t *testing.T) {
	in := messaging.ReplyCoordinate{TenantID: "tenant", RequestID: "request", InputSeq: 2, Stage: "terminal", Ordinal: 0}
	first, err := messaging.StableReplyID(in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := messaging.StableReplyID(in)
	if err != nil || first != second || len(first) < 4 || first[:3] != "r1_" {
		t.Fatalf("first=%q second=%q err=%v", first, second, err)
	}
	in.Ordinal++
	third, err := messaging.StableReplyID(in)
	if err != nil || third == first {
		t.Fatalf("different coordinate id=%q err=%v", third, err)
	}
}
