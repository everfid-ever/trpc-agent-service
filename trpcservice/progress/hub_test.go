package progress

import "testing"

func TestHubDropsOnlySlowSubscriberUpdates(t *testing.T) {
	hub := NewHub()
	slow, cancelSlow := hub.Subscribe(1)
	defer cancelSlow()
	fast, cancelFast := hub.Subscribe(2)
	defer cancelFast()
	hub.TryPublish(Event{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", Sequence: 1, Kind: RunStarted})
	<-fast
	hub.TryPublish(Event{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", Sequence: 2, Kind: MessageDelta, Content: "a"})
	if got := <-fast; got.Sequence != 2 {
		t.Fatalf("fast=%#v", got)
	}
	if got := <-slow; got.Sequence != 1 {
		t.Fatalf("slow first=%#v", got)
	}
	select {
	case unexpected := <-slow:
		t.Fatalf("slow received overflow=%#v", unexpected)
	default:
	}
}
