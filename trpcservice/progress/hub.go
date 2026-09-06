// Package progress provides best-effort, non-durable execution updates.
package progress

import (
	"sync"
)

const (
	RunStarted   = "run.started"
	MessageDelta = "message.delta"
)

// Event is a transient execution update. Consumers must obtain the completed
// answer from the durable Result/Outbox chain, never by replaying this feed.
type Event struct {
	SchemaVersion             uint16
	TenantID, RequestID, Kind string
	Sequence                  uint64
	Content, TraceParent      string
}

// Publisher accepts a best-effort update. TryPublish must return immediately:
// a slow subscriber may not backpressure execution or result persistence.
type Publisher interface {
	TryPublish(Event)
}

type PublisherFunc func(Event)

func (f PublisherFunc) TryPublish(event Event) { f(event) }

// Hub fans out local-process progress events through bounded subscriber
// buffers. Overflow drops only the transient update for that subscriber.
type Hub struct {
	mu          sync.Mutex
	nextID      uint64
	subscribers map[uint64]chan Event
}

func NewHub() *Hub { return &Hub{subscribers: make(map[uint64]chan Event)} }

func (h *Hub) TryPublish(event Event) {
	if h == nil || event.SchemaVersion != 1 || event.TenantID == "" || event.RequestID == "" || event.Sequence < 1 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, subscriber := range h.subscribers {
		select {
		case subscriber <- event:
		default:
		}
	}
}

// Subscribe returns a bounded, independently droppable event feed and its
// idempotent cancellation function.
func (h *Hub) Subscribe(buffer int) (<-chan Event, func()) {
	if h == nil {
		return nil, func() {}
	}
	if buffer < 1 {
		buffer = 1
	}
	h.mu.Lock()
	h.nextID++
	id := h.nextID
	stream := make(chan Event, buffer)
	h.subscribers[id] = stream
	h.mu.Unlock()
	var once sync.Once
	return stream, func() {
		once.Do(func() {
			h.mu.Lock()
			if current, ok := h.subscribers[id]; ok {
				delete(h.subscribers, id)
				close(current)
			}
			h.mu.Unlock()
		})
	}
}
