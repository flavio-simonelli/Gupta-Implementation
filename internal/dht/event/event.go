package event

import (
	"GuptaDHT/internal/dht/id"
	"errors"
	"sync"
)

// EventType represents the type of event in the DHT network
type EventType int

// Possible event types
const (
	JOIN EventType = iota
	LEAVE
	BECOME_UNITLEADER
	BECOME_SLICELEADER
)

var ErrInvalidSliceID = errors.New("invalid SliceID")

// EventEntry represents an entry in the eventboard (is necessary only few fields of TransportEntry)
type NodeDescriptor struct {
	ID          id.ID  // Unique identifier for the node
	Address     string // Address of the node in the DHT network (ip:port) (used only for JOIN event)
	IsSuperNode bool   // True if this node is a supernode (used only for JOIN event)
}

// The Event represents an event in the DHT network.
type Event struct {
	eventType EventType      // Type of the event
	target    NodeDescriptor // Target node for the event
}

// EventBoard is a struct that holds a list of events to be sent to the slice leader
type EventBoard struct {
	queue []*Event   // Slice of events
	mu    sync.Mutex // Mutex to protect access to the events slice
}

// ----- Initialization of Event Boards Parameters -----

// NewEventBoard creates a new EventBoard instance.
func NewEventBoard() *EventBoard {
	return &EventBoard{
		queue: make([]*Event, 0),
	}
}

// NewEvent creates a new Event instance with the specified type and target node.
func NewEvent(targetID id.ID, addressID string, sntarget bool, eventype EventType) *Event {
	return &Event{
		eventType: eventype,
		target: NodeDescriptor{
			ID:          targetID,
			Address:     addressID,
			IsSuperNode: sntarget,
		},
	}
}

// ----- Basic Operations for Event -----

// AddEvent adds an event to the queue of the EventBoard.
func (eb *EventBoard) AddEvent(e *Event) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.queue = append(eb.queue, e)
}

// Peek returns the first event in the queue without removing it.
func (eb *EventBoard) Peek() *Event {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if len(eb.queue) == 0 {
		return nil
	}
	return eb.queue[0]
}

// PeekN returns the first n events in the queue without removing them.
func (eb *EventBoard) PeekN(n int) []*Event {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if n <= 0 || len(eb.queue) == 0 {
		return nil
	}

	count := n
	if len(eb.queue) < n {
		count = len(eb.queue)
	}

	peeked := make([]*Event, count)
	copy(peeked, eb.queue[:count])
	return peeked
}

// Commit commits the first n (parameters) event in the queue, removing it from the queue.
func (eb *EventBoard) Commit(n int) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if n <= 0 || len(eb.queue) == 0 {
		return
	}

	if n >= len(eb.queue) {
		eb.queue = nil
	} else {
		eb.queue = eb.queue[n:]
	}
}

// Pop removes and returns the first event in the queue.
func (eb *EventBoard) Pop() *Event {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if len(eb.queue) == 0 {
		return nil
	}
	event := eb.queue[0]
	eb.queue = eb.queue[1:]
	return event
}

// IsEmpty checks if the EventBoard is empty.
func (eb *EventBoard) IsEmpty() bool {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	return len(eb.queue) == 0
}

// Clear removes all events from the EventBoard.
func (eb *EventBoard) Clear() {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.queue = nil
}
