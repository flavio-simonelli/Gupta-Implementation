package event

import "sync"

// Board is a struct that holds a list of events thread-safely
type Board struct {
	queue []*Event   // Slice of events
	mu    sync.Mutex // Mutex to protect access to the events slice
}

// ----- Initialization of Event Boards Parameters -----

// NewBoard creates a new EventBoard instance.
func NewBoard() *Board {
	return &Board{
		queue: make([]*Event, 0),
	}
}

// ----- Basic Operations for Event -----

// add adds a single event to the queue.
func (b *Board) add(event *Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queue = append(b.queue, event)
}

// addBatch adds multiple events to the queue.
func (b *Board) addBatch(events []*Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queue = append(b.queue, events...)
}

// isEmpty returns true if the queue has no events.
func (b *Board) isEmpty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue) == 0
}

// len returns the number of events in the queue.
func (b *Board) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue)
}

// popAll returns all events and clears the queue.
func (b *Board) popAll() []*Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	events := b.queue
	b.queue = nil
	return events
}

// getAll returns a copy of all events in the queue without clearing it.
func (b *Board) getAll() []*Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	copyQueue := make([]*Event, len(b.queue))
	copy(copyQueue, b.queue)
	return copyQueue
}

// clear removes all events from the queue.
func (b *Board) clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queue = nil
}
