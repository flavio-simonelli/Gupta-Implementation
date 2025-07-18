package event

import (
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/dht/routingtable"
	"context"
	"errors"
	"sync"
	"time"
)

// interface client for send events
type EventSender interface {
	// SendEventsToSliceLeader sends events to the slice leader.
	SendToSliceLeader(events []*Event, address string) error
	// SendEventsToUnitLeader sends events to the unit leader.
	SendToUnitLeader(events []*Event, address string) error
	// SendEventsToNormalNode sends events to a normal node.
	SendToNormalNode(events []*Event, address string) error
}

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

// ----- Dispacher for Event in normal node -----

// EventDispatcher is a struct that handles the dispatching of events to the slice leader.
type EventDispatcher struct {
	sliceLeaderAddr func() string // Address of the slice leader,
	eventBoard      *EventBoard   // buffer FIFO
	retryInterval   time.Duration
	client          EventSender // interfaccia astratta per l'invio (mockabile nei test)
}

// NewEventDispatcher creates a new EventDispatcher instance.
func NewEventDispatcher(client EventSender, retryInterval time.Duration) *EventDispatcher {
	return &EventDispatcher{
		eventBoard:    NewEventBoard(),
		retryInterval: retryInterval,
		client:        client,
	}
}

// SendOrEnqueueEvent sends an event to the slice leader or enqueues it if the send fails.
func (ed *EventDispatcher) SendOrEnqueue(ev *Event) {
	if !ed.eventBoard.IsEmpty() {
		// Qualcosa è in coda → manteniamo l’ordine
		ed.eventBoard.AddEvent(ev)
		return
	}

	// Prova invio immediato
	err := ed.client.SendToSliceLeader([]*Event{ev}, ed.sliceLeaderAddr())
	if err != nil {
		ed.eventBoard.AddEvent(ev)
	}
}

func (ed *EventDispatcher) StartRetryLoop(ctx context.Context, batchSize int) {
	ticker := time.NewTicker(ed.retryInterval)

	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return

			case <-ticker.C:
				ed.retryBatch(batchSize)
			}
		}
	}()
}

func (ed *EventDispatcher) retryBatch(n int) {
	events := ed.eventBoard.PeekN(n)
	if len(events) == 0 {
		return
	}

	err := ed.client.SendToSliceLeader(events, ed.sliceLeaderAddr())
	if err == nil {
		ed.eventBoard.Commit(len(events))
	}
}

// ----- SliceLeader EventDispatcher -----

type SliceLeaderDispatcher struct {
	// Buffer per ogni slice, cioè per ogni unità (indice = sliceID)
	sliceBuffers []*EventBoard
	// Buffer per ogni unità (unit leader)
	unitBuffers []*EventBoard
	// routingTable per risolvere leader attuale di slice e unità
	table  *routingtable.Table
	client EventSender
	tbig   time.Duration
	twait  time.Duration
}

// NewSliceLeaderDispatcher creates a new SliceLeaderDispatcher instance.
func NewSliceLeaderDispatcher(client EventSender, table *routingtable.Table, tbig, twait time.Duration) *SliceLeaderDispatcher {
	sliceBuffers := make([]*EventBoard, id.GetK())
	for i := range sliceBuffers {
		sliceBuffers[i] = NewEventBoard()
	}

	unitBuffers := make([]*EventBoard, id.GetU())
	for i := range unitBuffers {
		unitBuffers[i] = NewEventBoard()
	}

	return &SliceLeaderDispatcher{
		sliceBuffers: sliceBuffers,
		unitBuffers:  unitBuffers,
		table:        table,
		client:       client,
		tbig:         tbig,
		twait:        twait,
	}
}

// AddEvent adds an event on the all buffer tranne il buffer della slice leader che lo ha inviato mettere sliceID < 0 se ad inviare un  evento non è uno slice leader
func (sld *SliceLeaderDispatcher) AddEvent(ev []*Event, sliceID int) error {
	// Validazione semplice
	if ev == nil || len(ev) == 0 {
		return errors.New("no events to add")
	}
	// Aggiungi eventi a tutti i unit leader buffer
	for _, board := range sld.unitBuffers {
		for _, e := range ev {
			board.AddEvent(e)
		}
	}
	// Aggiungi eventi a tutti gli altri slice leader buffer (tranne mittente se noto)
	for idx, board := range sld.sliceBuffers {
		if sliceID >= 0 && idx == sliceID {
			continue // skip sender
		}
		for _, e := range ev {
			board.AddEvent(e)
		}
	}
	return nil
}

/*
func (sld *SliceLeaderDispatcher) StartTbigLoop(ctx context.Context, batchSize int) {
	ticker := time.NewTicker(sld.tbig)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				for sliceID, buffer := range sld.sliceBuffers {
					addr := sld.table.(sliceID)
					events := buffer.PeekN(batchSize)

					if len(events) == 0 {
						continue
					}

					err := sld.client.SendToSliceLeader(events, addr)
					if err == nil {
						buffer.Commit(len(events))
					} else {
						// log.Printf("Failed to send to slice %d: %v", sliceID, err)
					}
				}
			}
		}
	}()
}

func (sld *SliceLeaderDispatcher) StartTwaitLoop(ctx context.Context, batchSize int) {
	ticker := time.NewTicker(sld.twait)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				for unitID, buffer := range sld.unitBuffers {
					addr := sld.table.GetUnitLeaderAddress(unitID)
					events := buffer.PeekN(batchSize)

					if len(events) == 0 {
						continue
					}

					err := sld.client.SendToUnitLeader(events, addr)
					if err == nil {
						buffer.Commit(len(events))
					} else {
						// log.Printf("Failed to send to unit %d: %v", unitID, err)
					}
				}
			}
		}
	}()
}
*/
