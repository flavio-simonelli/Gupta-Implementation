package dht

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
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

// EventEntry represents an entry in the eventboard (is necessary only few fields of TransportEntry)
type EventEntry struct {
	ID          ID     // Unique identifier for the node
	Address     string // Address of the node in the DHT network (ip:port) (used only for JOIN event)
	IsSuperNode bool   // True if this node is a supernode (used only for JOIN event)
}

// The Event represents an event in the DHT network.
type Event struct {
	eventType EventType  // Type of the event
	target    EventEntry // Target node for the event
}

// EventBoard is a struct that holds a list of events to be sent to the slice leader
type EventBoard struct {
	events []*Event   // Slice of events
	mu     sync.Mutex // Mutex to protect access to the events slice
}

// NormalBoard is a struct that holds a list of events for a normal node
type NormalBoard struct {
	Board      *EventBoard        // Events from the normal node
	isRunning  atomic.Bool        // Flag to indicate if the goroutine that prova a a send the events is running
	cancel     context.CancelFunc // Channel to cancel the goroutine that sends events
	retryDelay time.Duration      // Delay before retrying to send events
}

// SliceLeaderBoard is a struct that holds a list of events for a slice leader
type SliceLeaderBoard struct {
	SliceLeaderBoard []*EventBoard // local eveents form the slice to send to other slice leaders (the slice have max K entry for K slice leader)
	UnitLeaderBoard  []*EventBoard // other events from the other slice leaders and unit leaders to sand to all unit leaders (the slice have max U entry for U unit leaders)
	T_big            time.Duration // Time to wait before sending the events to the slice leaders
	T_wait           time.Duration // Time to wait before retrying to send the events to the unit leaders
}

// ----- Initialization of Event Boards Parameters -----
// NewEventBoard creates a new EventBoard instance.
func NewEventBoard() *EventBoard {
	return &EventBoard{
		events: make([]*Event, 0),
	}
}

// NewNormalBoard creates a new EventBoard instance.
func NewNormalBoard(retryDelay time.Duration) *NormalBoard {
	nb := &NormalBoard{
		Board:      NewEventBoard(),
		cancel:     nil,
		retryDelay: retryDelay,
	}
	nb.isRunning.Store(false)
	return nb
}

// NewSliceLeaderBoard creates a new SliceLeaderBoard instance.
func NewSliceLeaderBoard(tBig time.Duration, tWait time.Duration, K, U, k int) *SliceLeaderBoard {
	sliceBoards := make([]*EventBoard, K)
	for i := range sliceBoards {
		if i == k {
			// is me
			sliceBoards[i] = nil
		}
		sliceBoards[i] = NewEventBoard()
	}
	unitBoards := make([]*EventBoard, U)
	for i := range unitBoards {
		unitBoards[i] = NewEventBoard()
	}
	return &SliceLeaderBoard{
		SliceLeaderBoard: sliceBoards,
		UnitLeaderBoard:  unitBoards,
		T_big:            tBig,
		T_wait:           tWait,
	}
}

// ----- Basic Operations for Event -----

// NewEvent creates a new event with the specified type and target node.
func NewEvent(tagertId ID, targetaddr string, sn bool, event EventType) Event {
	target := EventEntry{
		ID:          tagertId,
		Address:     targetaddr,
		IsSuperNode: sn,
	}
	return Event{
		eventType: event,
		target:    target,
	}
}

// ----- Operationd for SliceLeaderBoard -----

//AddSliceLeaderBoard adds a new EventBoard to the SliceLeaderBoard.

// quindi abbiamo creato queste due board in maiera tale che il nodo normale abbia un backup per archiavre tutti gli eventi che dovrebbero essere inviati allo slice leader se lo slice leader non funziona. Si ha la possibilità di iattivaere una goroutine che si può spegnere con il campo cancel.
// L'altro invece si ha una board per inviare i dati a ciascun nodo che devo contattare. considerando che c'è sempre una goroutin eattiva che effettua l'invio dei messaggi, il server handle degli arrivi dei messaggi semplicemente li mette nle buffer corretto. Poi la goroutine è quella che elimina i messaggi se inviati correttamente a quel determinato nod corrispondente.
