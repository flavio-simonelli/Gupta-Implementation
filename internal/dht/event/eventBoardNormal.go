package event

import "time"

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
