package event

import (
	"GuptaDHT/internal/dht/routingtable"
	"time"
)

type EventBoard struct {
	client      EventSender         // interface for the event sender
	table       *routingtable.Table // routing table of the node for taking the slice leader address and unit leader address
	Normal      *NormalBoard
	SliceLeader *SliceLeaderBoard
}

// NewEventBoard creates a new EventBoard instance.
func NewEventBoard(client EventSender, table *routingtable.Table, retryInterval, tBig, tWait time.Duration) *EventBoard {
	return &EventBoard{
		client:      client,
		table:       table,
		Normal:      newNormalBoard(retryInterval),
		SliceLeader: newSliceLeaderBoard(tBig, tWait),
	}
}

// InitializeSliceLeaderBoard initializes the SliceLeaderBoard with the given parameters.
func (eb *EventBoard) InitializeSliceLeaderBoard() {
	eb.SliceLeader.initializeSliceLeaderBoard(eb.client, eb.table)
}

// RemoveSliceLeaderBoard removes the SliceLeaderBoard.
func (eb *EventBoard) RemoveSliceLeaderBoard() {
	eb.SliceLeader.stop()
}

// AddEvent adds an event to the appropriate board based on the node type.
func (eb *EventBoard) AddEvent(event []*Event) error {
	err := eb.SliceLeader.addEvents(event)
	if err != nil {
		return err
	}
	return nil
}

// AddExternalEvents adds an event to the slice leader board for external events (e.g., from other slices).
func (eb *EventBoard) AddExternalEvents(ev []*Event, sliceID int) error {
	return eb.SliceLeader.addExternalEvents(ev, sliceID)
}

// SendFlowEvents sends the flow events
func (eb *EventBoard) SendFlowEvents(events []*Event, receiver string) error {
	if len(events) == 0 {
		return nil
	}
	// Send the events to the slice leader
	return eb.client.SendToNormalNode(events, eb.table.GetSelf().ID, receiver)
}

// SendEvent sends an event to the slice leader or enqueues it if the send fails.
func (eb *EventBoard) SendEvent(event *Event) {
	eb.Normal.sendEvent(event, eb.client, eb.table)
}

// UpdateTable updates the routing table with the given event information.
func (eb *EventBoard) UpdateTable(event []*Event) error {
	for _, ev := range event {
		switch ev.eventType {
		case JOIN:
			// add the new node to the routing table
			entry := routingtable.NewPublicEntry(
				ev.GetTargetID(),
				ev.GetTargetAddress(),
				ev.GetTargetIsSuperNode(),
				false,
				false,
			)
			err := eb.table.AddEntry(entry)
			if err != nil {
				return err
			}
		case LEAVE:
			// remove the node from the routing table
			_ = eb.table.RemoveEntry(ev.GetTargetID())
		case BECOME_UNITLEADER:
			// Update the routing table with the new unit leader information
			_ = eb.table.BecomeUnitLeader(ev.GetTargetID())
		case BECOME_SLICELEADER:
			// Update the routing table with the new slice leader information
			_ = eb.table.BecomeSliceLeader(ev.GetTargetID())
		}
	}
	return nil
}
