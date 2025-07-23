package event

import "GuptaDHT/internal/dht/id"

// interface client for send events
type EventSender interface {
	// SendEventsToSliceLeader sends events to the slice leader.
	SendToSliceLeader(events []*Event, myid id.ID, receiver string) error
	// SendEventsToUnitLeader sends events to the unit leader.
	SendToUnitLeader(events []*Event, myid id.ID, receiver string) error
	// SendEventsToNormalNode sends events to a normal node.
	SendToNormalNode(events []*Event, myid id.ID, receiver string) error
}
