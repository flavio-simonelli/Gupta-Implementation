package event

// interface client for send events
type EventSender interface {
	// SendEventsToSliceLeader sends events to the slice leader.
	SendToSliceLeader(events []*Event, address string) error
	// SendEventsToUnitLeader sends events to the unit leader.
	SendToUnitLeader(events []*Event, address string) error
	// SendEventsToNormalNode sends events to a normal node.
	SendToNormalNode(events []*Event, address string) error
}
