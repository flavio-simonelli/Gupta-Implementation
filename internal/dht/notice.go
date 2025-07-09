package dht

// EventType represents the type of event in the DHT network
type EventType int

// Possible event types
const (
	JOIN EventType = iota
	LEAVE_NORMAL
	LEAVE_UNITLEADER
	LEAVE_SLICELEADER
)

// The Event represents an event in the DHT network.
type Event struct {
	eventType EventType      // Type of the event
	target    TransportEntry // Target node for the event
}

// NewEvent creates a new event with the specified type and target node.
func NewEvent(tagertId ID, targetaddr string, sn bool, ul bool, sl bool, event EventType) Event {
	target := TransportEntry{
		Id:            tagertId,
		Address:       targetaddr,
		IsSliceLeader: sl,
		IsUnitLeader:  ul,
		IsSupernode:   sn,
	}
	return Event{
		eventType: event,
		target:    target,
	}
}

// ApplyEvent applies the event to the RoutingTable
func (e Event) ApplyEvent(t *Table, K int, U int) error {
	switch e.eventType {
	case JOIN:
		return t.AddEntry(e.target.Id, e.target.Address, e.target.IsSupernode, e.target.IsUnitLeader, e.target.IsSliceLeader, K, U)
	case LEAVE_NORMAL:
		return t.RemoveEntry(e.target.Id)
	case LEAVE_UNITLEADER:
		return t.RemoveEntry(e.target.Id)
	case LEAVE_SLICELEADER:
		return t.RemoveEntry(e.target.Id)
	default:
		return nil // No action for unknown event types
	}
}
