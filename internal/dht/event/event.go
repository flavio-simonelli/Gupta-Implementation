package event

import (
	"GuptaDHT/internal/dht/id"
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

// NodeDescriptor is the descriptor for a node in the DHT network for an event
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

// GetEventType returns the type of the event.
func (e *Event) GetEventType() EventType {
	return e.eventType
}

// GetTargetID returns the ID of the target node for the event.
func (e *Event) GetTargetID() id.ID {
	return e.target.ID
}

// GetTargetAddress returns the address of the target node for the event.
func (e *Event) GetTargetAddress() string {
	return e.target.Address
}

// GetTargetIsSuperNode returns true if the target node is a supernode.
func (e *Event) GetTargetIsSuperNode() bool {
	return e.target.IsSuperNode
}
