package routingtable

import "GuptaDHT/internal/dht/id"

// La struttura Routing Entry rappresenta l'unità elementare di una routing table. rappresenta una singola entry composta da identificativo del nodo e il suo indirizzo.

// routingEntry rapresent a single entry in the routing table of a DHT node.
type routingEntry struct {
	id      id.ID  // id node
	address string // ip:port address of the node
}

// NewRoutingEntry creates a new routingEntry with the given ID and address.
func newRoutingEntry(id id.ID, address string) *routingEntry {
	return &routingEntry{
		id:      id,
		address: address,
	}
}

// GetID returns the ID of the routing entry
func (re *routingEntry) GetID() id.ID {
	return re.id
}

// GetAddress returns the address of the routing entry
func (re *routingEntry) GetAddress() string {
	return re.address
}
