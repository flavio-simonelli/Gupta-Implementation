package routingtable

import "sync"

// neighbor rappresenta un puntatore al nodo predecessore e al nodo successore del nodo a cui si riferisce la tabella di routing

// neighbor represents a neighbor node in the DHT, containing its ID and address.
type neighbor struct {
	mu    sync.RWMutex  // thread-safe access to the neighbor
	entry *routingEntry // entry in rt table
}

// NewNeighbor creates a new neighbor with empty entry.
func newNeighbor() *neighbor {
	return &neighbor{
		entry: nil, // Initialize with no entry
	}
}

// setEntry sets the entry of the neighbor. (Thread-safe)
func (n *neighbor) setEntry(entry *routingEntry) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.entry = entry
}

// getEntry retrieves the entry of the neighbor. (Thread-safe)
func (n *neighbor) getEntry() *routingEntry {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.entry
}
