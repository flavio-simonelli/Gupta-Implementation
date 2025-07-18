package routingtable

import (
	"GuptaDHT/internal/dht/id"
	"errors"
	"sync"
)

var (
	// ErrRoutingTableEmpty is an error indicating that the routing table is empty.
	ErrRoutingTableEmpty = errors.New("routing table is empty")
	// ErrEntryAlreadyExists is an error indicating that the routing entry already exists in the table.
	ErrEntryAlreadyExists = errors.New("routing entry already exists")
	// ErrEntryNotFound is an error indicating that the routing entry was not found in the table.
	ErrEntryNotFound = errors.New("routing entry not found")
)

// la routing table è una struttura dati che mantiene le informazioni sui nodi della dht.
// è ideata per mantenere sempre l'ordine di id globale per una ricerca efficiente tramite ricerca bianria

// routingTable table rapresents the routing table of a DHT node, which contains multiple routingEntry entries and a mutex for thread safety.
type routingTable struct {
	mu      sync.RWMutex    // thread-safe access to the routing table
	entries []*routingEntry // slice of routingEntry entries in the routing table
}

// NewRoutingTable creates a new routingTable with an empty slice of routingEntry entries.
func newRoutingTable() *routingTable {
	return &routingTable{
		entries: make([]*routingEntry, 0),
	}
}

// FindSuccessor finds the successor entry for a given ID in the routing table using binary search. [NOT Thread-Safe]
// It returns the routingEntry, its index, and an error if the table is empty.
// If the ID is greater than all entries, it returns the first entry (wrap-around).
func (rt *routingTable) findSuccessor(id id.ID) (*routingEntry, int, error) {
	n := len(rt.entries)
	if n == 0 {
		return nil, -1, ErrRoutingTableEmpty
	}

	low, high := 0, n-1
	for low <= high {
		mid := low + (high-low)/2
		midID := rt.entries[mid].id

		if midID.LessThan(id) {
			low = mid + 1
		} else {
			if mid == 0 || rt.entries[mid-1].id.LessThan(id) {
				return rt.entries[mid], mid, nil
			}
			high = mid - 1
		}
	}
	// wrap-around: if the ID is greater than all entries, return the first entry
	return rt.entries[0], 0, nil
}

// findPredecessor finds the predecessor (strictly less than the given ID) in the routing table using binary search. [NOT Thread-Safe]
// If the ID is smaller than or equal to all entries, it returns the last entry (wrap-around).
func (rt *routingTable) findPredecessor(id id.ID) (*routingEntry, int, error) {
	n := len(rt.entries)
	if n == 0 {
		return nil, -1, ErrRoutingTableEmpty
	}

	low, high := 0, n-1
	var result *routingEntry
	var resultIdx = -1

	for low <= high {
		mid := low + (high-low)/2
		midID := rt.entries[mid].id

		if midID.LessThan(id) {
			// candidate predecessor
			result = rt.entries[mid]
			resultIdx = mid
			low = mid + 1 // try to find a closer one on the right
		} else {
			high = mid - 1
		}
	}

	if result != nil {
		return result, resultIdx, nil
	}

	// Wrap-around case: ID is less than or equal to all entries
	return rt.entries[n-1], n - 1, nil
}

// addEntry adds a new routingEntry to the routing table (NOT thread-safe).
func (rt *routingTable) addEntry(newEntry *routingEntry) error {
	n := len(rt.entries)
	if n == 0 {
		rt.entries = append(rt.entries, newEntry)
		return nil
	}

	// find the position to insert the new entry
	entry, idx, _ := rt.findSuccessor(newEntry.id)

	// If the entry already exists, return an error
	if entry != nil && entry.id.Equals(newEntry.id) {
		return ErrEntryAlreadyExists
	}

	// insert the new entry at the found index
	rt.entries = append(rt.entries, nil)       // add last empty slot
	copy(rt.entries[idx+1:], rt.entries[idx:]) // shift entries to the right
	rt.entries[idx] = newEntry

	return nil
}

// removeEntry removes a routingEntry from the routing table by its ID (NOT thread-safe).
func (rt *routingTable) removeEntry(id id.ID) error {
	n := len(rt.entries)
	if n == 0 {
		return ErrRoutingTableEmpty
	}

	entry, idx, _ := rt.findSuccessor(id)
	if entry == nil || !entry.id.Equals(id) {
		return ErrEntryNotFound
	}

	// rimozione con slicing
	rt.entries = append(rt.entries[:idx], rt.entries[idx+1:]...)
	return nil
}

// containsEntry checks if an entry with the given ID exists in the routing table.
func (rt *routingTable) containsEntry(id id.ID) bool {
	entry, _, _ := rt.findSuccessor(id)
	return entry != nil && entry.id.Equals(id)
}
