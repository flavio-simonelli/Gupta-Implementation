// File routingtable.go defines the routing table and its operations for a Distributed Hash Table (DHT) node in DHT GUPTA.
package dht

import (
	"GuptaDHT/internal/logger"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Errors defined for the RoutingTable operations.
var (
	// ErrEmptyId indicates that the provided ID is empty.
	ErrEmptyID = errors.New("ID cannot be empty")
	// ErrWarpAround indicates that the ID is greater than the last entry in the routing table, so the successor is the first entry in the routing table.
	ErrWarpAround = errors.New("ID is greater than the last entry in the routing table")
	// ErrEntryNotFound inicates that the entry with the given ID was not found in the routing table.
	ErrEntryNotFound = errors.New("entry not found in the routing table")
	// ErrEmptyAddress indicates that the provided address is empty.
	ErrEmptyAddress = errors.New("address cannot be empty")
	// ErrEntryAlreadyExists indicates that the entry with the given ID already exists in the routing table.
	ErrEntryAlreadyExists = errors.New("entry already exists in the routing table, i can't add it again")
	// ErrUnitLeaderAlreadyExists indicates that the entry with the given ID already exists in the unit leaders table.
	ErrUnitLeaderAlreadyExists = errors.New("unit leader already exists in the unit leaders table, i can't add it again")
	// ErrSliceLeaderAlreadyExists indicates that the entry with the given ID already exists in the slice leaders table.
	ErrSliceLeaderAlreadyExists = errors.New("slice leader already exists in the slice leaders table, i can't add it again")
)

// ----- Primitive Structures -----

// RoutingEntry rapresent a single entry in the routing table of a DHT node.
type RoutingEntry struct {
	ID      ID     // id node
	Address string // ip:port address of the node
}

// RoutingTable table rapresents the routing table of a DHT node, which contains multiple RoutingEntry entries and a mutex for thread safety.
type RoutingTable struct {
	mu      sync.RWMutex    // thread-safe access to the routing table
	entries []*RoutingEntry // slice of RoutingEntry entries in the routing table
}

// Neighbor represents a neighbor node in the DHT, containing its ID and address.
type Neighbor struct {
	mu       sync.RWMutex  // thread-safe access to the neighbor
	entry    *RoutingEntry // entry in rt table
	lastSeen atomic.Int64  // last time the neighbor has been seen (in nanoseconds since epoch)
}

// Table struct that contains the routing table, slice leaders table, unit leaders table and supernodes table.
type Table struct {
	rt   RoutingTable // routing table of the node
	ul   RoutingTable // unit leaders table of the node
	sl   RoutingTable // slice leaders table of the node
	sn   RoutingTable // supernodes table of the node
	pred Neighbor     // entry pointer to the predecessor node
	succ Neighbor     // entry pointer to the successor node
}

// TransportEntry is the rappresentation of a entry for exchange information between nodes in the DHT.
type TransportEntry struct {
	Id            ID
	Address       string
	IsSupernode   bool
	IsSliceLeader bool
	IsUnitLeader  bool
}

// ----- Costructors -----

// NewRoutingEntry create a new RoutingEntry with the specified ID and address.
func NewRoutingEntry(id ID, address string) *RoutingEntry {
	entry := RoutingEntry{
		ID:      id,
		Address: address,
	}
	return &entry
}

// NewRoutingTable creates a new RoutingTable instance with an empty slice of RoutingEntry.
func newRoutingTable() RoutingTable {
	return RoutingTable{
		entries: make([]*RoutingEntry, 0),
	}
}

// NewTable creates a new Table instance with empty routing tables for supernodes, unit leaders, and slice leaders.
func NewTable() *Table {
	return &Table{
		rt: newRoutingTable(),
		ul: newRoutingTable(),
		sl: newRoutingTable(),
		sn: newRoutingTable(),
	}
}

// convertToTransportEntry converts a RoutingEntry to a TransportEntry for network transmission.
func (re *RoutingEntry) convertToTransportEntry(isSupernode, isSliceLeader, isUnitLeader bool) TransportEntry {
	return TransportEntry{
		Id:            re.ID,
		Address:       re.Address,
		IsSupernode:   isSupernode,
		IsSliceLeader: isSliceLeader,
		IsUnitLeader:  isUnitLeader,
	}
}

// ----- RoutingTable Operations exported -----

// FindEntry finds a RoutingEntry in the RoutingTable by its ID. It returns the entry, otherwise returns an error.
func (rt *RoutingTable) FindEntry(id ID) (*RoutingEntry, error) {
	// control parameters
	if id == (ID{}) {
		return nil, ErrEmptyID // if the ID is empty, we return an error
	}
	// lock the read mutex on the routing table
	rt.mu.RLock()
	defer rt.mu.RUnlock() // unlock the table at the end of the reading
	// binary search on the routing table entries to find the entry
	entry, err := rt.findEntry(id)
	if err != nil {
		if errors.Is(err, ErrEntryNotFound) {
			return nil, ErrEntryNotFound // if the entry is not found, we return an error
		}
		return nil, fmt.Errorf("failed to find entry: %w", err) // return the error if something went wrong
	}
	// if the entry is found, we return it
	return entry, nil
}

// FindSuccessor finds the successor of a given ID in the RoutingTable. It returns the entry otherwise returns an error.
func (rt *RoutingTable) FindSuccessor(id ID) (*RoutingEntry, error) {
	// control parameters
	if id == (ID{}) {
		return nil, ErrEmptyID // if the ID is empty, we return an error
	}
	// lock the read mutex on the routing table
	rt.mu.RLock()
	defer rt.mu.RUnlock() // unlock the table at the end of the reading
	// find the successor of the given ID in the routing table
	entry, _, err := rt.binarySearchLittleMore(id, 0, len(rt.entries)-1)
	if err != nil {
		if errors.Is(err, ErrWarpAround) {
			return rt.entries[0], nil // if the ID is greater than the last entry, we return the first entry as the successor
		}
		return nil, fmt.Errorf("failed to find successor: %w", err) // return the error if something went wrong
	}
	return entry, nil // return the entry if found
}

// FindPredecessor finds the predecessor of a given ID in the RoutingTable. It returns the entry otherwise returns an error.
func (rt *RoutingTable) FindPredecessor(id ID) (*RoutingEntry, error) {
	// control parameters
	if id == (ID{}) {
		return nil, ErrEmptyID // if the ID is empty, we return an error
	}
	// lock the read mutex on the routing table
	rt.mu.RLock()
	defer rt.mu.RUnlock() // unlock the table at the end of the reading
	// find the predecessor of the given ID in the routing table
	_, idx, err := rt.binarySearchLittleMore(id, 0, len(rt.entries)-1)
	if err != nil {
		if errors.Is(err, ErrWarpAround) {
			return rt.entries[len(rt.entries)-1], nil // if the index is 0, we return the last entry as the predecessor
		}
		return nil, fmt.Errorf("failed to find predecessor: %w", err) // return the error if something went wrong
	}
	if idx == 0 {
		return rt.entries[len(rt.entries)-1], nil // Warp around case, if the index is 0, we return the last entry as the predecessor
	}
	return rt.entries[idx-1], nil // return the entry at index - 1 as the predecessor
}

// AddEntry adds a new node in the Table if it does not already exist, otherwise returns an error. It maintains the order of entries based on their IDs.
func (t *Table) AddEntry(id ID, address string, superNode bool, unitLeader bool, sliceLeader bool) error {
	// Control Parameters
	if id == (ID{}) {
		return ErrEmptyID // if the ID is empty, we return an error
	}
	if address == "" {
		return ErrEmptyAddress
	}
	// lock the write mutex to ensure thread safety
	t.rt.mu.Lock()         // lock directly the Write mutex because if read first with only read mutex and after request a write mutex is not thread-safe
	defer t.rt.mu.Unlock() // unlock the table at the end of the writing
	// check if the entry is already in the routing table
	_, _, err := t.rt.binarySearchLittleMore(id, 0, len(t.rt.entries)-1)
	if err == nil {
		// the entry already exists, return an error
		return ErrEntryAlreadyExists
	}
	// create a new entry
	entry := NewRoutingEntry(id, address)
	// insert in the routing table
	err = t.rt.addEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to add entry to routing table: %w", err) // return the error if something went wrong
	}
	// if the entry is a supernode, we add it to the supernodes table
	if superNode {
		t.sn.mu.Lock()         // lock the write mutex on the supernodes table
		defer t.sn.mu.Unlock() // unlock the supernodes table at the end of the writing
		err = t.sn.addEntry(entry)
		if err != nil {
			_ = t.rt.removeEntry(id)                                              // remove from routing table if it was added
			return fmt.Errorf("failed to add entry to supernodes table: %w", err) // return the error if something went wrong
		}
	}
	// if the entry is a unit leader, we add it to the unit leaders table
	if unitLeader {
		t.ul.mu.Lock()         // lock the write mutex on the unit leaders table
		defer t.ul.mu.Unlock() // unlock the unit leaders table at the end of the writing
		err = t.ul.addEntry(entry)
		if err != nil {
			_ = t.rt.removeEntry(id)                                                // remove from routing table if it was added
			_ = t.sn.removeEntry(id)                                                // remove from supernodes table if it was added
			return fmt.Errorf("failed to add entry to unit leaders table: %w", err) // return the error if something went wrong
		}
	}
	// if the entry is a slice leader, we add it to the slice leaders table
	if sliceLeader {
		t.sl.mu.Lock()         // lock the write mutex on the slice leaders table
		defer t.sl.mu.Unlock() // unlock the slice leaders table at the end of the writing
		err = t.sl.addEntry(entry)
		if err != nil {
			_ = t.rt.removeEntry(id)                                                 // remove from routing table if it was added
			_ = t.sn.removeEntry(id)                                                 // remove from supernodes table if it was added
			_ = t.ul.removeEntry(id)                                                 // remove from unit leaders table if it was added
			return fmt.Errorf("failed to add entry to slice leaders table: %w", err) // return the error if something went wrong
		}
	}
	return nil
}

// PromoveUL promove a RoutingEntry to Unit Leader in the Unit Leaders Table. It returns an error if there is a unit leader of the same unit already present or if the entry is not in the routing table.
func (t *Table) PromoveUL(id ID) error {
	// control parameters
	if id == (ID{}) {
		return ErrEmptyID // if the ID is empty, we return an error
	}
	// control if the entry is in the routing table
	t.rt.mu.RLock()
	entry, err := t.rt.findEntry(id)
	if err != nil {
		t.rt.mu.RUnlock() // unlock the routing table before returning
		if errors.Is(err, ErrEntryNotFound) {
			return ErrEntryNotFound
		}
		return fmt.Errorf("failed to find entry in routing table for promotion: %w", err) // return the error if something went wrong
	}
	t.rt.mu.RUnlock() // unlock the routing table after finding the entry
	// control the unit of the id not have a unit leader already present
	t.ul.mu.Lock()
	defer t.ul.mu.Unlock() // unlock the unit leaders table at the end of the writing
	firstIDOfUnit, err := id.FirstIDOfUnit()
	if err != nil {
		return fmt.Errorf("failed to get first ID of unit: %w", err) // return the error if something went wrong
	}
	_, err = t.ul.findEntry(firstIDOfUnit)
	if !errors.Is(err, ErrEntryNotFound) {
		if err == nil {
			return ErrUnitLeaderAlreadyExists
		}
		return fmt.Errorf("failed to find a unit leader already present: %w", err) // return the error if something went wrong
	}
	// we can promote the entry to unit leader
	err = t.ul.addEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to promote entry to unit leader: %w", err) // return the error if something went wrong
	}
	return nil
}

// PromoveSL promove a RoutingEntry to Slice Leader in the Slice Leaders Table. It returns an error if there is a slice leader of the same slice already present or if the entry is not in the routing table.
func (t *Table) PromoveSL(id ID) error {
	// control parameters
	if id == (ID{}) {
		return ErrEmptyID // if the ID is empty, we return an error
	}
	// control if the entry is in the routing table
	t.rt.mu.RLock()
	entry, err := t.rt.findEntry(id)
	if err != nil {
		t.rt.mu.RUnlock() // unlock the routing table before returning
		if errors.Is(err, ErrEntryNotFound) {
			return ErrEntryNotFound
		}
		return fmt.Errorf("failed to find entry in routing table for promotion: %w", err) // return the error if something went wrong
	}
	t.rt.mu.RUnlock() // unlock the routing table after finding the entry
	// control the unit of the id not have a unit leader already present
	t.sl.mu.Lock()
	defer t.sl.mu.Unlock() // unlock the unit leaders table at the end of the writing
	firstIDOfSlice, err := id.FirstIDOfSlice()
	if err != nil {
		return fmt.Errorf("failed to get first ID of slice: %w", err) // return the error if something went wrong
	}
	_, err = t.sl.findEntry(firstIDOfSlice)
	if !errors.Is(err, ErrEntryNotFound) {
		if err == nil {
			return ErrUnitLeaderAlreadyExists
		}
		return fmt.Errorf("failed to find a slice leader already present: %w", err) // return the error if something went wrong
	}
	// we can promote the entry to unit leader
	err = t.sl.addEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to promote entry to slice leader: %w", err) // return the error if something went wrong
	}
	return nil
}

// fare da qui
// RemoveEntry removes a RoutingEntry from the RoutingTable and other table by its ID. It returns an error if the entry does not exist or if the ID is invalid.
func (t *Table) RemoveEntry(id ID) error

// IsPredecessor checks if the given ID is the predecessor of the node. It returns true if it is, otherwise false.
func (t *Table) IsPredecessor(id ID) bool {
	t.pred.mu.RLock()
	defer t.pred.mu.RUnlock() // unlock the predecessor entry at the end of the reading
	if t.pred.entry == nil {
		return false // if the predecessor entry is nil, it means that there is no predecessor
	}
	return t.pred.entry.ID.Equals(id) // check if the ID is equal to the predecessor entry ID
}

// IsSuccessor checks if the given ID is the successor of the node. It returns true if it is, otherwise false.
func (t *Table) IsSuccessor(id ID) bool {
	t.succ.mu.RLock()
	defer t.succ.mu.RUnlock() // unlock the successor entry at the end of the reading
	if t.succ.entry == nil {
		return false // if the successor entry is nil, it means that there is no successor
	}
	return t.succ.entry.ID.Equals(id) // check if the ID is equal to the successor entry ID
}

// FindSliceLeader finds the slice leader for a given ID in the Slice Leaders Table. It returns the entry and its index if found, otherwise returns an error.
func (t *Table) FindSliceLeader(id ID) (*RoutingEntry, error)

// FindUnitLeader finds the unit leader for a given ID in the Unit Leaders Table. It returns the entry and its index if found, otherwise returns an error.
func (t *Table) FindUnitLeader(id ID) (*RoutingEntry, error)

// GetNumberOfEntries returns the number of entries in the RoutingTable.
func (t *Table) GetNumberOfEntries() int {
	t.rt.mu.RLock()
	defer t.rt.mu.RUnlock()  // unlock the table at the end of the reading
	return len(t.rt.entries) // return the number of entries in the routing table
}

// GetSliceTransportEntries returns a slice of TransportEntry for all entries in the Slice Leaders Table.
func (t *Table) GetSliceTransportEntries(start, end int) []TransportEntry {
}

// ----- Internal Operation -----

// binarySearchLittleMore performs a binary search to find the index of the successor by a given ID in the RoutingTable entries. (NO thread-safe)
// if return error ErrWarpAround, it means that the ID is greater than the last entry in the routing table, so the successor is the first entry in the routing table.
func (rt *RoutingTable) binarySearchLittleMore(id ID, start int, end int) (*RoutingEntry, int, error) {
	if start > end {
		return nil, -1, ErrWarpAround // if start is greater than end, we return an error
	}
	if end == len(rt.entries) && rt.entries[end].ID.LessThan(id) {
		return nil, -1, ErrWarpAround // id is greater than the last entry, so we cannot find
	}
	low, high := start, end
	for low <= high {
		// prendiamo la entry centrale
		mid := low + (high-low)/2
		midId := rt.entries[mid].ID
		// se midId < id iteriamo la procedura con la metà destra
		if midId.LessThan(id) {
			return rt.binarySearchLittleMore(id, mid+1, high)
		} else if midId.Equals(id) {
			// se midId == id ritorniamo l'endpoint associato poichè siamo sicuri che sia il successore
			return rt.entries[mid], mid, nil
		} else {
			// se midId > id il punto trovato è un possibile candidato a essere il successore
			// verifichiamo che sia il più vicino possibile, cioè la entryID precedente deve essere minore di id
			if mid == 0 || rt.entries[mid-1].ID.LessThan(id) {
				return rt.entries[mid], mid, nil // ritorniamo l'endpoint associato alla entry corrente
			} else if rt.entries[mid-1].ID.Equals(id) {
				// la entry precedente ha lo stesso ID, quindi ritorniamo il suo endpoint
				return rt.entries[mid-1], mid - 1, nil
			}
			// altrimenti iteriamo la procedura con la metà sinistra
			return rt.binarySearchLittleMore(id, low, mid)
		}
	}
	return nil, -1, fmt.Errorf("unknow error binary search") // errore nella ricerca, non è stato trovato un successore (impossibile)
}

// findEntry finds a RoutingEntry in the RoutingTable by its ID. It returns the entry and its index if found, otherwise returns an error. (NO thread-safe)
func (rt *RoutingTable) findEntry(id ID) (*RoutingEntry, error) {
	if len(rt.entries) == 0 {
		return nil, ErrEntryNotFound // if the routing table is empty, we return an error
	}
	// binary search on the routing table entries to find the entry
	entry, _, err := rt.binarySearchLittleMore(id, 0, len(rt.entries)-1)
	if err != nil {
		if errors.Is(err, ErrWarpAround) {
			return nil, ErrEntryNotFound // if the ID is greater than the last entry, we return an error
		}
		return nil, fmt.Errorf("failed to find entry: %w", err) // return the error if something went wrong
	}
	return entry, nil // return the entry and its index if found
}

// addEntry adds a new RoutingEntry to the RoutingTable in the position in parameter idx (NO thread-safe).
func (rt *RoutingTable) addEntry(entry *RoutingEntry) error {
	var idx int // index where to insert the new entry
	if len(rt.entries) == 0 {
		// if the routing table is empty, we add the entry at the beginning
		idx = 0
	} else {
		var err error
		_, idx, err = rt.binarySearchLittleMore(entry.ID, 0, len(rt.entries)-1) // find the position to insert the new entry
		if err != nil {
			if !errors.Is(err, ErrWarpAround) {
				logger.Log.Errorf("failed to find entry for insertion: %v", err) // log the error if something went wrong
				return fmt.Errorf("failed to find entry for insertion: %w", err) // return the error if something went wrong
			}
			idx = len(rt.entries) // if the ID is greater than the last entry, we append it at the end
		}
	}
	rt.entries = append(rt.entries, &RoutingEntry{}) // add an empty entry at the end
	copy(rt.entries[idx+1:], rt.entries[idx:])       // shift the entries to the right
	rt.entries[idx] = entry                          // insert the new entry at the found index
	return nil
}

// da rivedere da qui
// removeEntry removes a RoutingEntry from the RoutingTable in a specific index. (NO thread-safe)
func (rt *RoutingTable) removeEntry(id ID) error {
	// control parameters
	if id == (ID{}) {
		return ErrInvalidParameters // if the ID is empty, we return an error
	}
	// if the routing table is empty, we return an error
	if len(rt.entries) == 0 {
		return ErrRoutingTableEmpty
	}
	// find the position of the entry to remove
	_, idx, err := rt.findEntry(id)
	if err != nil {
		return fmt.Errorf("failed to find entry for removal: %w", err) // return the error if something went wrong
	}
	copy(rt.entries[idx:], rt.entries[idx+1:])  // shift the entries to the left
	rt.entries = rt.entries[:len(rt.entries)-1] // reduce the length of the slice
	return nil                                  // return nil if the removal was successful
}

// ----- Vecchie -----

// RemoveEntry removes a RoutingEntry from the RoutingTable by its ID. It returns an error if the entry does not exist or if the ID is invalid. (Thread-safe)
func (t *Table) RemoveEntry(id ID) error {
	// control parameters
	if id == (ID{}) {
		return ErrInvalidParameters // if the ID is empty, we return an error
	}
	// if the routing table is empty, we return an error
	if len(t.rt.entries) == 0 {
		return ErrRoutingTableEmpty
	}
	// block the routing table for writing
	t.rt.mu.Lock()
	defer t.rt.mu.Unlock() // unlock the table at the end of the writing
	// remove the entry from the routing table
	err := t.rt.removeEntry(id)
	if err != nil {
		return err // return the error if something went wrong
	}
	// remove the entry from the supernodes table if it exists
	if _, _, err := t.sn.findEntry(id); err == nil {
		t.sn.mu.Lock()
		defer t.sn.mu.Unlock()
		err = t.sn.removeEntry(id)
		if err != nil {
			return fmt.Errorf("failed to remove entry from supernodes table: %w", err)
		}
	}
	// remove the entry from the unit leaders table if it exists
	if _, _, err := t.ul.findEntry(id); err == nil {
		t.ul.mu.Lock()
		defer t.ul.mu.Unlock()
		err = t.ul.removeEntry(id)
		if err != nil {
			return fmt.Errorf("failed to remove entry from unit leaders table: %w", err)
		}
	}
	// remove the entry from the slice leaders table if it exists
	if _, _, err := t.sl.findEntry(id); err == nil {
		t.sl.mu.Lock()
		defer t.sl.mu.Unlock()
		err = t.sl.removeEntry(id)
		if err != nil {
			return fmt.Errorf("failed to remove entry from slice leaders table: %w", err)
		}
	}
	// if the entry was the predecessor, we remove it from the predecessor entry
	if t.Pred.entry != nil && t.Pred.entry.ID.Equals(id) {
		t.Pred.mu.Lock()
		defer t.Pred.mu.Unlock() // unlock the predecessor entry at the end of the writing
		t.Pred.entry = nil       // set the predecessor entry to nil
	}
	// if the entry was the successor, we remove it from the successor entry
	if t.Succ.entry != nil && t.Succ.entry.ID.Equals(id) {
		t.Succ.mu.Lock()
		defer t.Succ.mu.Unlock() // unlock the successor entry at the end of the writing
		t.Succ.entry = nil       // set the successor entry to nil
	}
	return nil // return nil if the removal was successful
}

// ChangePredecessor changes the predecessor of the node to the given RoutingEntry. It returns an error if the entry is invalid or if the predecessor already exists, return the address of real predecessor. (Thread-safe)
func (t *Table) ChangePredecessor(id ID, address string, supernode bool, K, U int) (string, error) {
	// control parameters
	if address == "" || id == (ID{}) {
		return "", ErrInvalidParameters // if the ID or address is empty, we return an error
	}
	// add the entry to the routing table
	err := t.AddEntry(id, address, supernode, false, false, K, U)
	if err != nil && !errors.Is(err, ErrEntryAlreadyExists) {
		return "", fmt.Errorf("failed to add entry to routing table: %w", err)
	}
	// lock the predecessor entry for writing
	t.Pred.mu.Lock()
	defer t.Pred.mu.Unlock() // unlock the predecessor entry at the end of the writing
	// check if the node is really a new predecessor
	if t.Pred.entry == nil || t.Pred.entry.ID.LessThan(id) {
		// get the entry from the routing table
		entry, _, err := t.FindEntry(id)
		if err != nil {
			return "", fmt.Errorf("failed to find entry for predecessor: %w", err)
		}
		// change the predecessor entry to the new entry
		old := ""
		if t.Pred.entry != nil {
			old = t.Pred.entry.Address // save the old predecessor address
		} else {
			old = "" // if the predecessor was nil, we set the old address to empty
		}
		t.Pred.entry = entry
		return old, nil
	}
	// if the predecessor already exists and is greater than the new entry, we return the address of the old predecessor
	return t.Pred.entry.Address, ErrPredRedirect
}

// ChangeSuccessor changes the successor of the node to the given RoutingEntry. It returns an error if the entry is invalid or if the successor already exists, return the address of real successor. (Thread-safe)
func (t *Table) ChangeSuccessor(id ID, address string, supernode bool, K, U int) (string, error) {
	// control parameters
	if address == "" || id == (ID{}) {
		return "", ErrInvalidParameters // if the ID or address is empty, we return an error
	}
	// add the entry to the routing table
	err := t.AddEntry(id, address, supernode, false, false, K, U)
	if err != nil && !errors.Is(err, ErrEntryAlreadyExists) {
		return "", fmt.Errorf("failed to add entry to routing table: %w", err)
	}
	// lock the successor entry for writing
	t.Succ.mu.Lock()
	defer t.Succ.mu.Unlock() // unlock the successor entry at the end of the writing
	// check if the node is really a new successor
	if t.Succ.entry == nil || id.LessThan(t.Succ.entry.ID) {
		// get the entry from the routing table
		entry, _, err := t.FindEntry(id)
		if err != nil {
			return "", fmt.Errorf("failed to find entry for successor: %w", err)
		}
		// change the successor entry to the new entry
		old := ""
		if t.Succ.entry != nil {
			old = t.Succ.entry.Address // save the old successor address
		} else {
			old = "" // if the successor was nil, we set the old address to empty
		}
		t.Succ.entry = entry
		return old, nil
	}
	// if the successor already exists and is greater than the new entry, we return the address of the old successor
	return t.Succ.entry.Address, ErrPredRedirect
}

// ----- Transport Convention -----

// ToTransportEntry converts a Table to a slice of TransportEntry for network transmission. (Server-side)
func (t *Table) ToTransportEntry(start, end int) ([]TransportEntry, error) {
	if start < 0 {
		start = 0
	}
	t.rt.mu.RLock()
	rtLen := len(t.rt.entries)
	t.rt.mu.RUnlock()
	if end <= 0 || end > rtLen {
		end = rtLen
	}
	if start >= end {
		return nil, fmt.Errorf("invalid range: start (%d) must be less than end (%d)", start, end)
	}
	// block in read mode all the routing tables
	t.rt.mu.RLock()
	t.ul.mu.RLock()
	t.sl.mu.RLock()
	t.sn.mu.RLock()
	// defer to unlock the mutexes at the end of the function
	defer t.sn.mu.RUnlock()
	defer t.sl.mu.RUnlock()
	defer t.ul.mu.RUnlock()
	defer t.rt.mu.RUnlock()
	// for each entry in the routing table, convert it to a TransportEntry and append it to the slice
	part := t.rt.entries[start:end]
	entries := make([]TransportEntry, 0, len(part)) // preallocate the slice with the expected size
	var isSupernode, isSliceLeader, isUnitLeader bool
	// find the indices for the supernodes, slice leaders, and unit leaders
	_, j, err := t.sn.binarySearchLittleMore(part[0].ID, 0, len(t.sn.entries)-1)
	if err != nil {
		if errors.Is(err, ErrWarpAround) {
			j = len(t.sn.entries) // no search needed
		}
		return nil, fmt.Errorf("failed to find supernode index: %w", err)
	}
	_, k, err := t.sl.binarySearchLittleMore(part[0].ID, 0, len(t.sl.entries)-1)
	if err != nil {
		if errors.Is(err, ErrWarpAround) {
			k = len(t.sl.entries) // no search needed
		}
		return nil, fmt.Errorf("failed to find slice leader index: %w", err)
	}
	_, v, err := t.ul.binarySearchLittleMore(part[0].ID, 0, len(t.ul.entries)-1)
	if err != nil {
		if errors.Is(err, ErrWarpAround) {
			v = len(t.ul.entries) // no search needed
		}
		return nil, fmt.Errorf("failed to find unit leader index: %w", err)
	}
	for _, entry := range t.rt.entries {
		if j < len(t.sn.entries) && entry.ID.Equals(t.sn.entries[j].ID) {
			isSupernode = true
			j++
		} else {
			isSupernode = false
		}
		if k < len(t.sl.entries) && entry.ID.Equals(t.sl.entries[k].ID) {
			isSliceLeader = true
			k++
		} else {
			isSliceLeader = false
		}
		if v < len(t.ul.entries) && entry.ID.Equals(t.ul.entries[v].ID) {
			isUnitLeader = true
			v++
		} else {
			isUnitLeader = false
		}
		// convert the entry to a TransportEntry and append it to the slice
		entries = append(entries, entry.convertToTransportEntry(isSupernode, isSliceLeader, isUnitLeader))
	}
	return entries, nil
}

// AddTransportEntry adds a TransportEntry to the Table, converting it to a RoutingEntry and adding it to the appropriate routing tables. (Client-side)
func (t *Table) AddTransportEntry(te TransportEntry, K int, U int) error {
	// add the entry to the routing table
	err := t.AddEntry(te.Id, te.Address, te.IsSupernode, te.IsUnitLeader, te.IsSliceLeader, K, U)
	if err != nil {
		return fmt.Errorf("failed to add transport entry: %w", err)
	}
	return nil
}

// for the keep-alive neighbor
// StartKeepAlive avvia una goroutine che garantisce che
// now‑lastSeen ≤ maxSilence per tutta la durata di ctx.
// sendKA viene chiamato per spedire il messaggio gRPC.
func (n *Neighbor) StartKeepAlive(
	ctx context.Context,
	maxSilence time.Duration,
	sendKA func() error,
) {
	go func() {
		for {
			// 1. Calcola quanto manca alla scadenza
			last := time.Unix(0, n.lastSeen.Load())
			deadline := last.Add(maxSilence)
			sleep := time.Until(deadline)

			// 2. Se già scaduto (o quasi), manda subito
			if sleep <= 0 {
				if err := sendKA(); err == nil {
					n.lastKeepSend.Store(time.Now().UnixNano())
				}
				// Dopo l'invio, ricomincia il ciclo:
				// potrebbe arrivare un ack che aggiorna lastSeen
				continue
			}

			// 3. Dormi finché serve o finché ctx non viene cancellato
			t := time.NewTimer(sleep)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
				// timer scaduto → loop e ricontrollo
			}
		}
	}()
}
