package dht

import (
	"GuptaDHT/internal/logger"
	"errors"
	"fmt"
	"sync"
)

// Errors defined for the RoutingTable operations.
var (
	ErrRoutingTableEmpty       = errors.New("routing table is empty")
	ErrEntryNotFound           = errors.New("routingEntry not found in RoutingTable")
	ErrEntryAlreadyExists      = errors.New("routingEntry already exists in RoutingTable")
	ErrInvalidParameters       = errors.New("invalid parameters for operation in RoutingTable")
	ErrWarpAround              = errors.New("warp around error in binary search, ID is greater than the last entry")
	ErrBinarySearch            = errors.New("error in binary search, impossible")
	ErrUnitLeaderAlreadyExists = errors.New("unit leader already exists in the unit leaders table")
	ErrPredRedirect            = errors.New("predecessor already exists, redirecting to the real predecessor")
)

// ----- Primitive Structures -----

// RoutingEntry rapresent a single entry in the routing table of a DHT node.
type RoutingEntry struct {
	ID      ID     // id node
	Address string // ip:port address of the node
}

// NewRoutingEntry create a new RoutingEntry with the specified ID and address.
func NewRoutingEntry(id ID, address string) *RoutingEntry {
	entry := RoutingEntry{
		ID:      id,
		Address: address,
	}
	return &entry
}

// RoutingTable table rapresents the routing table of a DHT node, which contains multiple RoutingEntry entries and a mutex for thread safety.
type RoutingTable struct {
	mu      sync.RWMutex    // thread-safe access to the routing table
	entries []*RoutingEntry // slice of RoutingEntry entries in the routing table
}

// NewRoutingTable creates a new RoutingTable instance with an empty slice of RoutingEntry.
func newRoutingTable() RoutingTable {
	return RoutingTable{
		entries: make([]*RoutingEntry, 0),
	}
}

// Neighbor represents a neighbor node in the DHT, containing its ID and address.
type Neighbor struct {
	mu    sync.RWMutex  // thread-safe access to the neighbor
	entry *RoutingEntry // entry in rt table
}

// Table struct that contains the routing table, slice leaders table, unit leaders table and supernodes table.
type Table struct {
	rt   RoutingTable // routing table of the node
	ul   RoutingTable // unit leaders table of the node
	sl   RoutingTable // slice leaders table of the node
	sn   RoutingTable // supernodes table of the node
	Pred Neighbor     // entry pointer to the predecessor node
	Succ Neighbor     // entry pointer to the successor node
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

// ----- RoutingTable Operations -----

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
	return nil, -1, ErrBinarySearch // errore nella ricerca, non è stato trovato un successore (impossibile)
}

// findEntry finds a RoutingEntry in the RoutingTable by its ID. (NO thread-safe)
func (rt *RoutingTable) findEntry(id ID) (*RoutingEntry, int, error) {
	if len(rt.entries) == 0 {
		return nil, -1, ErrRoutingTableEmpty
	}
	// find the successor of the given ID in the routing table
	entry, idx, err := rt.binarySearchLittleMore(id, 0, len(rt.entries)-1)
	if err != nil {
		if errors.Is(err, ErrWarpAround) {
			return nil, -1, ErrEntryNotFound
		} else {
			return nil, -1, fmt.Errorf("failed to find entry: %w", err)
		}
	}
	// if the successor ID is equal to the given ID, we return the entry
	if entry.ID.Equals(id) {
		return entry, idx, nil
	} else {
		return nil, -1, ErrEntryNotFound
	}
}

// addEntry adds a new RoutingEntry to the RoutingTable in the position in parameter idx (NO thread-safe).
func (rt *RoutingTable) addEntry(entry *RoutingEntry, idx int) error {
	// if the index is out of bounds, we return an error
	if idx < 0 || idx > len(rt.entries) {
		return ErrInvalidParameters
	}
	rt.entries = append(rt.entries, &RoutingEntry{}) // add an empty entry at the end
	copy(rt.entries[idx+1:], rt.entries[idx:])       // shift the entries to the right
	rt.entries[idx] = entry                          // insert the new entry at the found index
	return nil
}

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
		if errors.Is(err, ErrEntryNotFound) {
			return ErrEntryNotFound // if the entry is not found, we return an error
		}
		return fmt.Errorf("failed to find entry for removal: %w", err) // return the error if something went wrong
	}
	copy(rt.entries[idx:], rt.entries[idx+1:])  // shift the entries to the left
	rt.entries = rt.entries[:len(rt.entries)-1] // reduce the length of the slice
	return nil                                  // return nil if the removal was successful
}

// ----- Table Operations -----

// FindSuccessor finds the successor of a given ID in the RoutingTable. It returns the entry and its index if found, otherwise returns an error. (Thread-safe)
func (t *Table) FindSuccessor(id ID) (*RoutingEntry, int, error) {
	// control parameters
	if id == (ID{}) {
		return nil, -1, ErrInvalidParameters // if the ID is empty, we return an error
	}
	// lock the read mutex to ensure thread safety
	t.rt.mu.RLock()
	defer t.rt.mu.RUnlock() // unlock the table at the end of the reading
	// find the entry in the routing table
	logger.Log.Infof("parameter: id: %s, start: %d, end: %d", id.ToHexString(), 0, len(t.rt.entries)-1)
	entry, idx, err := t.rt.binarySearchLittleMore(id, 0, len(t.rt.entries)-1)
	if err != nil {
		if errors.Is(err, ErrWarpAround) {
			return t.rt.entries[0], 0, nil // if the ID is greater than the last entry, we return the first entry as the successor
		}
		return nil, -1, fmt.Errorf("failed to find successor: %w", err) // return the error if something went wrong
	}
	return entry, idx, nil // return the entry and its index if found
}

// FindEntry finds a RoutingEntry in the RoutingTable by its ID. It returns the entry and its index if found, otherwise returns an error. (Thread-safe)
func (t *Table) FindEntry(id ID) (*RoutingEntry, int, error) {
	// control parameters
	if id == (ID{}) {
		return nil, -1, ErrInvalidParameters // if the ID is empty, we return an error
	}
	// lock the read mutex to ensure thread safety
	t.rt.mu.RLock()
	defer t.rt.mu.RUnlock() // unlock the table at the end of the reading
	// find the entry in the routing table
	entry, idx, err := t.rt.findEntry(id)
	if err != nil {
		return nil, -1, err // return the error if something went wrong
	}
	return entry, idx, nil // return the entry and its index if found
}

// AddEntry adds a new RoutingEntry to the RoutingTable if it does not already exist, otherwise returns an error. manteining the order of entries based on their IDs. (Thread-safe)
func (t *Table) AddEntry(id ID, address string, superNode bool, unitLeader bool, sliceLeader bool, K int, U int) error {
	// control parameters
	if id == (ID{}) || address == "" {
		return ErrInvalidParameters
	}
	// create a new RoutingEntry with the given ID and address
	entry := NewRoutingEntry(id, address)
	// check if the entry is already in the routing table
	_, _, err := t.rt.findEntry(id)
	if err == nil {
		// the entry already exists, return an error
		return ErrEntryAlreadyExists
	} else if errors.Is(err, ErrRoutingTableEmpty) {
		// the entry does not exist, but the table is empty, so we can proceed to add it
		// lock the write mutex to ensure thread safety
		t.rt.mu.Lock()
		defer t.rt.mu.Unlock()
		if superNode {
			t.sn.mu.Lock()
			defer t.sn.mu.Unlock()
		}
		if unitLeader {
			t.ul.mu.Lock()
			defer t.ul.mu.Unlock()
		}
		if sliceLeader {
			t.sl.mu.Lock()
			defer t.sl.mu.Unlock()
		}
		err := t.rt.addEntry(entry, 0)
		if err != nil {
			return err
		} // add the entry at the end of the slice
		if superNode {
			err = t.sn.addEntry(entry, 0) // add the entry at the end of the supernodes table
			if err != nil {
				// if we fail to add the entry to the supernodes table, we remove it from the routing table
				_ = t.rt.removeEntry(id)
				return err
			}
		}
		if unitLeader {
			err = t.ul.addEntry(entry, 0) // add the entry at the end of the unit leaders table
			if err != nil {
				_ = t.rt.removeEntry(id) // remove
				_ = t.sn.removeEntry(id) // remove from supernodes table if it was added
				return err
			}
		}
		if sliceLeader {
			err = t.sl.addEntry(entry, 0) // add the entry at the end of the slice leaders table
			if err != nil {
				_ = t.rt.removeEntry(id) // remove
				_ = t.sn.removeEntry(id) // remove from supernodes table if it was added
				_ = t.ul.removeEntry(id) // remove from unit leaders table if it was added
				return err
			}
		}
		return nil
	} else if errors.Is(err, ErrEntryNotFound) {
		// the entry does not exist, but the table is not empty, so we can proceed to add it
		t.rt.mu.Lock()         // lock the write mutex to ensure thread safety
		defer t.rt.mu.Unlock() // unlock the table at the end of the writing
		if superNode {
			t.sn.mu.Lock()
			defer t.sn.mu.Unlock()
		}
		if unitLeader {
			t.ul.mu.Lock()
			defer t.ul.mu.Unlock()
		}
		if sliceLeader {
			t.sl.mu.Lock()
			defer t.sl.mu.Unlock()
		}
		_, idx, err := t.rt.binarySearchLittleMore(id, 0, len(t.rt.entries)-1)
		if errors.Is(err, ErrWarpAround) {
			// is the last entry, we can append it
			err := t.rt.addEntry(entry, len(t.rt.entries))
			if err != nil {
				return err
			}
		} else if err != nil {
			return fmt.Errorf("failed to find entry for insertion: %w", err)
		} else {
			// insert the entry at the found index
			err := t.rt.addEntry(entry, idx)
			if err != nil {
				return err
			}
		}
		// add the entry to the supernodes table if it is a supernode
		if superNode {
			// find the index of the entry in the supernodes table
			_, idx, err := t.sn.binarySearchLittleMore(id, 0, len(t.sn.entries)-1)
			if errors.Is(err, ErrWarpAround) {
				// is the last entry, we can append it
				err = t.sn.addEntry(entry, len(t.sn.entries))
				if err != nil {
					_ = t.rt.removeEntry(entry.ID) // remove
					return err
				}
			} else if err != nil {
				return fmt.Errorf("failed to find supernode entry: %w", err)
			} else {
				err = t.sn.addEntry(entry, idx)
				if err != nil {
					_ = t.rt.removeEntry(entry.ID) // remove
					return err
				}
			}
		}
		// add the entry to the unit leaders table if it is a unit leader
		if unitLeader {
			// find the index of the entry in the unit leaders table
			firstIdofUnit, err := entry.ID.FirstIDOfUnit(K, U)
			if err != nil {
				_ = t.rt.removeEntry(entry.ID) // remove
				_ = t.sn.removeEntry(entry.ID) // remove from supernodes table if it was added
				return fmt.Errorf("failed to get first ID of unit: %w", err)
			}
			temp, idx, err := t.ul.binarySearchLittleMore(firstIdofUnit, 0, len(t.ul.entries)-1)
			if errors.Is(err, ErrWarpAround) {
				// is the last entry, we can append it
				err = t.ul.addEntry(entry, len(t.ul.entries))
				if err != nil {
					_ = t.rt.removeEntry(entry.ID) // remove
					_ = t.sn.removeEntry(entry.ID) // remove from supernodes table if it was added
					return err
				}
			} else if err != nil {
				return fmt.Errorf("failed to find supernode entry: %w", err)
			} else {
				// check if the temp is in the same unit of the entry
				same, err := temp.ID.SameUnit(entry.ID, K, U)
				if err != nil {
					_ = t.rt.removeEntry(entry.ID) // remove
					_ = t.sn.removeEntry(entry.ID) // remove from supernodes table if it was added
					return fmt.Errorf("failed to check if unit leader is the same: %w", err)
				}
				if !same {
					err = t.ul.addEntry(entry, idx)
					if err != nil {
						_ = t.rt.removeEntry(entry.ID) // remove
						_ = t.sn.removeEntry(entry.ID) // remove from supernodes table if it was added
						return err
					}
				} else {
					_ = t.rt.removeEntry(entry.ID)    // remove
					_ = t.sn.removeEntry(entry.ID)    // remove from supernodes table if it was added
					return ErrUnitLeaderAlreadyExists // if the entry is already in the unit leaders table, we return an error
				}
			}
		}
		if sliceLeader {
			// find the index of the entry in the slice leaders table
			firstIdSlice, err := entry.ID.FirstIDOfSlice(K)
			if err != nil {
				_ = t.rt.removeEntry(entry.ID) // remove
				_ = t.sn.removeEntry(entry.ID) // remove
				_ = t.ul.removeEntry(entry.ID) // remove
				return fmt.Errorf("failed to get first ID of slice: %w", err)
			}
			_, idx, err := t.sl.binarySearchLittleMore(firstIdSlice, 0, len(t.sl.entries)-1)
			if errors.Is(err, ErrWarpAround) {
				// is the last entry, we can append it
				err = t.sl.addEntry(entry, len(t.sl.entries))
				if err != nil {
					_ = t.rt.removeEntry(entry.ID) // remove
					_ = t.sn.removeEntry(entry.ID) // remove from supernodes table if it was added
					_ = t.ul.removeEntry(entry.ID) // remove from unit leaders table if it was added
					return err
				}
			} else if err != nil {
				return fmt.Errorf("failed to find slice leader entry: %w", err)
			} else {
				same, err := t.sl.entries[idx].ID.SameSlice(entry.ID, K)
				if err != nil {
					_ = t.rt.removeEntry(entry.ID) // remove
					_ = t.sn.removeEntry(entry.ID) // remove from supernodes table if it was added
					_ = t.ul.removeEntry(entry.ID) // remove from unit leaders table if it was added
					return fmt.Errorf("failed to check if slice leader is the same: %w", err)
				}
				if !same {
					err = t.sl.addEntry(entry, idx)
					if err != nil {
						_ = t.rt.removeEntry(entry.ID) // remove
						_ = t.sn.removeEntry(entry.ID) // remove from supernodes table if it was added
						_ = t.ul.removeEntry(entry.ID) // remove from unit leaders table if it was added
						return err
					}
				} else {
					_ = t.rt.removeEntry(entry.ID)                                              // remove
					_ = t.sn.removeEntry(entry.ID)                                              // remove from supernodes table if it was added
					_ = t.ul.removeEntry(entry.ID)                                              // remove from unit leaders table if it was added
					return fmt.Errorf("slice leader already exists in the slice leaders table") // if the entry is already in the slice leaders table, we return an error
				}
			}
		}
		return nil
	} else {
		return fmt.Errorf("failed to add entry: %w", err) // return the error if something went wrong
	}
}

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

// ----- Transport Convention -----

// TransportEntry is the rappresentation of a entry for exchange information between nodes in the DHT.
type TransportEntry struct {
	Id            ID
	Address       string
	IsSupernode   bool
	IsSliceLeader bool
	IsUnitLeader  bool
}

// LenTable returns the number of entries in the routing table
func (t *Table) LenTable() int {
	return len(t.rt.entries)
}

// convertToTransportEntry converts a RoutingEntry to a TransportEntry for network transmission. (Server-side)
func (re *RoutingEntry) convertToTransportEntry(isSupernode, isSliceLeader, isUnitLeader bool) TransportEntry {
	return TransportEntry{
		Id:            re.ID,
		Address:       re.Address,
		IsSupernode:   isSupernode,
		IsSliceLeader: isSliceLeader,
		IsUnitLeader:  isUnitLeader,
	}
}

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
