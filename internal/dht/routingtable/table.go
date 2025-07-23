package routingtable

import (
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/logger"
	"errors"
	"sync/atomic"
)

var (
	// ErrInvalidAddress is an error indicating that the provided address is invalid.
	ErrInvalidAddress = errors.New("invalid address")
	// ErrNeighborNotFound is an error indicating that the neighbor entry was not found.
	ErrNeighborNotFound = errors.New("neighbor not found")
	// SliceLeaderItself is an error indicating that the node is the slice leader of itself.
	SliceLeaderItself = errors.New("node is the slice leader of itself")
	// ErrSliceLeaderNotFound is an error indicating that the slice leader entry was not found.
	ErrSliceLeaderNotFound = errors.New("slice leader not found")
)

// Table is the main structure representing the routing table of a DHT node and contains multiple routingTable instances for different purposes. (NOT Thread-Safe)
type Table struct {
	rt            *routingTable // routing table of the node
	ul            *routingTable // unit leaders table of the node
	sl            *routingTable // slice leaders table of the node
	sn            *routingTable // supernodes table of the node
	pred          *neighbor     // entry pointer to the predecessor node
	succ          *neighbor     // entry pointer to the successor node
	self          *routingEntry // entry pointer to the self node (the node itself)
	isSliceLeader atomic.Bool   // indicates if the node is a slice leader
	isUnitLeader  atomic.Bool   // indicates if the node is a unit leader
}

// NewTable creates a new Table with empty routing tables and initializes the predecessor, successor, and self entries.
func NewTable(selfID id.ID, selfAddr string, selfSN bool) (*Table, error) {
	if selfAddr == "" {
		return nil, ErrInvalidAddress // Invalid parameters, return nil
	}
	// create the self entry
	selfEntry := newRoutingEntry(selfID, selfAddr)
	// create the table with empty routing tables
	table := &Table{
		rt:   newRoutingTable(),
		ul:   newRoutingTable(),
		sl:   newRoutingTable(),
		sn:   newRoutingTable(),
		pred: newNeighbor(),
		succ: newNeighbor(),
		self: selfEntry,
	}
	// set the self entry in the routing table
	err := table.rt.addEntry(selfEntry)
	if err != nil {
		return nil, err
	}
	// set the self entry in the supernodes table if the node is a supernode
	if selfSN {
		err = table.sn.addEntry(selfEntry)
		if err != nil {
			return nil, err
		}
	}
	return table, nil
}

// GetSelf returns the self-information of the node.
func (t *Table) GetSelf() PublicEntry {
	// lock thread-safe access
	t.sn.mu.RLock()
	defer t.sn.mu.RUnlock()
	t.ul.mu.RLock()
	defer t.ul.mu.RUnlock()
	t.sl.mu.RLock()
	defer t.sl.mu.RUnlock()
	// search in supernodes table if the node is a supernode
	isSN := t.sn.containsEntry(t.self.id)
	isUL := t.ul.containsEntry(t.self.id)
	isSL := t.sl.containsEntry(t.self.id)
	// return a public entry with self information
	return toPublicEntry(t.self, isSL, isSN, isUL)
}

// GetMyPredecessor returns the predecessor entry of the node.
func (t *Table) GetMyPredecessor() (PublicEntry, error) {
	// lock thread-safe access
	t.pred.mu.RLock()
	defer t.pred.mu.RUnlock()
	t.sn.mu.RLock()
	defer t.sn.mu.RUnlock()
	t.ul.mu.RLock()
	defer t.ul.mu.RUnlock()
	t.sl.mu.RLock()
	defer t.sl.mu.RUnlock()

	pred := t.pred.getEntry()
	if pred == nil {
		return PublicEntry{}, ErrNeighborNotFound
	}
	isSN := t.sn.containsEntry(pred.GetID())
	isUL := t.ul.containsEntry(pred.GetID())
	isSL := t.sl.containsEntry(pred.GetID())
	return toPublicEntry(pred, isSL, isSN, isUL), nil
}

// GetMySuccessor returns the successor entry of the node.
func (t *Table) GetMySuccessor() (PublicEntry, error) {
	// lock thread-safe access
	t.succ.mu.RLock()
	defer t.succ.mu.RUnlock()
	t.sn.mu.RLock()
	defer t.sn.mu.RUnlock()
	t.ul.mu.RLock()
	defer t.ul.mu.RUnlock()
	t.sl.mu.RLock()
	defer t.sl.mu.RUnlock()

	succ := t.succ.getEntry()
	if succ == nil {
		return PublicEntry{}, ErrNeighborNotFound
	}
	isSN := t.sn.containsEntry(succ.GetID())
	isUL := t.ul.containsEntry(succ.GetID())
	isSL := t.sl.containsEntry(succ.GetID())
	return toPublicEntry(succ, isSL, isSN, isUL), nil
}

// GetMySliceLeader returns the slice leaders entries of the node
func (t *Table) GetMySliceLeader() (PublicEntry, error) {
	// lock thread-safe access
	t.sl.mu.RLock()
	defer t.sl.mu.RUnlock()
	// find my slice leader
	firstIdSlice, err := t.self.id.FirstIDOfSlice()
	if err != nil {
		return PublicEntry{}, err
	}
	sliceleader, _, err := t.sl.findSuccessor(firstIdSlice)
	if err != nil {
		return PublicEntry{}, err
	}
	// check if the node find is the slice leader of myself
	if t.self.id.SameSlice(sliceleader.id) {
		if t.self.id.Equals(sliceleader.id) {
			// the node is the slice leader of itself
			// return the self entry
			isSN := t.sn.containsEntry(t.self.id)
			isUL := t.ul.containsEntry(t.self.id)
			node := toPublicEntry(t.self, true, isSN, isUL)
			return node, SliceLeaderItself
		}
		// the node is not the slice leader of itself, return the slice leader entry
		isSN := t.sn.containsEntry(sliceleader.GetID())
		isUL := t.ul.containsEntry(sliceleader.GetID())
		node := toPublicEntry(t.self, true, isSN, isUL)
		return node, nil
	}
	return PublicEntry{}, ErrSliceLeaderNotFound
}

// GetSliceLeader returns the slice leader entry for a given ID in the routing table.
func (t *Table) GetSliceLeader(index int) (PublicEntry, error) {
	// find the slice leader for the given ID
	firstIdSlice, err := id.FirstIDOfSlice(index)
	if err != nil {
		return PublicEntry{}, err
	}
	// lock thread-safe access
	t.sl.mu.RLock()
	defer t.sl.mu.RUnlock()
	sliceleader, _, err := t.sl.findSuccessor(firstIdSlice)
	if err != nil {
		return PublicEntry{}, err
	}
	// check if the node find is the slice leader of myself
	t.sn.mu.RLock()
	isSN := t.sn.containsEntry(sliceleader.GetID())
	t.sn.mu.RUnlock()
	t.ul.mu.RLock()
	isUL := t.ul.containsEntry(sliceleader.GetID())
	t.ul.mu.RUnlock()
	t.sl.mu.RLock()
	isSL := t.sl.containsEntry(sliceleader.GetID())
	t.sl.mu.RUnlock()
	return toPublicEntry(sliceleader, isSL, isSN, isUL), nil
}

// GetUnitLeader finds the unit leader entry for a given ID in the routing table.
func (t *Table) GetUnitLeader(sliceIndex, unitIndex int) (PublicEntry, error) {
	// find the first ID of the unit that contains the given slice and unit indices
	firstIdUnit, err := id.FirstIDOfUnit(sliceIndex, unitIndex)
	if err != nil {
		return PublicEntry{}, err
	}
	// lock thread-safe access
	t.ul.mu.RLock()
	defer t.ul.mu.RUnlock()

	entry, _, err := t.ul.findSuccessor(firstIdUnit)
	if err != nil {
		return PublicEntry{}, err
	}
	t.sn.mu.RLock()
	isSN := t.sn.containsEntry(entry.GetID())
	t.sn.mu.RUnlock()
	t.ul.mu.RLock()
	isUL := t.ul.containsEntry(entry.GetID())
	t.ul.mu.RUnlock()
	t.sl.mu.RLock()
	isSL := t.sl.containsEntry(entry.GetID())
	t.sl.mu.RUnlock()
	return toPublicEntry(entry, isSL, isSN, isUL), nil
}

// GetSuccessor finds the successor entry for a given ID in the routing table.
func (t *Table) GetSuccessor(id id.ID) (PublicEntry, error) {
	// lock thread-safe access
	t.rt.mu.RLock()
	defer t.rt.mu.RUnlock()
	t.sl.mu.RLock()
	defer t.sl.mu.RUnlock()
	t.sn.mu.RLock()
	defer t.sn.mu.RUnlock()
	t.ul.mu.RLock()
	defer t.ul.mu.RUnlock()

	entry, _, err := t.rt.findSuccessor(id)
	if err != nil {
		return PublicEntry{}, err
	}
	isSN := t.sn.containsEntry(entry.GetID())
	isUL := t.ul.containsEntry(entry.GetID())
	isSL := t.sl.containsEntry(entry.GetID())
	return toPublicEntry(entry, isSL, isSN, isUL), nil
}

// GetPredecessor finds the predecessor entry for a given ID in the routing table.
func (t *Table) GetPredecessor(id id.ID) (PublicEntry, error) {
	// lock thread-safe access
	t.rt.mu.RLock()
	defer t.rt.mu.RUnlock()
	t.sl.mu.RLock()
	defer t.sl.mu.RUnlock()
	t.sn.mu.RLock()
	defer t.sn.mu.RUnlock()
	t.ul.mu.RLock()
	defer t.ul.mu.RUnlock()

	entry, _, err := t.rt.findPredecessor(id)
	if err != nil {
		return PublicEntry{}, err
	}
	isSN := t.sn.containsEntry(entry.GetID())
	isUL := t.ul.containsEntry(entry.GetID())
	isSL := t.sl.containsEntry(entry.GetID())
	return toPublicEntry(entry, isSL, isSN, isUL), nil
}

// AddEntry adds a new entry to the routing table. It returns an error if the entry already exists or if the address is invalid.
func (t *Table) AddEntry(entry PublicEntry) error {
	// lock thread-safe access
	t.rt.mu.RLock()
	defer t.rt.mu.RUnlock()

	if entry.Address == "" {
		return ErrInvalidAddress // Invalid parameters, return error
	}
	// create a new routing entry from the public entry
	rEntry := newRoutingEntry(entry.ID, entry.Address)
	// add the entry to the routing table
	err := t.rt.addEntry(rEntry)
	if err != nil {
		return err
	}
	// if the entry is a supernode, add it to the supernodes table
	if entry.IsSN {
		t.sn.mu.RLock()
		err = t.sn.addEntry(rEntry)
		t.sn.mu.RUnlock()
		if err != nil {
			return err
		}
	}
	// if the entry is a unit leader, add it to the unit leaders table
	if entry.IsUL {
		t.ul.mu.RLock()
		err = t.ul.addEntry(rEntry)
		t.ul.mu.RUnlock()
		if err != nil {
			return err
		}
	}
	// if the entry is a slice leader, add it to the slice leaders table
	if entry.IsSL {
		t.sl.mu.RLock()
		err = t.sl.addEntry(rEntry)
		t.sl.mu.RUnlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// SetPredecessor sets the node as the predecessor of the given ID in the routing table.
func (t *Table) SetPredecessor(id id.ID) error {
	// lock thread-safe access
	t.rt.mu.RLock()
	defer t.rt.mu.RUnlock()
	logger.Log.Infof("finding predecessor for ID: %s", id.ToHexString())
	entry, _, err := t.rt.findSuccessor(id)
	if err != nil {
		return err
	}
	t.pred.mu.Lock()
	defer t.pred.mu.Unlock()
	logger.Log.Infof("setting predecessor for ID: %s to entry: %s", id.ToHexString(), entry.GetID().ToHexString())
	t.pred.setEntry(entry) // set the predecessor entry
	return nil
}

// SetSuccessor sets the node as the successor of the given ID in the routing table.
func (t *Table) SetSuccessor(id id.ID) error {
	t.rt.mu.RLock()
	defer t.rt.mu.RUnlock()
	entry, _, err := t.rt.findPredecessor(id)
	if err != nil {
		return err
	}
	t.succ.mu.Lock()
	defer t.succ.mu.Unlock()
	t.succ.setEntry(entry) // set the successor entry
	return nil
}

// BecomeUnitLeader sets the node as a unit leader in the unit leaders table.
func (t *Table) BecomeUnitLeader(id id.ID) error {
	// lock thread-safe access
	t.rt.mu.RLock()
	defer t.rt.mu.RUnlock()
	entry, _, err := t.rt.findSuccessor(id)
	if err != nil {
		return err
	}
	// set the entry in the unit leaders table
	t.ul.mu.Lock()
	defer t.ul.mu.Unlock()
	return t.ul.addEntry(entry)
}

// BecomeSliceLeader sets the node as a slice leader in the slice leaders table.
func (t *Table) BecomeSliceLeader(id id.ID) error {
	entry, _, err := t.rt.findSuccessor(id)
	if err != nil {
		return err
	}
	// set the entry in the slice leaders table
	return t.sl.addEntry(entry)
}

// IBecomeSliceLeader sets the node as a slice leader in the slice leaders table, but does not check if the node is already a slice leader.
func (t *Table) IBecomeSliceLeader() error {
	// set the entry in the slice leaders table
	t.sl.mu.Lock()
	defer t.sl.mu.Unlock()
	err := t.sl.addEntry(t.self)
	if err != nil {
		return err
	}
	t.isSliceLeader.Store(true) // mark the node as a slice leader
	return nil
}

// IsSliceLeader checks if the node is a slice leader.
func (t *Table) IsSliceLeader() bool {
	return t.isSliceLeader.Load() // return the slice leader status
}

// IBecomeUnitLeader sets the node as a unit leader in the unit leaders table, but does not check if the node is already a unit leader.
func (t *Table) IBecomeUnitLeader() error {
	// set the entry in the unit leaders table
	t.ul.mu.Lock()
	defer t.ul.mu.Unlock()
	err := t.ul.addEntry(t.self)
	if err != nil {
		return err
	}
	t.isUnitLeader.Store(true) // mark the node as a unit leader
	return nil
}

// IsUnitLeader checks if the node is a unit leader.
func (t *Table) IsUnitLeader() bool {
	return t.isUnitLeader.Load() // return the unit leader status
}

// RemoveEntry removes an entry from the routing table. It returns an error if the entry does not exist.
func (t *Table) RemoveEntry(identity id.ID) error {
	// remove the entry from the routing table
	t.rt.mu.Lock()
	err := t.rt.removeEntry(identity)
	if err != nil {
		t.rt.mu.Unlock()
		return err
	}
	t.rt.mu.Unlock()
	// remove in the supernodes table if the entry is a supernode
	t.sn.mu.Lock()
	_ = t.sn.removeEntry(identity)
	t.sn.mu.Unlock()
	// remove in the unit leaders table if the entry is a unit leader
	t.ul.mu.Lock()
	_ = t.ul.removeEntry(identity)
	t.ul.mu.Unlock()
	// remove in the slice leaders table if the entry is a slice leader
	t.sl.mu.Lock()
	_ = t.sl.removeEntry(identity)
	t.sl.mu.Unlock()
	return nil
}
