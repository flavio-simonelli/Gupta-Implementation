package routingtable

import (
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/logger"
	"errors"
)

var (
	// ErrInvalidAddress is an error indicating that the provided address is invalid.
	ErrInvalidAddress = errors.New("invalid address")
	// ErrNeighborNotFound is an error indicating that the neighbor entry was not found.
	ErrNeighborNotFound = errors.New("neighbor not found")
)

// Table is the main structure representing the routing table of a DHT node and contains multiple routingTable instances for different purposes. (NOT Thread-Safe)
type Table struct {
	rt   *routingTable // routing table of the node
	ul   *routingTable // unit leaders table of the node
	sl   *routingTable // slice leaders table of the node
	sn   *routingTable // supernodes table of the node
	pred *neighbor     // entry pointer to the predecessor node
	succ *neighbor     // entry pointer to the successor node
	self *routingEntry // entry pointer to the self node (the node itself)
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

// RemoveEntry removes an entry from the routing table. It returns an error if the entry does not exist.
func (t *Table) RemoveEntry(entry PublicEntry) error {
	if entry.Address == "" {
		return ErrInvalidAddress // Invalid parameters, return error
	}
	// remove the entry from the routing table
	err := t.rt.removeEntry(entry.ID)
	if err != nil {
		return err
	}
	// if the entry is a supernode, remove it from the supernodes table
	if entry.IsSN {
		err = t.sn.removeEntry(entry.ID)
		if err != nil {
			return err
		}
	}
	// if the entry is a unit leader, remove it from the unit leaders table
	if entry.IsUL {
		err = t.ul.removeEntry(entry.ID)
		if err != nil {
			return err
		}
	}
	// if the entry is a slice leader, remove it from the slice leaders table
	if entry.IsSL {
		err = t.sl.removeEntry(entry.ID)
		if err != nil {
			return err
		}
	}
	return nil
}
