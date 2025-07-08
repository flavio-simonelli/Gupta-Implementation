package dht

import (
	"GuptaDHT/internal/logger"
	"errors"
	"fmt"
	"time"
)

var (
	ErrCreateDHTNetwork = errors.New("error creating DHT network")
	ErrContactBootstrap = errors.New("error contacting bootstrap node")
	ErrContactSucc      = errors.New("error contacting successor node")
	ErrRedirectSucc     = errors.New("redirect to another successor")
)

type Communicator interface {
	// FindSuccessor ask the node in addr for the successor of the given ID
	FindSuccessor(id ID, addr string, timeout time.Duration) (string, error)
	// BecomePredecessor sends a join request to the successor node in addr
	BecomePredecessor(id ID, addr string, sn bool, table *Table, store *Storage, K int, U int, target string) (string, error)
	// NotifyPredecessor notifies the predecessor node in addr that this node is its successor
	//NotifyPredecessor(ctx context.Context, addr string) error
	// Notify notifies a nod one or more events
	//Notify(ctx context.Context, addr string, event string) error
	// per adesso ci fermiamo alla join
}

type Node struct {
	U           int          // number of units for each slice
	K           int          // number of slices in the DHT
	ID          ID           // Unique identifier for the node
	Addr        string       // Address of the server in this node (ip:port)
	T           *Table       // Routing table for the node
	Supernode   bool         // true if this node is a supernode
	UnitLeader  bool         // true if this node is a unit leader
	SliceLeader bool         // true if this node is a slice leader
	net         Communicator // Communicator interface for network operations (client)
	Store       *Storage     // Storage for the node, can be nil if not used
}

// NewNode creates a new Node with the given ID, address, and communicator and initializes empty routing tables.
func NewNode(u int, k int, idHex string, addr string, client Communicator, sn bool, store *Storage) (*Node, error) {
	id, err := IDFromHexString(idHex)
	if err != nil {
		if errors.Is(err, ErrEmptyHexString) {
			// Generate a new ID if the provided hex string is empty
			id = GenerateID(addr)
			logger.Log.Warnf("Provided ID is empty, generated new ID: %s", id.ToHexString())
		} else {
			return nil, fmt.Errorf("error creating node ID from hex string: %w", err)
		}
	} else {
		logger.Log.Infof("Node ID created from hex string in config: %s", id.ToHexString())
	}
	return &Node{
		U:           u,
		K:           k,
		ID:          id,
		Addr:        addr,
		T:           NewTable(),
		Supernode:   sn,
		UnitLeader:  false,
		SliceLeader: false,
		net:         client,
		Store:       store,
	}, nil
}

func (n *Node) Join(bootstrap string) error {
	// if the bootstrap string is empty, it means that the node is the first node in the DHT network
	if bootstrap == "" {
		logger.Log.Info("The bootstrap string is empty, creating a new DHT network")
		// add the node itself to the routing table and become a slice leader and unit leader
		err := n.T.AddEntry(n.ID, n.Addr, n.Supernode, true, true, n.K, n.U)
		if err != nil {
			logger.Log.Errorf("Error adding self to routing table: %v", err)
			return fmt.Errorf("%w: %w", ErrCreateDHTNetwork, err)
		}
	} else {
		// if the bootstrap string is not empty, it means that the node is joining an existing DHT network
		logger.Log.Infof("Joining an existing DHT network contacting node %s", bootstrap)
		// ask the bootstrap node for my successor
		target, err := n.net.FindSuccessor(n.ID, bootstrap, 5*time.Second)
		if err != nil {
			logger.Log.Errorf("Error contacting bootstrap node: %v", err)
			return fmt.Errorf("%w: %w", ErrContactBootstrap, err)
		}
		// contact the successor node to become its predecessor
		logger.Log.Infof("Contacting successor node at address %s", target)
		for temp, err := n.net.BecomePredecessor(n.ID, n.Addr, n.Supernode, n.T, n.Store, n.K, n.U, target); err != nil; {
			if !errors.Is(err, ErrRedirectSucc) {
				return fmt.Errorf("%w: %w", ErrContactSucc, err)
			}
			target = temp
			logger.Log.Warnf("Redirected to another successor node, retrying with new target: %s", target)
		}
		// set successor and predecessor in the routing table
		// contact the predecessor node to notify it that this node is its successor
		// notify the slice leader that this node is joined the DHT network
	}
	return nil
}

func (n *Node) Leave() error {
	// Notify the successor that this node is leaving the DHT network (send all resources to the successor)
	// Notify the predecessor that this node is leaving the DHT network
	// Notify the slice leader that this node is leaving the DHT network
}
