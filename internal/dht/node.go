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
	//FindSuccessor(id ID, addr string, timeout time.Duration) (string, error)
	// BecomePredecessor sends a join request to the successor node in addr
	//BecomePredecessor(id ID, addr string, sn bool, table *Table, store *Storage, K int, U int, target string) (string, error)
	// NotifyPredecessor notifies the predecessor node in addr that this node is its successor
	//NotifyPredecessor(id ID, addr string, sn bool, target string) (string, error)
	// Notify notifies a nod one or more events
	//Notify(ctx context.Context, addr string, event string) error
	// per adesso ci fermiamo alla join
}

type Node struct {
	ID                 ID                // Unique identifier for the node
	Addr               string            // Address of the server in this node (ip:port)
	T                  *Table            // Routing table for the node
	Supernode          bool              // true if this node is a supernode
	UnitLeader         bool              // true if this node is a unit leader
	SliceLeader        bool              // true if this node is a slice leader
	net                Communicator      // Communicator interface for network operations (client)
	MainStore          *Storage          // Storage for the node
	PredecessorStorage *Storage          // Storage for the predecessor node. Is a 1 backup of the resources
	StandardBoard      *NormalBoard      // Board of events for normal nodes (used to send events to the slice leader)
	LeaderBoard        *SliceLeaderBoard //  board of events use only if the node is a slice leader
}

// NewNode creates a new Node with the given ID, address, and communicator and initializes empty routing tables.
func NewNode(id ID, ip string, port int, sn bool, client Communicator, mainStorage *Storage, predecessorStorage *Storage, standardBoard *NormalBoard) *Node {
	addr := fmt.Sprintf("%s:%d", ip, port)
	return &Node{
		ID:                 id,
		Addr:               addr,
		T:                  NewTable(),
		Supernode:          sn,
		UnitLeader:         false,
		SliceLeader:        false,
		net:                client,
		MainStore:          mainStorage,
		PredecessorStorage: predecessorStorage,
		StandardBoard:      standardBoard,
	}
}

// Join is the method that allows a node to join an existing DHT network.
func (n *Node) Join(bootstrap string) error {
	// if the bootstrap string is empty, it means that the node is the first node in the DHT network
	if bootstrap == "" {
		return fmt.Errorf("%w: bootstrap string is empty", ErrCreateDHTNetwork)
	}
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
	for temp, err := n.net.BecomePredecessor(n.ID, n.Addr, n.Supernode, n.T, n.MainStore, n.K, n.U, target); err != nil; {
		if !errors.Is(err, ErrRedirectSucc) {
			return fmt.Errorf("%w: %w", ErrContactSucc, err)
		}
		target = temp
		logger.Log.Warnf("Redirected to another successor node, retrying with new target: %s", target)
	}
	return nil
}

/*
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
		for temp, err := n.net.BecomePredecessor(n.ID, n.Addr, n.Supernode, n.T, n.MainStore, n.K, n.U, target); err != nil; {
			if !errors.Is(err, ErrRedirectSucc) {
				return fmt.Errorf("%w: %w", ErrContactSucc, err)
			}
			target = temp
			logger.Log.Warnf("Redirected to another successor node, retrying with new target: %s", target)
		}
		//TODO: check if the node have a slice leader and unit leader, if not, set this node as slice leader and unit leader contattando gli sltri slice leader o il solo slice leader sevuole diventare unit leader
		// set successor in the routing table
		succ, _, err := n.T.FindSuccessor(n.ID.Next())
		if err != nil {
			logger.Log.Errorf("Error Find successor for set successor during the join: %v", err)
			return fmt.Errorf("error finding successor for set successor during the join: %w", err)
		}
		if succ.Address != target {
			logger.Log.Errorf("Expected successor address %s, but got %s during set successor", target, succ.Address)
			return fmt.Errorf("expected successor address %s, but got %s during set successor", target, succ.Address)
		}
		for old, err := n.T.ChangeSuccessor(succ.ID, succ.Address, false, n.K, n.U); err != nil; {
			if !errors.Is(err, ErrRedirectSucc) {
				return fmt.Errorf("error changing successor node to successor during the join: %w", err)
			}
			succ, _, err = n.T.FindSuccessor(n.ID.Next())
			if err != nil {
				logger.Log.Errorf("Error Find successor for set successor during the join: %v", err)
				return fmt.Errorf("error finding successor for set successor during the join: %w", err)
			}
			if succ.Address != old {
				logger.Log.Warnf("Redirected to another successor node, retrying with new target: %s", succ.Address)
				continue
			}
			break
		}
		// contact the predecessor node to notify it that this node is its successor
		pred, _, err := n.T.FindPredecessor(n.ID)
		if err != nil {
			logger.Log.Errorf("Error finding predecessor during the join: %v", err)
			return fmt.Errorf("error finding predecessor during the join: %w", err)
		}
		for temp, err := n.net.NotifyPredecessor(n.ID, n.Addr, n.Supernode, pred.Address); err != nil; {
			if !errors.Is(err, ErrRedirectSucc) {
				return fmt.Errorf("%w: %w", ErrContactSucc, err)
			}
			target = temp
			logger.Log.Warnf("Redirected to another successor node, retrying with new target: %s", target)
		}
		// set the predecessor in the routing table
		// notify the slice leader that this node is joined the DHT network
	}
	return nil
}

func (n *Node) Leave() error {
	// Notify the successor that this node is leaving the DHT network (send all resources to the successor)
	// Notify the predecessor that this node is leaving the DHT network
	// Notify the slice leader that this node is leaving the DHT network
}

func (n *Node)GetResources() ()

*/
