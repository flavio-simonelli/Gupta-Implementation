package dht

import (
	"GuptaDHT/internal/logger"
	"context"
	"errors"
	"fmt"
)

var (
	ErrCreateDHTNetwork = errors.New("error creating DHT network")
	ErrContactBootstrap = errors.New("error contacting bootstrap node")
	ErrContactSucc      = errors.New("error contacting successor node")
	ErrRedirectSucc     = errors.New("redirect to another successor")
)

type Communicator interface {
	// FindSuccessor ask the node in addr for the successor of the given ID
	FindSuccessor(ctx context.Context, id ID, addr string) (string, error)
	// BecomePredecessor sends a join request to the successor node in addr
	BecomePredecessor(ctx context.Context, id ID, addr string, recive string) ([]RoutingEntry, string, error) // per adesso perchè mancano i file
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
}

// NewNode creates a new Node with the given ID, address, and communicator and initializes empty routing tables.
func NewNode(u int, k int, idHex string, addr string, client Communicator, sn bool) (*Node, error) {
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
	}, nil
}

func (n *Node) Join(bootstrap string) error {
	// if bootstrap is empty, create a new dht network
	if bootstrap == "" {
		// create a new dht network
		fmt.Println("Creating a new DHT network")
		// insert the node itself in the table insert in slice leader and unit leader
		me := RoutingEntry{
			ID:      n.ID,
			Address: n.Addr,
		}
		err := n.T.AddEntry(me, false, true, true)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrCreateDHTNetwork, err)
		}
		// test
		prova, _ := n.T.FindSuccessor(me.ID)
		fmt.Println("trovato nella routing table il successore di me:", prova.ID)
	} else {
		// join an existing dht network
		fmt.Println("Joining an existing DHT network contacting node", bootstrap)
		// ask the bootstrap node for my successor
		addrSucc, err := n.net.FindSuccessor(context.Background(), n.ID, bootstrap)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrContactBootstrap, err)
		}
		// contact the successor node to become its predecessor
		fmt.Println("Joining an existing DHT network contacting node", addrSucc)
		// Declare variables before the loop
		var entries []RoutingEntry
		var realSucc string
		var joinErr error

		// Initial call to BecomePredecessor
		entries, realSucc, joinErr = n.net.BecomePredecessor(context.Background(), n.ID, n.Addr, addrSucc)

		// Handle redirects and errors
		for joinErr != nil {
			if errors.Is(joinErr, ErrRedirectSucc) {
				fmt.Println("Redirecting to another successor:", realSucc)
				addrSucc = realSucc
				entries, realSucc, joinErr = n.net.BecomePredecessor(context.Background(), n.ID, n.Addr, addrSucc)
			} else {
				return fmt.Errorf("%w: %w", ErrContactSucc, joinErr)
			}
		}

		// Now entries is in scope for this loop
		for _, entry := range entries {
			fmt.Println("Adding entry to routing table:", entry.ID.ToHexString(), "at address", entry.Address)
			err = n.T.AddEntry(entry, false, false, false)
			if err != nil {
				return fmt.Errorf("error adding entry to routing table: %w", err)
			}
		}

	}
	return nil
}
