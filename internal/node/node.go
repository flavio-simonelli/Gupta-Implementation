package node

import (
	"GuptaDHT/internal/dht/event"
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/dht/keepalive"
	"GuptaDHT/internal/dht/routingtable"
	"GuptaDHT/internal/logger"
	"GuptaDHT/internal/transport/grpc/grpcclient"
	"fmt"
	"time"
)

type Node struct {
	T                *routingtable.Table     // Routing table for the node
	kpSuccessor      *keepalive.KeepAlive    // struct to manage the successor node's keep-alive
	client           grpcclient.NodeClient   // gRPC client for communication with other nodes
	EventBoard       *event.EventBoard       // Event board for dispatching events
	SliceLeaderBoard *event.SliceLeaderBoard // Event board for slice leader
}

// NewNode creates a new Node
func NewNode(selfID id.ID, selfAddr string, selfSN bool, clientNode grpcclient.NodeClient, kpSender grpcclient.KeepAliveSender, kpInterval time.Duration, eventSender event.EventSender, retryEventInterval, tBig, tWait time.Duration) (*Node, error) {
	// Create a new table for the node
	table, err := routingtable.NewTable(selfID, selfAddr, selfSN)
	if err != nil {
		return nil, fmt.Errorf("failed to create routing table: %w", err)
	}
	// Initialize the event board
	board := event.NewEventBoard(eventSender, table, retryEventInterval, tBig, tWait)
	// Create a new Node with the routing table
	node := &Node{
		T:           table,
		kpSuccessor: keepalive.InitializeKeepAlive(kpSender, table, kpInterval),
		client:      clientNode,
		EventBoard:  board,
	}
	return node, nil
}

// Join is the method to join the DHT network
func (n *Node) Join(bootstrapAddr string) error {
	// request the bootstrap node to find my predecessor
	predecessor, err := n.client.FindPredecessor(n.T.GetSelf().ID, bootstrapAddr)
	if err != nil {
		return fmt.Errorf("failed to find predecessor: %w", err)
	}
	// log the predecessor found
	logger.Log.Infof("Predecessor found: %s at %s", predecessor.ID.ToHexString(), predecessor.Address)

	// become the successor of the predecessor
	successor, err := n.client.BecomeSuccessor(n.T.GetSelf().ID, n.T.GetSelf().Address, n.T.GetSelf().IsSN, predecessor.Address)
	if err != nil {
		return fmt.Errorf("failed to become successor: %w", err)
	}
	// log the successor found
	logger.Log.Infof("Successor found: %s at %s", successor.ID.ToHexString(), successor.Address)
	return nil
}

// CreateNetwork creates a new DHT network by becoming the first node
func (n *Node) CreateNetwork() error {
	// Set the predecessor and successor to self
	logger.Log.Infof("setting self as predecessor for node %s", n.T.GetSelf().ID.ToHexString())
	err := n.T.SetPredecessor(n.T.GetSelf().ID)
	if err != nil {
		return err
	}
	// Set the successor to self
	logger.Log.Infof("setting self as successor for node %s", n.T.GetSelf().ID.ToHexString())
	err = n.T.SetSuccessor(n.T.GetSelf().ID)
	if err != nil {
		return err
	}
	// become the unit leader
	err = n.T.BecomeUnitLeader(n.T.GetSelf().ID)
	if err != nil {
		return fmt.Errorf("failed to become unit leader: %w", err)
	}
	// become the slice leader
	err = n.T.IBecomeSliceLeader()
	if err != nil {
		return fmt.Errorf("failed to become slice leader: %w", err)
	}
	// initialize the slice leader board
	n.EventBoard.InitializeSliceLeaderBoard()
	// Log the creation of the network
	logger.Log.Infof("Node %s created a new DHT network at %s", n.T.GetSelf().ID.ToHexString(), n.T.GetSelf().Address)
	return nil
}
