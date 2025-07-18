package node

import (
	"GuptaDHT/internal/dht/event"
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/dht/routingtable"
	"GuptaDHT/internal/dht/storage"
	"GuptaDHT/internal/logger"
	"errors"
	"fmt"
	"io"
)

var (
	ErrCreateDHTNetwork = errors.New("error creating DHT network")
	ErrContactBootstrap = errors.New("error contacting bootstrap node")
	ErrContactSucc      = errors.New("error contacting successor node")
	ErrResourceNotFound = errors.New("resource not found")
)

type Communicator interface {
	FindPredecessor(id id.ID, target string) (id.ID, string, bool, error)
	BecomeSuccessor(id id.ID, addr string, sn bool, target string) (id.ID, string, bool, error)
	BecomePredecessor(id id.ID, addr string, sn bool, table *routingtable.Table, store *storage.Storage, target string) error
	// GetResource retrieves a resource from another node by ID and filename
	GetResource(resourceID id.ID, filename string, targetAddr string) (*storage.MetadataResource, func() ([]byte, error), error)
	// Notify notifies a nod one or more events
	//Notify(ctx context.Context, addr string, event string) error
	// per adesso ci fermiamo alla join
}

type Node struct {
	ID                 id.ID                        // Unique identifier for the node
	Addr               string                       // Address of the server in this node (ip:port)
	T                  *routingtable.Table          // Routing table for the node
	Supernode          bool                         // true if this node is a supernode
	UnitLeader         bool                         // true if this node is a unit leader
	SliceLeader        bool                         // true if this node is a slice leader
	net                Communicator                 // Communicator interface for network operations (client)
	MainStore          *storage.Storage             // Storage for the node
	PredecessorStorage *storage.Storage             // Storage for the predecessor node. Is a 1 backup of the resources
	StandardBoard      *event.EventDispatcher       // Board of events for normal nodes (used to send events to the slice leader)
	LeaderBoard        *event.SliceLeaderDispatcher //  board of events use only if the node is a slice leader
}

// NewNode creates a new Node with the given ID, address, and communicator and initializes empty routing tables.
func NewNode(id id.ID, ip string, port int, sn bool, client Communicator, mainStorage *storage.Storage, predecessorStorage *storage.Storage, standardBoard *event.EventDispatcher) *Node {
	addr := fmt.Sprintf("%s:%d", ip, port)
	return &Node{
		ID:                 id,
		Addr:               addr,
		T:                  routingtable.NewTable(),
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
	logger.Log.Infof("Node %s is trying to join the DHT network", n.ID.ToHexString())
	// find the predecessor of the node in the DHT network
	predID, predAddr, predSn, err := n.net.FindPredecessor(n.ID, bootstrap)
	if err != nil {
		logger.Log.Errorf("Node %s failed to find predecessor: %v", n.ID.ToHexString(), err)
		return fmt.Errorf("%w: %v", ErrContactBootstrap, err)
	}
	logger.Log.Infof("Node %s found predecessor %s at address %s", n.ID.ToHexString(), predID.ToHexString(), predAddr)
	// insert the predecessor in the routing table
	err = n.T.AddEntry(predID, predAddr, predSn, false, false)
	if err != nil {
		logger.Log.Errorf("Node %s failed to add predecessor to routing table: %v", n.ID.ToHexString(), err)
		return err
	}
	// become the successor of the predecessor
	succID, succAddr, succSn, err := n.net.BecomeSuccessor(n.ID, n.Addr, n.Supernode, predAddr)
	if err != nil {
		for errors.Is(err, routingtable.ErrSuccRedirect) {
			logger.Log.Infof("Node %s is being redirected to another successor: %v", n.ID.ToHexString(), err)
			// retry with the new successor address
			succID, succAddr, succSn, err = n.net.BecomeSuccessor(n.ID, n.Addr, n.Supernode, predAddr)
		}
		if err != nil {
			logger.Log.Errorf("Node %s failed to become successor: %v", n.ID.ToHexString(), err)
			return fmt.Errorf("%w: %v", ErrContactSucc, err)
		}
	}
	// add the successor to the routing table
	err = n.T.AddEntry(succID, succAddr, succSn, false, false)
	if err != nil {
		logger.Log.Errorf("Node %s failed to add successor to routing table: %v", n.ID.ToHexString(), err)
	}
	// become the predecessor of the successor
	n.net.BecomePredecessor(n.ID, n.Addr, n.Supernode, n.T, n.MainStore, succAddr)
	logger.Log.Infof("Node %s successfully joined the DHT network as successor of %s", n.ID.ToHexString(), succID.ToHexString())
	return nil
}

// CreateDHT creates a new DHT network.
func (n *Node) CreateDHT() error {
	// add the node in the routing table
	if err := n.T.AddEntry(n.ID, n.Addr, n.Supernode, true, true); err != nil {
		logger.Log.Errorf("Node %s failed to add itself to routing table: %v", n.ID.ToHexString(), err)
		return fmt.Errorf("%w: %v", ErrCreateDHTNetwork, err)
	}
	logger.Log.Infof("Node %s created a new DHT network", n.ID.ToHexString())
	return nil
}

// GetResource retrieves a resource from the storage by its ID.
// If the resource is not in this node, it forwards the request to the successor.
// Returns the resource metadata and a function to read chunks of the resource.
func (n *Node) GetResource(filename string) (*storage.MetadataResource, func() ([]byte, error), error) {
	resourceID := id.StringToID(filename)
	logger.Log.Debugf("Node %s: Request to get resource %s", n.ID.ToHexString(), resourceID.ToHexString())
	// Check if the resource ID is between the current node's ID and its predecessor's ID
	// or if this node is the only node in the DHT
	isResponsible := false
	if  == n.ID { // This is the only node in the DHT
		isResponsible = true
	} else if Between(n.PredID, n.ID, resourceID, true) {
		isResponsible = true
	}

	if isResponsible {
		// The resource should be in this node
		logger.Log.Debugf("Node %s: Resource %s should be in this node", n.ID.ToHexString(), resourceID.ToHexString())

		// Try to get metadata and open the file
		metadata, exists := n.MainStore.GetMetadata(resourceID)
		if !exists {
			return nil, nil, ErrResourceNotFound
		}

		file, err := n.MainStore.OpenFile(resourceID)
		if err != nil {
			return nil, nil, err
		}

		// Return the metadata and a function to read chunks
		chunkReader := func() ([]byte, error) {
			chunk, err := storage.ReadChunkFile(file, storage.FileChunkSize)
			if err == io.EOF {
				// When we reach EOF, close the file and release the resource lock
				n.MainStore.CloseFile(file, metadata)
				return nil, io.EOF
			}
			if err != nil {
				// On error, close the file and release the resource lock
				n.MainStore.CloseFile(file, metadata)
				return nil, err
			}
			return chunk, nil
		}

		return metadata, chunkReader, nil
	}

	// The resource is not in this node, forward the request to the successor
	logger.Log.Debugf("Node %s: Resource %s not in this node, forwarding to successor", n.ID.ToHexString(), resourceID.ToHexString())

	// Get successor info
	succID, succAddr, _, err := n.T.GetEntry(0)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get successor: %w", err)
	}

	// Forward the request to the successor
	metadata, chunkReader, err := n.net.GetResource(resourceID, filename, succAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get resource from successor %s: %w", succID.ToHexString(), err)
	}

	return metadata, chunkReader, nil
}
