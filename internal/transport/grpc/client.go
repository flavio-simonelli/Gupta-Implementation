package grpc

import (
	pb "GuptaDHT/api/gen/node"
	"GuptaDHT/internal/dht"
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrInvalidMaxSize  = errors.New("invalid maximum size for connection pool, must be greater than 0")
	ErrConnectionInUse = errors.New("connection is still in use, cannot remove it")
)

// ConnectionInfo holds information about a gRPC connection to other node.
type ConnectionInfo struct {
	conn     *grpc.ClientConn
	target   string // address:port of receiver
	useCount atomic.Int32
	lastUse  atomic.Int64
}

// increment the use count of the connection
func (ci *ConnectionInfo) acquire() {
	ci.useCount.Add(1)
	ci.lastUse.Store(time.Now().UnixNano()) // update last use time
}

// decrement the use count of the connection
func (ci *ConnectionInfo) release() {
	ci.useCount.Add(-1)
}

// check if the connection is still in use
func (ci *ConnectionInfo) isInUse() bool {
	return ci.useCount.Load() > 0
}

// remove the connection if it is no longer in use
func (ci *ConnectionInfo) remove() error {
	if !ci.isInUse() {
		ci.conn.Close()
	} else {
		return ErrConnectionInUse
	}
	return nil
}

// ConnectionPool manages a pool of gRPC connections to other nodes.
type ConnectionPool struct {
	mu          sync.RWMutex
	connections []*ConnectionInfo
	maxSize     int
}

// NewConnectionPool creates a new ConnectionPool with a specified maximum size.
func NewConnectionPool(maxSize int) (*ConnectionPool, error) {
	if maxSize <= 0 {
		return nil, ErrInvalidMaxSize
	}
	return &ConnectionPool{
		connections: make([]*ConnectionInfo, 0, maxSize),
		maxSize:     maxSize,
	}, nil
}

// getConnection retrieves an existing connection or creates a new one and delete the old connection if the pool is full.
func (p *ConnectionPool) getConnection(target string) (*ConnectionInfo, error) {
	// Acquire lock to ensure thread-safe access to connections
	p.mu.RLock()

	// look for an existing connection
	for _, info := range p.connections {
		if info.target == target && info.conn.GetState() != connectivity.Shutdown {
			// increment the use count of the connection
			info.acquire()
			p.mu.RUnlock() // release the read lock before returning
			return info, nil
		}
	}
	p.mu.RUnlock() // release the read lock before creating a new connection
	// create a new connection
	conn, err := p.createConnection(target)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// createConnection establishes a new gRPC connection and adds it to the pool
func (p *ConnectionPool) createConnection(target string) (*ConnectionInfo, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	// Create a new ConnectionInfo instance
	info := &ConnectionInfo{
		conn:   conn,
		target: target,
	}
	// write lock to ensure exclusive access to the connections slice
	p.mu.Lock()
	// If pool is full, remove oldest connection
	if len(p.connections) >= p.maxSize {
		// find the oldest connection not in use
		oldestNotUseIdx := -1
		newestInUseIdx := -1
		oldestTime := time.Now().UnixNano()
		newestTime := int64(0)
		for i, info := range p.connections {
			if !info.isInUse() && info.lastUse.Load() < oldestTime {
				oldestTime = info.lastUse.Load()
				oldestNotUseIdx = i
			}
			if info.lastUse.Load() > newestTime {
				newestTime = info.lastUse.Load()
				newestInUseIdx = i
			}
		}
		// If we found an old connection, close it and remove it from the pool and add the new connection
		if oldestNotUseIdx != -1 {
			err := p.connections[oldestNotUseIdx].remove()
			if err != nil {
				p.mu.Unlock() // release the write lock before returning
				return nil, err
			}
			p.connections[oldestNotUseIdx] = info // replace with the new connection
		} else {
			// If no old connection not in use was found, wait the newest connection and remove it
			for p.connections[newestInUseIdx].isInUse() {
				// wait for a short time to allow the connection to be released
				time.Sleep(100 * time.Millisecond)
			}
			err := p.connections[newestInUseIdx].remove()
			if err != nil {
				p.mu.Unlock() // release the write lock before returning
				return nil, err
			}
		}
	} else {
		// If the pool is not full, just add the new connection
		p.connections = append(p.connections, info)
	}
	p.mu.Unlock() // release the write lock after modifying the connections slice
	return info, nil
}

// CloseAll closes all connections in the pool.
func (p *ConnectionPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, info := range p.connections {
		if info.conn != nil {
			for info.isInUse() {
				// wait for the connection to be released
				time.Sleep(100 * time.Millisecond)
			}
			err := info.conn.Close()
			if err != nil {
				info.conn.Close() // force close the connection if it is still in use
			}
		}
	}
	p.connections = nil // clear the pool
}

func (p *ConnectionPool) FindSuccessor(ctx context.Context, id dht.ID, addr string) (string, error) {
	fmt.Printf("Searching for successor of ID %s at address %s\n", id.ToHexString(), addr)
	// get a connection to the target node
	info, err := p.getConnection(addr)
	if err != nil {
		return "", err
	}
	defer info.release() // ensure the connection is released after use
	client := pb.NewJoinServiceClient(info.conn)
	// build the request
	req := &pb.FindSuccessorRequest{
		Id: &pb.NodeId{
			NodeId: id.ToHexString(),
		},
	}
	// call the remote method
	resp, err := client.FindSuccessor(ctx, req)
	if err != nil {
		return "", err
	}
	// print the successor address
	fmt.Println("Successor address found:", resp.Address.Address)
	return resp.Address.Address, nil
}

func (p *ConnectionPool) BecomePredecessor(ctx context.Context, id dht.ID, addr string, reciver string) ([]dht.RoutingEntry, string, error) {
	fmt.Printf("Becoming predecessor for ID %s at address %s\n", id.ToHexString(), reciver)
	// get a connection to the target node
	info, err := p.getConnection(reciver)
	if err != nil {
		return nil, "", err
	}
	defer info.release() // ensure the connection is released after use
	client := pb.NewJoinServiceClient(info.conn)
	// build the request
	req := &pb.BecomePredecessorRequest{
		Node: &pb.Node{
			NodeId: &pb.NodeId{
				NodeId: id.ToHexString(),
			},
			Address: &pb.NodeAddress{
				Address: addr,
			},
		},
	}
	stream, err := client.BecomePredecessor(ctx, req)
	if err != nil {
		return nil, "", err
	}
	var entries []dht.RoutingEntry
	var realSucc string
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // end of stream
			}
			return nil, "", err
		}

		// Check which payload type we received
		switch payload := resp.GetPayload().(type) {
		case *pb.BecomePredecessorResponse_RoutingChunk:
			routingChunk := payload.RoutingChunk
			for _, entry := range routingChunk.Entries {
				nodeID, err := dht.IDFromHexString(entry.NodeId)
				if err != nil {
					return nil, "", fmt.Errorf("invalid node ID: %w", err)
				}

				rEntry := dht.RoutingEntry{
					ID:      nodeID,
					Address: entry.Address,
				}
				fmt.Println("Adding entry to routing table:", rEntry.ID.ToHexString(), "at address", rEntry.Address)
				entries = append(entries, rEntry)
			}
		case *pb.BecomePredecessorResponse_ResourceMetadata:
			// Handle resource metadata if needed
			fmt.Println("Received resource metadata")
		case *pb.BecomePredecessorResponse_StoreChunk:
			// Handle store chunk if needed
			fmt.Println("Received store chunk")
		}
	}

	return entries, realSucc, nil
}
