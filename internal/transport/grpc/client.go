package grpc

import (
	pb "GuptaDHT/api/gen/node"
	"GuptaDHT/internal/dht"
	"GuptaDHT/internal/logger"
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrInvalidMaxSize    = errors.New("invalid maximum size for connection pool, must be greater than 0")
	ErrConnectionInUse   = errors.New("connection is still in use, cannot remove it")
	ErrPoolClosed        = errors.New("pool is closed, cannot use it")
	ErrDeadlineExceeded  = errors.New("deadline exceeded, operation timed out")
	ErrServerUnavailable = errors.New("service unavailable, no connection available")
	ErrRedirectSuccessor = errors.New("redirect to another successor node, operation not supported yet")
	ErrRedirect          = errors.New("redirect to another node, operation not supported yet")
)

// ----- ConnectionInfo struct and operations on a single gRPC connection -----

// ConnectionInfo holds information about a gRPC connection to other node.
type ConnectionInfo struct {
	conn     *grpc.ClientConn
	target   string // address:port of receiver
	useCount atomic.Int32
	lastUse  atomic.Int64
	p        *ConnectionPool // pointer to the connection pool that manages this connection
}

// increment the use count of the connection
func (ci *ConnectionInfo) acquire() {
	ci.useCount.Add(1)
	ci.lastUse.Store(time.Now().UnixNano()) // update last use time
}

// decrement the use count of the connection
func (ci *ConnectionInfo) release() error {
	if n := ci.useCount.Add(-1); n < 0 {
		return ErrConnectionInUse
	} else if n == 0 {
		// wake up any waiting goroutines if the use count reaches zero
		ci.p.cond.Broadcast()
	}
	return nil
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

// ----- ConnectionPool struct and operations -----

// ConnectionPool manages a pool of gRPC connections to other nodes.
type ConnectionPool struct {
	mu          sync.RWMutex
	cond        *sync.Cond // condition variable for waiting connections
	connections []*ConnectionInfo
	maxSize     int
	close       bool // for a correct shutdown of the node
}

// NewConnectionPool creates a new ConnectionPool with a specified maximum size.
func NewConnectionPool(maxSize int) (*ConnectionPool, error) {
	if maxSize <= 0 {
		return nil, ErrInvalidMaxSize
	}
	p := &ConnectionPool{
		connections: make([]*ConnectionInfo, 0, maxSize),
		maxSize:     maxSize,
		close:       false, // initially not closed
	}
	p.cond = sync.NewCond(&p.mu) // initialize the condition variable with the pool's mutex
	return p, nil
}

// getConnection retrieves an existing connection or creates a new one and delete the old connection if the pool is full. (Thread-safe)
func (p *ConnectionPool) GetConnection(target string) (*ConnectionInfo, error) {
	// Acquire lock to ensure thread-safe access to connections
	p.mu.Lock()
	defer p.mu.Unlock()
	// Check if the pool is closed
	if p.close {
		return nil, ErrPoolClosed
	}
	// look for an existing connection
	for _, info := range p.connections {
		if info.target == target {
			// increment the use count of the connection
			info.acquire()
			return info, nil
		}
	}
	// create a new connection
	newInfo, err := p.newConnection(target)
	if err != nil {
		return nil, err
	}
	// increment the use count of the new connection
	newInfo.acquire()
	for {
		// if the pool is closed, we cannot add a new connection
		if p.close {
			_ = newInfo.release()
			return nil, ErrPoolClosed
		}
		// add the new connection to the pool
		if len(p.connections) < p.maxSize {
			p.connections = append(p.connections, newInfo)
			return newInfo, nil
		}
		// If the pool is full, we need to remove an old connection
		idx := -1
		oldest := time.Now().UnixNano()
		for i, c := range p.connections {
			if !c.isInUse() && c.lastUse.Load() < oldest {
				oldest = c.lastUse.Load()
				idx = i
			}
		}
		if idx != -1 {
			if err := p.connections[idx].remove(); err != nil {
				return nil, err
			}
			p.connections[idx] = newInfo
			return newInfo, nil
		}
		// if no connection not in use is found, we wait for one to be released
		p.cond.Wait()
	}
}

// createConnection creates a new gRPC connection to the target address and adds it to the pool.
func (p *ConnectionPool) newConnection(target string) (*ConnectionInfo, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	// create a new connection info instance
	info := &ConnectionInfo{
		conn:   conn,
		target: target,
		p:      p,
	}
	return info, nil
}

// Close closes all connections in the pool and waits for them to be idle before shutting down. (Thread-safe)
func (p *ConnectionPool) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.close { // if the pool is already closed, return immediately
		p.mu.Unlock()
		return nil
	}
	p.close = true
	p.cond.Broadcast() // wake up any waiting goroutines
	var firstErr error

	// 1. Attendo che refs==0 per tutte (o ctx.Done)
	for {
		allIdle := true
		for _, c := range p.connections {
			if c.isInUse() {
				allIdle = false
				break
			}
		}
		if allIdle {
			break
		}

		// Attendo con cancellazione ctx‑aware
		waitCh := make(chan struct{})
		go func() {
			p.cond.Wait()
			close(waitCh)
		}()

		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err() // timeout o cancellazione
		case <-waitCh:
		}
		p.mu.Lock()
	}

	// 2. Tutte idle → chiudo e svuoto il vettore
	for _, c := range p.connections {
		if err := c.remove(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.connections = nil
	p.mu.Unlock()
	return firstErr
}

// ----- grpc Handlers for DHT operations -----

// FindSucccessor è la funzione che un nuovo nodo che vuole unirsi alla rete usa per contattare il nodo di bootstrap per conoscere il suo successore.
// Riceve in input l'id del nodo che vuole unirsi alla rete e l'indirizzo del nodo di bootstrap. Viene restituito l'indirizzo del successore del nodo che ha l'id più vicino a quello del nodo che vuole unirsi alla rete o errore
// FindSuccessor finds the successor of a given ID in the DHT network by contacting the target node, return an error E
func (p *ConnectionPool) FindSuccessor(id dht.ID, target string, timeout time.Duration) (string, error) {
	// create a context with timeout for the gRPC call
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel() // ensure the context is cancelled after use
	// parse the id in hex string format
	idHex := id.ToHexString()
	info, err := p.GetConnection(target)
	if err != nil {
		return "", err
	}
	defer func(info *ConnectionInfo) {
		_ = info.release()
	}(info) // ensure the connection is released after use
	// create a gRPC client for the FindSuccessor method
	client := pb.NewJoinServiceClient(info.conn)
	// build the request
	req := &pb.FindSuccessorRequest{
		Id: &pb.NodeId{
			NodeId: idHex,
		},
	}
	// call the remote method
	resp, err := client.FindSuccessor(ctx, req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "", ErrDeadlineExceeded
		}
		if status.Code(err) == codes.Unavailable {
			return "", ErrServerUnavailable
		}
		return "", fmt.Errorf("error finding successor: %w", err)
	}
	// return the successor address
	return resp.Address.Address, nil
}

// Become predecessore è la funzione che un nuovo nodo effettua quando deve entrare nella rete, contatta quello che è il suo successore per ricevere la routing table aggioranta e le risorse che deve mantenere, se non è lui il successore perchè il suo id è più piccolo del vecchio predecessore riceve un messaggio di redirect
// BecomePredecessor sends a join request to the successor node in addr and returns the routing entries received from the successor and the resource
func (p *ConnectionPool) BecomePredecessor(id dht.ID, addr string, sn bool, table *dht.Table, store *dht.Storage, K int, U int, target string) (string, error) {
	// create a context for the gRPC call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel() // ensure the context is cancelled after use
	// get a connection to the target node
	info, err := p.GetConnection(target)
	if err != nil {
		return "", err
	}
	defer func(info *ConnectionInfo) {
		_ = info.release()
	}(info) // ensure the connection is released after use
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
			Supernode: sn,
		},
	}
	// call the remote method
	stream, err := client.BecomePredecessor(ctx, req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "", ErrDeadlineExceeded
		}
		if status.Code(err) == codes.Unavailable {
			return "", ErrServerUnavailable
		}
		if status.Code(err) == codes.FailedPrecondition {
			st, ok := status.FromError(err)
			if ok {
				for _, detail := range st.Details() {
					if redirect, ok := detail.(*pb.RedirectInfo); ok {
						logger.Log.Errorf("Redirecting to successor %s", redirect.Target.Address)
						return redirect.Target.Address, ErrRedirectSuccessor
					}
				}
			}
		}
		return "", fmt.Errorf("error becoming predecessor: %w", err)
	}
	// pipe in corso per ogni risorsa in arrivo
	type xfer struct {
		pw   *io.PipeWriter
		done chan error
	}
	filePipe := make(map[string]*xfer)

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("recv: %w", err)
		}

		switch pay := msg.Payload.(type) {

		//------------------------------------------------------------------
		case *pb.BecomePredecessorResponse_RoutingChunk:
			for _, entry := range pay.RoutingChunk.Entries {
				// Convert the NodeId from hex string to dht.ID
				id, err := dht.IDFromHexString(entry.NodeId)
				if err != nil {
					return "", fmt.Errorf("invalid node ID %s: %w", entry.NodeId, err)
				}
				err = table.AddEntry(id, entry.Address, entry.IsSupernode, entry.IsUnitLeader, entry.IsSliceLeader, K, U)
				if err != nil {
					return "", fmt.Errorf("error adding routing entry: %w", err)
				}
			}

		//------------------------------------------------------------------
		case *pb.BecomePredecessorResponse_ResourceMetadata:
			meta := pay.ResourceMetadata

			pr, pw := io.Pipe()
			done := make(chan error, 1)
			go func() { // salva su disco
				done <- store.PutStream(meta.Name, pr)
			}()
			filePipe[meta.Key] = &xfer{pw: pw, done: done}

		//------------------------------------------------------------------
		case *pb.BecomePredecessorResponse_StoreChunk:
			ch := pay.StoreChunk
			x, ok := filePipe[ch.ResourceKey]
			if !ok {
				return "", fmt.Errorf("chunk for unknown %s", ch.ResourceKey)
			}

			if _, err := x.pw.Write(ch.Data); err != nil {
				x.pw.CloseWithError(err)
				return "", err
			}
			if ch.Eof {
				x.pw.Close()
				if err := <-x.done; err != nil {
					return "", err
				}
				delete(filePipe, ch.ResourceKey)
			}

		default:
			return "", fmt.Errorf("unknown payload %T", pay)
		}
	}

	if len(filePipe) != 0 {
		return "", fmt.Errorf("stream ended but %d file incomplete", len(filePipe))
	}
	return "", nil
}

// NotifyPredecessor notifies the predecessor node about the new successor node.
func (p *ConnectionPool) NotifyPredecessor(id dht.ID, addr string, sn bool, target string) (string, error) {
	// create a context for the gRPC call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel() // ensure the context is cancelled after use
	// get a connection to the target node
	info, err := p.GetConnection(target)
	if err != nil {
		return "", err
	}
	defer func(info *ConnectionInfo) {
		_ = info.release()
	}(info) // ensure the connection is released after use
	client := pb.NewJoinServiceClient(info.conn)
	// build the request
	req := &pb.NotifyPredecessorRequest{
		NewSuccessor: &pb.Node{
			NodeId: &pb.NodeId{
				NodeId: id.ToHexString(),
			},
			Address: &pb.NodeAddress{
				Address: addr,
			},
			Supernode: sn,
		},
	}
	// call the remote method
	_, err = client.NotifyPredecessor(ctx, req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "", ErrDeadlineExceeded
		}
		if status.Code(err) == codes.Unavailable {
			return "", ErrServerUnavailable
		}
		if status.Code(err) == codes.FailedPrecondition {
			st, ok := status.FromError(err)
			if ok {
				for _, detail := range st.Details() {
					if redirect, ok := detail.(*pb.RedirectInfo); ok {
						logger.Log.Errorf("Redirecting to predecessor %s", redirect.Target.Address)
						return redirect.Target.Address, ErrRedirect
					}
				}
			}
		}
		return "", fmt.Errorf("error notifying predecessor: %w", err)
	}
	return "", nil
}
