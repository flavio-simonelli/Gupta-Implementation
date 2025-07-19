package grpcclient

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"sync/atomic"
)

var (
	ErrInvalidMaxSize   = errors.New("invalid max size for connection pool")
	ErrConnectionClosed = errors.New("connection pool is closed")
)

// ConnectionPool manages gRPC connections to peers.
type ConnectionPool struct {
	mu          sync.RWMutex               // mutex to protect access to the connections map
	cond        *sync.Cond                 // condition variable to signal when connections are available
	connections map[string]*ConnectionInfo // map of connections indexed by target address (ip:port)
	maxSize     int                        // maximum number of connections allowed in the pool
	closed      atomic.Bool                // atomic boolean to indicate if the pool is closed
}

// NewConnectionPool creates a new pool with max number of allowed connections.
func NewConnectionPool(maxSize int) (*ConnectionPool, error) {
	if maxSize <= 0 {
		return nil, ErrInvalidMaxSize
	}
	pool := &ConnectionPool{
		connections: make(map[string]*ConnectionInfo),
		maxSize:     maxSize,
	}
	pool.cond = sync.NewCond(&pool.mu) // initialize the condition variable with the pool's mutex
	return pool, nil
}

// GetConnection returns a connection to the given target, reusing existing ones.
func (p *ConnectionPool) GetConnection(ctx context.Context, target string) (*ConnectionInfo, error) {
	if p.closed.Load() {
		// If the pool is closed, return an error
		return nil, ErrConnectionClosed
	}
	// check if the target is already in the connections map
	p.mu.RLock() // lock read access to the connections map
	// If connection exists, reuse it
	if ci, ok := p.connections[target]; ok {
		ci.acquire()
		p.mu.RUnlock() // release read lock
		return ci, nil
	}
	p.mu.RUnlock() // release read lock
	// If connection does not exist, we need to create a new one
	p.mu.Lock()         // lock write access to the connections map
	defer p.mu.Unlock() // ensure the write lock is released after this function
	// Check if the pool is closed again after acquiring the write lock
	if p.closed.Load() {
		return nil, ErrConnectionClosed
	}
	// Check again if the connection was added while waiting for the lock
	if ci, ok := p.connections[target]; ok {
		ci.acquire()
		return ci, nil
	}
	// If at capacity, wait for available slot
	for len(p.connections) >= p.maxSize {
		waitCh := make(chan struct{})
		go func() {
			p.cond.Wait()
			close(waitCh) // signal that a connection has been released
		}()
		p.mu.Unlock() // release the write lock to allow other goroutines to proceed
		select {
		case <-ctx.Done():
			// timeout or cancellation from the context
			return nil, ctx.Err()
		case <-waitCh:
			// woke up because a connection was released
		}
		p.mu.Lock()
		// Re-check the pool close status
		if p.closed.Load() {
			return nil, ErrConnectionClosed
		}
		// check if there are the connection requested
		if ci, ok := p.connections[target]; ok {
			ci.acquire()
			p.mu.Unlock() // release write lock before returning
			return ci, nil
		}
		// delete an old connection if it is not in use
		for t, ci := range p.connections {
			if !ci.isInUse() {
				_ = ci.remove()
				delete(p.connections, t)
				break
			}
		}
	}
	// Create a new connection
	newConn, err := p.newConnection(target)
	if err != nil {
		return nil, err
	}
	p.connections[target] = newConn
	newConn.acquire()
	return newConn, nil
}

// newConnection initializes a gRPC connection to a peer.
func (p *ConnectionPool) newConnection(target string) (*ConnectionInfo, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	ci := &ConnectionInfo{
		conn: conn,
		p:    p,
	}
	return ci, nil
}

// ReleaseConnection must be called when done using a connection.
func (p *ConnectionPool) ReleaseConnection(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if ci, ok := p.connections[target]; ok {
		ci.release()
	}
}

// Shutdown closes all connections and prevents further use.
func (p *ConnectionPool) Shutdown(ctx context.Context) error {
	// Mark the pool as closed to prevent new connections
	p.closed.Store(true)

	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		allFree := true
		for _, ci := range p.connections {
			if ci.isInUse() {
				allFree = false
				break
			}
		}

		if allFree {
			// All connections are free, safe to proceed
			break
		}

		// If context done, exit waiting
		select {
		case <-ctx.Done():
			goto CLOSE
		default:
		}

		// Wait until a connection is released or pool closed
		waitCh := make(chan struct{})
		go func() {
			p.cond.Wait()
			close(waitCh)
		}()

		p.mu.Unlock()

		select {
		case <-ctx.Done():
			p.mu.Lock()
			goto CLOSE
		case <-waitCh:
			p.mu.Lock()
		}
	}

CLOSE:
	// Close all connections regardless of usage state after timeout or all free
	for _, ci := range p.connections {
		_ = ci.conn.Close()
	}
	p.connections = make(map[string]*ConnectionInfo)
	// Wake up all goroutines waiting on cond.Wait()
	p.cond.Broadcast()

	return ctx.Err()
}
