package grpcclient

import (
	"errors"
	"google.golang.org/grpc"
	"sync/atomic"
	"time"
)

var (
	// ErrConnectionInUse error indicates that the connection is still in use and cannot be removed.
	ErrConnectionInUse = errors.New("connection is still in use, cannot remove it")
)

// ConnectionInfo holds information about a gRPC connection to other node.
type ConnectionInfo struct {
	conn     *grpc.ClientConn // gRPC connection to the target node
	useCount atomic.Int32     // number of goroutine currently using this connection
	lastUse  atomic.Int64     // last time this connection was used, in nanoseconds (update on acquire)
	p        *ConnectionPool  // pointer to the connection pool that manages this connection for notify if it is no longer in use
}

// increment the use count of the connection
func (ci *ConnectionInfo) acquire() {
	ci.useCount.Add(1)                      // increment the use count
	ci.lastUse.Store(time.Now().UnixNano()) // update last use time
}

// decrement the use count of the connection
func (ci *ConnectionInfo) release() {
	if ci.useCount.Add(-1) == 0 { // if the use count reaches zero
		ci.p.cond.Broadcast() // notify the connection pool that this connection is no longer in use
	}
}

// check if the connection is still in use
func (ci *ConnectionInfo) isInUse() bool {
	return ci.useCount.Load() > 0
}

// remove the connection if it is no longer in use
func (ci *ConnectionInfo) remove() error {
	if ci.isInUse() {
		return ErrConnectionInUse
	}
	return ci.conn.Close()
}
