package grpcclient

import (
	pb "GuptaDHT/api/gen/node"
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/dht/routingtable"
	"GuptaDHT/internal/logger"
	"context"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"time"
)

var (
	// ErrRedirected is returned when the node is redirected to a different node.
	ErrRedirected = fmt.Errorf("redirected to a different node")
)

type NodeClient interface {
	// FindPredecessor sends a request to find the predecessor of the node with the given identity.
	FindPredecessor(identity id.ID, receiver string) (*routingtable.PublicEntry, error)
	// BecomeSuccessor sends a request to become the successor of the node with the given identity.
	BecomeSuccessor(identity id.ID, address string, sn bool, receiver string) (*routingtable.PublicEntry, error)
}

// KeepAliveSender is the interface responsible for sending keep-alive messages to the successor.
type KeepAliveSender interface {
	// SendKeepAlive sends a keep-alive message to the specified receiver.
	SendKeepAlive(receiver string) error
}

// getConnectionWithTimeout retrieves a connection from the pool with a specified timeout.
func (p *ConnectionPool) getConnectionWithTimeout(target string) (*ConnectionInfo, error) {
	ctx := context.Background()
	timeout := 2 * time.Second // default timeout
	Ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := p.GetConnection(Ctx, target)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (p *ConnectionPool) SendKeepAlive(receiver string) error {
	//TODO implement me
	panic("implement me")
}

func (p *ConnectionPool) FindPredecessor(identity id.ID, receiver string) (*routingtable.PublicEntry, error) {
	conn, err := p.getConnectionWithTimeout(receiver)
	if err != nil {
		return nil, err
	}
	defer p.ReleaseConnection(receiver)
	// make the client
	joinClient := pb.NewJoinServiceClient(conn.client)
	// make the request
	ctx := context.Background()
	md := metadata.Pairs("node-id", identity.ToHexString())
	ctx = metadata.NewOutgoingContext(ctx, md)

	resp, err := joinClient.FindPredecessor(ctx, &emptypb.Empty{})
	if err != nil {
		logger.Log.Infof("could not call FindPredecessor: %v", err)
		return nil, err
	}
	predId, err := id.FromHexString(resp.Node.NodeId)
	if err != nil {
		logger.Log.Errorf("could not parse node ID from response: %v", err)
		return nil, err
	}
	predecessor := routingtable.NewPublicEntry(predId, resp.Node.Address, resp.Supernode, false, false)
	logger.Log.Infof("Predecessor node: %v", resp)
	return &predecessor, nil
}

// BecomeSuccessor sends a request to become the successor of the node with the given identity.
func (p *ConnectionPool) BecomeSuccessor(identity id.ID, address string, sn bool, receiver string) (*routingtable.PublicEntry, error) {
	conn, err := p.getConnectionWithTimeout(receiver)
	if err != nil {
		return nil, err
	}
	defer p.ReleaseConnection(receiver)
	// make the client
	joinClient := pb.NewJoinServiceClient(conn.client)
	// make the request
	ctx := context.Background()
	md := metadata.Pairs("node-id", identity.ToHexString())
	ctx = metadata.NewOutgoingContext(ctx, md)
	// create the node info to send
	in := &pb.BecomeSuccessorRequest{
		NewSuccessor: &pb.NodeInfo{
			Node: &pb.Node{
				NodeId:  identity.ToHexString(),
				Address: address,
			},
			Supernode: sn,
		},
	}
	resp, err := joinClient.BecomeSuccessor(ctx, in)
	if err != nil {
		logger.Log.Infof("error when call BecomeSuccessor: %v", err)
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.FailedPrecondition {
			for _, detail := range st.Details() {
				switch info := detail.(type) {
				case *pb.RedirectInfo:
					logger.Log.Infof("Redirected to real predecessor: %v", info.Target)
					// create a public entry for the redirect target
					redirectId, err := id.FromHexString(info.Target.Node.NodeId)
					if err != nil {
						logger.Log.Errorf("could not parse redirect node ID: %v", err)
						return nil, fmt.Errorf("invalid redirect node ID: %w", err)
					}
					redirectEntry := routingtable.NewPublicEntry(redirectId, info.Target.Node.Address, info.Target.Supernode, false, false)
					return &redirectEntry, ErrRedirected
				default:
					logger.Log.Warnf("Unknown error detail: %v", detail)
				}
			}
		}
		logger.Log.Errorf("could not call BecomeSuccessor: %v", err)
		return nil, err
	}
	succId, err := id.FromHexString(resp.Node.NodeId)
	if err != nil {
		logger.Log.Errorf("could not parse node ID from response: %v", err)
		return nil, err
	}
	successor := routingtable.NewPublicEntry(succId, resp.Node.Address, resp.Supernode, false, false)
	logger.Log.Infof("Successor node: %v", resp)
	return &successor, nil
}
