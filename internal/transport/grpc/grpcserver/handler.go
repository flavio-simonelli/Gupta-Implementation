package grpcserver

import (
	pb "GuptaDHT/api/gen/node"
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/dht/routingtable"
	"GuptaDHT/internal/logger"
	"GuptaDHT/internal/node"
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Server implements all gRPC services for a DHT node
type Server struct {
	node *node.Node
	pb.UnimplementedDisseminationServiceServer
	pb.UnimplementedStorageServiceServer
	pb.UnimplementedJoinServiceServer
	pb.UnimplementedLeaveServiceServer
}

// NewServer constructs a Server that handles multiple gRPC services
func NewServer(n *node.Node) *Server {
	return &Server{node: n}
}

// FindPredecessor is a gRPC method that returns the predecessor of the id sender
func (s *Server) FindPredecessor(ctx context.Context, _ *emptypb.Empty) (*pb.NodeInfo, error) {
	// extract metadata from the context
	var senderId id.ID
	var err error
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ids := md["node-id"]; len(ids) > 0 {
			logger.Log.Infof("FindPredecessor called by node-id: %s", ids[0])
			senderId, err = id.FromHexString(ids[0])
			if err != nil {
				logger.Log.Errorf("Invalid node ID from metadata: %v", err)
				return nil, err
			}
		}
	}
	// search the predecessor in the routing table
	predecessor, err := s.node.T.GetPredecessor(senderId)
	if err != nil {
		return nil, err
	}
	// Log the predecessor found
	logger.Log.Infof("Predecessor found for node %s: %s", predecessor.ID.ToHexString(), predecessor.Address)
	// return the predecessor node info
	return &pb.NodeInfo{
		Node: &pb.Node{
			NodeId:  predecessor.ID.ToHexString(),
			Address: predecessor.Address,
		},
		Supernode: predecessor.IsSN,
	}, nil
}

// BecomeSuccessor is a gRPC method that allows a node to become the new successor
func (s *Server) BecomeSuccessor(ctx context.Context, in *pb.BecomeSuccessorRequest) (*pb.NodeInfo, error) {
	// check the request for the new successor node info
	if in.NewSuccessor == nil || in.NewSuccessor.Node == nil {
		return nil, status.Errorf(codes.InvalidArgument, "NewSuccessor missing in request")
	}
	senderId, err := id.FromHexString(in.NewSuccessor.Node.NodeId)
	if err != nil {
		logger.Log.Errorf("Invalid node ID from request: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid node ID: %v", err)
	}
	// check if the new successor node ID is less than the sender ID
	currentSucc, err := s.node.T.GetMySuccessor()
	if err != nil || currentSucc.ID.Equals(s.node.T.GetSelf().ID) || !senderId.IsOwnedBy(s.node.T.GetSelf().ID, currentSucc.ID) {
		logger.Log.Errorf("New successor ID %s is not greater than current successor ID %s", senderId.ToHexString(), currentSucc.ID.ToHexString())
		// return the redirect info
		redirect := &pb.RedirectInfo{
			Target: &pb.NodeInfo{
				Node: &pb.Node{
					NodeId:  currentSucc.ID.ToHexString(),
					Address: currentSucc.Address,
				},
				Supernode: currentSucc.IsSN,
			},
		}
		detail, err := anypb.New(redirect)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "could not pack RedirectInfo: %v", err)
		}
		st := status.New(codes.FailedPrecondition, "not the correct predecessor")
		st, err = st.WithDetails(detail)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "could not attach details: %v", err)
		}
		return nil, st.Err()
	}
	// add the new successor to the routing table
	// create a new public entry for the successor
	newSuccessor := routingtable.NewPublicEntry(
		senderId,
		in.NewSuccessor.Node.Address,
		in.NewSuccessor.Supernode,
		false, // not a unit leader
		false, // not a slice leader
	)
	logger.Log.Infof("Adding new successor: %s", in.NewSuccessor.Node.NodeId)
	if err := s.node.T.AddEntry(newSuccessor); err != nil {
		logger.Log.Errorf("Error adding new successor to routing table: %v", err)
		return nil, status.Errorf(codes.Internal, "could not add new successor: %v", err)
	}
	// change the successor in the routing table
	logger.Log.Infof("Setting new successor: %s", in.NewSuccessor.Node.NodeId)
	if err := s.node.T.SetSuccessor(senderId); err != nil {
		logger.Log.Errorf("Error setting new successor: %v", err)
		return nil, status.Errorf(codes.Internal, "could not set new successor: %v", err)
	}
	// Log the info returned
	logger.Log.Infof("The successor of my new successor is my old successor: %s", currentSucc.ID.ToHexString())
	//TODO: send event to slice leader
	// TODO: apply the change to keepalive
	// return the successor of the new successor
	return &pb.NodeInfo{
		Node: &pb.Node{
			NodeId:  currentSucc.ID.ToHexString(),
			Address: currentSucc.Address,
		},
		Supernode: currentSucc.IsSN,
	}, nil
}
