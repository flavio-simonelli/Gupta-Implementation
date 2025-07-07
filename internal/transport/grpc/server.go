package grpc

import (
	pb "GuptaDHT/api/gen/node"
	"GuptaDHT/internal/dht"
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"net"
	"strconv"
)

var (
	ErrCreationListener = errors.New("error creating listener")
	ErrNilNode          = errors.New("DHT node cannot be nil")
	ErrNilListener      = errors.New("listener cannot be nil")
	ErrServerFailed     = errors.New("failed gRPC server")
)

type Server struct {
	node *dht.Node
	pb.UnimplementedDisseminationServiceServer
	pb.UnimplementedStorageServiceServer
	pb.UnimplementedJoinServiceServer
	pb.UnimplementedLeaveServiceServer
}

// newServer creates a new gRPC server with the given DHT node
func newServer(node *dht.Node) *Server {
	return &Server{
		node: node,
	}
}

// ----- Configuration Server Operations -----

// getLocalIP retrieves the local IP address of the server opening a fittizia connection
func getLocalIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80") // Google's public DNS server
	if err != nil {
		return "", err
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

// GetListener creates a TCP listener on the specified IP and port or on all interfaces if IP is empty and port chosen dynamically if is 0
func GetListener(ip string, port int) (net.Listener, error) {
	if ip == "" {
		// If no IP is provided, get the local IP address
		var err error
		ip, err = getLocalIP()
		if err != nil {
			return nil, fmt.Errorf("failed to get local IP address: %w", err)
		}
	}
	if port < 0 {
		// if the port is 0, choose a random available port
		// if the port is less than 0, return an error
		return nil, fmt.Errorf("invalid port value: %d, must be greater than or equal to 0", port)
	}
	addr := net.JoinHostPort(ip, strconv.Itoa(port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, ErrCreationListener
	}
	return listener, nil
}

// RunServer starts the gRPC server with the given DHT node and listener
func RunServer(node *dht.Node, lis net.Listener) error {
	// check if the node is nil
	if node == nil {
		return ErrNilNode
	}
	// check if the listener is nil
	if lis == nil {
		return ErrNilListener
	}
	defer lis.Close()
	// Create a new gRPC server
	grpcServer := grpc.NewServer()
	// create a new server instance with the DHT node
	nodeServer := newServer(node)
	// Register the server with the gRPC server
	pb.RegisterJoinServiceServer(grpcServer, nodeServer)
	pb.RegisterDisseminationServiceServer(grpcServer, nodeServer)
	pb.RegisterStorageServiceServer(grpcServer, nodeServer)
	pb.RegisterLeaveServiceServer(grpcServer, nodeServer)
	// Print the address the server is listening on
	fmt.Println("Node server is listening on:", lis.Addr().String())
	if err := grpcServer.Serve(lis); err != nil {
		return ErrServerFailed
	}
	return nil
}

// FindSuccessor handles the gRPC call to find a successor
func (s *Server) FindSuccessor(ctx context.Context, req *pb.FindSuccessorRequest) (*pb.FindSuccessorResponse, error) {
	id, err := dht.IDFromHexString(req.Id.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid node ID: %v", err)
	}
	fmt.Println("Requesting successor for node ID:", id.ToHexString())
	successor, err := s.node.T.FindSuccessor(id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error finding successor: %v", err)
	}
	fmt.Println("Successor:", successor)
	return &pb.FindSuccessorResponse{
		Address: &pb.NodeAddress{
			Address: successor.Address,
		},
	}, nil
}

// BecomePredecessor handles the gRPC call to become a predecessor
func (s *Server) BecomePredecessor(req *pb.BecomePredecessorRequest, stream pb.JoinService_BecomePredecessorServer) error {
	// Parse the node ID from the request
	newId, err := dht.IDFromHexString(req.Node.NodeId.NodeId)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid node ID: %v", err)
	}

	// Check if we have a predecessor
	if s.node.Pred == nil {
		// First node joining, accept it as predecessor
		fmt.Println("No predecessor, accepting new node as predecessor:", newId.ToHexString())
		// Create new entry for the node
		newPred := &dht.RoutingEntry{
			ID:      newId,
			Address: req.Node.Address.Address,
		}
		s.node.Pred = newPred

		// Send empty routing table since this is the first node
		response := &pb.BecomePredecessorResponse{
			Payload: &pb.BecomePredecessorResponse_RoutingChunk{
				RoutingChunk: &pb.RoutingTableChunk{
					Entries: []*pb.Entry{},
				},
			},
		}
		return stream.Send(response)
	}

	// Check if the node ID is > than the current predecessor ID
	if s.node.Pred.ID.LessThan(newId) {
		// OK to become predecessor
		fmt.Println("Becoming predecessor for node ID:", newId.ToHexString())

		// Get all routing entries to send to the new predecessor
		entries := s.node.T.GetAllEntries()

		// Create a new routing entry for the node
		newPred := &dht.RoutingEntry{
			ID:      newId,
			Address: req.Node.Address.Address,
		}

		// Update predecessor (save old predecessor reference if needed)
		// oldPred := s.node.Pred
		s.node.Pred = newPred

		// Send routing table entries in chunks
		const chunkSize = 10
		for i := 0; i < len(entries); i += chunkSize {
			end := i + chunkSize
			if end > len(entries) {
				end = len(entries)
			}

			// Convert entries to protobuf format
			pbEntries := make([]*pb.Entry, 0, end-i)
			for _, entry := range entries[i:end] {
				pbEntry := &pb.Entry{
					NodeId:        entry.E.ID.ToHexString(),
					Address:       entry.E.Address,
					IsSupernode:   entry.IsSupernode,
					IsSliceLeader: entry.IsSliceLeader,
					IsUnitLeader:  entry.IsUnitLeader,
				}
				pbEntries = append(pbEntries, pbEntry)
			}

			// Create and send response with chunk
			response := &pb.BecomePredecessorResponse{
				Payload: &pb.BecomePredecessorResponse_RoutingChunk{
					RoutingChunk: &pb.RoutingTableChunk{
						Entries: pbEntries,
					},
				},
			}

			if err := stream.Send(response); err != nil {
				return status.Errorf(codes.Internal, "failed to send routing entries: %v", err)
			}
		}

		// TODO: Send resources (once implemented)
		// This would go here, after sending all routing table entries
		// for each resource:
		//   1. Send ResourceMetadata
		//   2. Send StoreChunks in sequence

		return nil
	} else {
		// Need to redirect to the successor
		// This uses standard gRPC error with status details
		redirect := &pb.RedirectInfo{
			Target: &pb.Node{
				NodeId: &pb.NodeId{
					NodeId: s.node.Succ.ID.ToHexString(),
				},
				Address: &pb.NodeAddress{
					Address: s.node.Succ.Address,
				},
			},
		}

		anyRedirect, err := anypb.New(redirect)
		if err != nil {
			return status.Errorf(codes.Internal, "error creating redirect info: %v", err)
		}

		st := status.New(codes.FailedPrecondition, "cannot become predecessor for node ID "+newId.ToHexString())
		stDetail, err := st.WithDetails(anyRedirect)
		if err != nil {
			return st.Err()
		}

		return stDetail.Err()
	}
}
