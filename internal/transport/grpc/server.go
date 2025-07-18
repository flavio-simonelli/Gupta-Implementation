package grpc

import (
	pb "GuptaDHT/api/gen/node"
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/dht/routingtable"
	"GuptaDHT/internal/dht/storage"
	"GuptaDHT/internal/logger"
	"GuptaDHT/internal/node"
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
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
	node *node.Node
	pb.UnimplementedDisseminationServiceServer
	pb.UnimplementedStorageServiceServer
	pb.UnimplementedJoinServiceServer
	pb.UnimplementedLeaveServiceServer
}

// newServer creates a new gRPC server with the given DHT node
func newServer(node *node.Node) *Server {
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
func RunServer(node *node.Node, lis net.Listener) error {
	logger.Log.Infof("Starting gRPC server for DHT node %s at %s", node.ID.ToHexString(), lis.Addr().String())
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

// ----- grpc Handlers for DHT operations -----

func (s *Server) FindPredecessor(ctx context.Context, _ *emptypb.Empty) (*pb.NodeInfo, error) {
	// extract the sender ID from the metadata
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "missing metadata")
	}
	senderIDs := md.Get("sender-id")
	if len(senderIDs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing sender-id in metadata")
	}
	logger.Log.Infof("Received FindPredecessorRequest from: %s", senderIDs[0])

	senderID, err := id.IDFromHexString(senderIDs[0])
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid sender-id: %v", err)
	}

	// find the predecessor of the sender ID
	pred, err := s.node.T.FindPredecessor(senderID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error finding predecessor: %v", err)
	}
	// construct the response with the predecessor's information
	return &pb.NodeInfo{
		Node: &pb.Node{
			NodeId:  pred.ID.ToHexString(),
			Address: pred.Address,
		},
		Supernode: s.node.T.IsSupernode(pred.ID), // questo campo dipende dalla tua implementazione interna
	}, nil
}

// BecomeSuccessor handles the gRPC call to become a successor
func (s *Server) BecomeSuccessor(ctx context.Context, req *pb.BecomeSuccessorRequest) (*pb.NodeInfo, error) {
	// Extract the sender ID from the metadata only for Debugging purposes
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "missing metadata")
	}
	senderIDs := md.Get("sender-id")
	if len(senderIDs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing sender-id in metadata")
	}
	logger.Log.Infof("Received BecomePredecessor from: %s", senderIDs[0])

	// Extract the new successor proposed by the request
	newSuccID, err := id.IDFromHexString(req.GetNewSuccessor().Node.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid node ID: %v", err)
	}
	newSuccAddr := req.GetNewSuccessor().Node.Address
	newSuccSupernode := req.GetNewSuccessor().Supernode
	// insert the new node in the routing table
	err = s.node.T.AddEntry(newSuccID, newSuccAddr, newSuccSupernode, false, false)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error adding new successor to routing table: %v", err)
	}
	// Change the successor to the new node
	oldSucc, err := s.node.T.ChangeSuccessor(newSuccID)
	if err != nil {
		if errors.Is(err, routingtable.ErrSuccRedirect) {
			// Redirect to the current successor if the new ID is not valid
			logger.Log.Warnf("Redirecting to current successor: %v", oldSucc)
			st := status.New(codes.FailedPrecondition, "not the real successor")
			// check if the old successor is a supernode
			oldSuccSn := s.node.T.IsSupernode(oldSucc.ID)
			stWithDetail, err := st.WithDetails(&pb.RedirectInfo{
				Target: &pb.NodeInfo{
					Node: &pb.Node{
						NodeId:  oldSucc.ID.ToHexString(),
						Address: oldSucc.Address,
					},
					Supernode: oldSuccSn,
				},
			})
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to attach redirect: %v", err)
			}
			return nil, stWithDetail.Err()
		}
		return nil, status.Errorf(codes.Internal, "error changing successor: %v", err)
	}
	// send the old successor's (is the new successor for him) information back to the caller
	logger.Log.Infof("Changed successor to %s for node ID %s", newSuccAddr, newSuccID.ToHexString())
	// TODO: manca l'invio allo slice leader del nuovo nodo che è entrato nella rete
	// Check if the old successor is a supernode
	oldSuccSn := s.node.T.IsSupernode(oldSucc.ID)
	return &pb.NodeInfo{
		Node: &pb.Node{
			NodeId:  oldSucc.ID.ToHexString(),
			Address: oldSucc.Address,
		},
		Supernode: oldSuccSn,
	}, nil
}

func (s *Server) BecomePredecessor(req *pb.NodeInfo, stream pb.JoinService_BecomePredecessorServer) error {
	// Log the request for debugging purposes
	metadataID, err := id.IDFromHexString(req.Node.NodeId)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid metadata node ID: %v", err)
	}
	logger.Log.Infof("Received BecomePredecessorRequest for node ID: %s", metadataID.ToHexString())
	// Get the information request
	newPredID, err := id.IDFromHexString(req.Node.NodeId)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid node ID: %v", err)
	}
	newPredAddr := req.Node.Address
	newPredSupernode := req.Supernode
	// insert the new node in the routing table
	err = s.node.T.AddEntry(newPredID, newPredAddr, newPredSupernode, false, false)
	if err != nil {
		return status.Errorf(codes.Internal, "error adding new predecessor to routing table: %v", err)
	}
	// change predecessor
	oldPred, err := s.node.T.ChangePredecessor(newPredID)
	if err != nil {
		if errors.Is(err, routingtable.ErrPredRedirect) {
			// Redirect to the current predecessor if the new ID is not valid
			logger.Log.Warnf("Redirecting to current predecessor: %v", oldPred)
			st := status.New(codes.FailedPrecondition, "not the real predecessor")
			// check if the old predecessor is a supernode
			oldPredSn := s.node.T.IsSupernode(oldPred.ID)
			stWithDetail, err := st.WithDetails(&pb.RedirectInfo{
				Target: &pb.NodeInfo{
					Node: &pb.Node{
						NodeId:  oldPred.ID.ToHexString(),
						Address: oldPred.Address,
					},
					Supernode: oldPredSn,
				},
			})
			if err != nil {
				return status.Errorf(codes.Internal, "failed to attach redirect: %v", err)
			}
			return stWithDetail.Err()
		}
	}
	logger.Log.Infof("Changed predecessor to %s for node ID %s", newPredAddr, newPredID.ToHexString())
	// send the routing table entries in chunks to the new predecessor
	err = s.node.T.IterateRoutingTableChunks(func(entries []routingtable.TransportEntry) bool {
		pbEntries := make([]*pb.Entry, 0, len(entries))
		for _, entry := range entries {
			pbEntry := &pb.Entry{
				Node: &pb.NodeInfo{
					Node: &pb.Node{
						NodeId:  entry.Id.ToHexString(),
						Address: entry.Address,
					},
					Supernode: entry.IsSupernode,
				},
				IsSliceLeader: entry.IsSliceLeader,
				IsUnitLeader:  entry.IsUnitLeader,
			}
			pbEntries = append(pbEntries, pbEntry)
		}
		resp := &pb.BecomePredecessorResponse{
			Payload: &pb.BecomePredecessorResponse_RoutingChunk{RoutingChunk: &pb.RoutingTableChunk{
				Entries: pbEntries,
			},
			},
		}
		if err := stream.Send(resp); err != nil {
			logger.Log.Errorf("failed to send routing chunk: %v", err)
			return false // stop iteration
		}
		return true // continue
	})
	// send the data for predecessor
	// obtain all IDs stored in the main store
	allIDs := s.node.MainStore.ListStoredIDs()
	// for each ID, check if it should be handled by the new predecessor and send it
	for _, id := range allIDs {
		// if the file ID is not owned by the new predecessor, skip it
		if !id.LessThan(newPredID) && !id.Equals(newPredID) {
			continue
		}
		// obtain metadata for the file
		meta, ok := s.node.MainStore.GetMetadata(id)
		if !ok {
			continue // Skip if metadata not found
		}
		// send the resource metadata
		if err := stream.Send(&pb.BecomePredecessorResponse{
			Payload: &pb.BecomePredecessorResponse_Resource{
				Resource: &pb.Resource{
					Payload: &pb.Resource_ResourceMetadata{
						ResourceMetadata: &pb.ResourceMetadata{
							Filename: meta.Filename,
							Size:     meta.Size,
						},
					},
				},
			},
		}); err != nil {
			return err
		}
		// send the file chunks
		file, err := s.node.MainStore.OpenFile(id)
		if err != nil {
			continue
		}
		defer s.node.MainStore.CloseFile(file, meta)

		offset := uint64(0)
		for {
			data, err := storage.ReadChunkFile(file, storage.FileChunkSize)
			if err != nil && err != io.EOF {
				break
			}
			eof := len(data) < storage.FileChunkSize || err == io.EOF

			// invia chunk
			err = stream.Send(&pb.BecomePredecessorResponse{
				Payload: &pb.BecomePredecessorResponse_Resource{
					Resource: &pb.Resource{
						Payload: &pb.Resource_StoreChunk{
							StoreChunk: &pb.StoreChunk{
								Filename: meta.Filename,
								Offset:   offset,
								Data:     data,
								Eof:      eof,
							},
						},
					},
				},
			})
			if err != nil {
				break
			}

			offset += uint64(len(data))
			if eof {
				break
			}
		}
	}
	return nil
}
