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
	_ "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	_ "google.golang.org/grpc/status"
	_ "google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"net"
	"strconv"
)

const (
	ChunkSize     = 10        // Size of each chunk for routing table entries
	FileChunkSize = 64 * 1024 // 64KiB per chunk di file
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

// FindSuccessor handles the gRPC call to find a successor (manca la gestione del timeout nel context)
func (s *Server) FindSuccessor(ctx context.Context, req *pb.FindSuccessorRequest) (*pb.FindSuccessorResponse, error) {
	// get the node ID from the request
	logger.Log.Infof("Received FindSuccessorRequest %s", req.Id.NodeId)
	id, err := dht.IDFromHexString(req.Id.NodeId)
	if err != nil {
		logger.Log.Warnf("Invalid node ID: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid node ID: %v", err)
	}
	// Find the successor for the given ID
	succ, _, err := s.node.T.FindSuccessor(id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error finding successor: %v", err)
	}
	logger.Log.Infof("Found successor for ID %s: %s", id.ToHexString(), succ.Address)
	return &pb.FindSuccessorResponse{
		Address: &pb.NodeAddress{
			Address: succ.Address,
		},
	}, nil
}

// BecomePredecessor handles the gRPC call to become a predecessor
func (s *Server) BecomePredecessor(req *pb.BecomePredecessorRequest, stream pb.JoinService_BecomePredecessorServer) error {
	logger.Log.Infof("Received BecomePredecessorRequest for node ID: %s", req.Node.NodeId.NodeId)
	// parse the node ID from the request
	newId, err := dht.IDFromHexString(req.Node.NodeId.NodeId)
	if err != nil {
		logger.Log.Warnf("Invalid node ID: %v", err)
		return status.Errorf(codes.InvalidArgument, "invalid node ID: %v", err)
	}
	// change the predecessor
	oldPred, err := s.node.T.ChangePredecessor(newId, req.Node.Address.Address, req.Node.Supernode, s.node.K, s.node.U)
	if err != nil {
		if errors.Is(err, dht.ErrPredRedirect) {
			// Redirect to the successor if the new ID is not greater than the current predecessor
			logger.Log.Warnf("Redirecting to successor: %v", oldPred)
			st := status.New(codes.FailedPrecondition, "not the real successor")
			stWithInfo, err := st.WithDetails(&pb.RedirectInfo{
				Target: &pb.NodeAddress{
					Address: oldPred,
				},
			})
			if err != nil {
				logger.Log.Errorf("Error creating redirect info: %v", err)
				return status.Errorf(codes.Internal, "error creating redirect info: %v", err)
			}
			return stWithInfo.Err()
		} else {
			logger.Log.Errorf("Error changing predecessor: %v", err)
			return status.Errorf(codes.Internal, "error changing predecessor: %v", err)
		}
	}
	// send the routing table entries in chunks
	logger.Log.Infof("Sending routing table entries to new predecessor: %s", newId.ToHexString())
	for i := 0; i < s.node.T.LenTable(); i += ChunkSize {
		end := i + ChunkSize
		if end > s.node.T.LenTable() {
			end = s.node.T.LenTable()
		}
		// Get the entries for the current chunk
		entries, err := s.node.T.ToTransportEntry(i, end)
		if err != nil {
			logger.Log.Errorf("Error getting routing entries chunk: %v", err)
			return status.Errorf(codes.Internal, "error getting routing entries chunk: %v", err)
		}
		logger.Log.Infof("Sending chunk %d to new predecessor with %d entries", i/ChunkSize+1, len(entries))
		pbEntries := make([]*pb.Entry, 0, len(entries))
		for _, entry := range entries {
			pbEntry := &pb.Entry{
				NodeId:        entry.Id.ToHexString(),
				Address:       entry.Address,
				IsSupernode:   entry.IsSupernode,
				IsSliceLeader: entry.IsSliceLeader,
				IsUnitLeader:  entry.IsUnitLeader,
			}
			pbEntries = append(pbEntries, pbEntry)
		}
		response := &pb.BecomePredecessorResponse{
			Payload: &pb.BecomePredecessorResponse_RoutingChunk{
				RoutingChunk: &pb.RoutingTableChunk{
					Entries: pbEntries,
				},
			},
		}
		if err := stream.Send(response); err != nil {
			logger.Log.Errorf("Failed to send routing entries chunk: %v", err)
			return status.Errorf(codes.Internal, "failed to send routing entries chunk: %v", err)
		}
	}
	// send the resources that the predecessor should take over
	resToSend, err := s.node.Store.ListResources(newId, s.node.ID)
	if err != nil {
		logger.Log.Errorf("Error listing resources for new predecessor: %v", err)
		return status.Errorf(codes.Internal, "error listing resources for new predecessor: %v", err)
	}
	for _, res := range resToSend {
		metadata, err := s.node.Store.GetFileInfo(res.Filename)
		if err != nil {
			logger.Log.Errorf("Error getting file info for resource %s: %v", res.Filename, err)
			continue
		}
		// create and send the resource metadata
		meta := &pb.ResourceMetadata{
			Key:  metadata.ID.ToHexString(),
			Name: metadata.Filename,
			Size: metadata.Size,
		}
		if err := stream.Send(&pb.BecomePredecessorResponse{
			Payload: &pb.BecomePredecessorResponse_ResourceMetadata{
				ResourceMetadata: meta,
			},
		}); err != nil {
			logger.Log.Errorf("Failed to send resource metadata for %s: %v", res.Filename, err)
			return status.Errorf(codes.Internal, "failed to send resource metadata for %s: %v", res.Filename, err)
		}
		// now send the file chunks
		rc, err := s.node.Store.GetStream(res.Filename)
		if err != nil {
			return status.Errorf(codes.Internal, "open resource: %v", err)
		}
		defer rc.Close()

		buf := make([]byte, FileChunkSize)
		idx := uint64(0)
		for {
			n, rerr := rc.Read(buf)
			if n > 0 {
				chunk := &pb.StoreChunk{
					ResourceKey: res.ID.ToHexString(),
					Offset:      idx,
					Data:        buf[:n],
					Eof:         false, // sovrascrivo dopo
				}
				// anticipiamo il last prima di mandare
				if rerr == io.EOF {
					chunk.Eof = true
				}
				if err := stream.Send(&pb.BecomePredecessorResponse{
					Payload: &pb.BecomePredecessorResponse_StoreChunk{
						StoreChunk: chunk,
					},
				}); err != nil {
					rc.Close()
					return status.Errorf(codes.Internal, "send chunk: %v", err)
				}
				idx++
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				rc.Close()
				return status.Errorf(codes.Internal, "read: %v", rerr)
			}
		}
	}
	return nil
}

// NotifyPredecessor handles the gRPC call to notify a predecessor
func (s *Server) NotifyPredecessor(ctx context.Context, req *pb.NotifyPredecessorRequest) (*emptypb.Empty, error) {
	logger.Log.Infof("Received NotifyPredecessorRequest for node ID: %s", req.NewSuccessor.NodeId.NodeId)
	// parse the node ID from the request
	newId, err := dht.IDFromHexString(req.NewSuccessor.NodeId.NodeId)
	if err != nil {
		logger.Log.Warnf("Invalid node ID: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid node ID: %v", err)
	}
	// change the predecessor
	oldPred, err := s.node.T.ChangeSuccessor(newId, req.NewSuccessor.Address.Address, req.NewSuccessor.Supernode, s.node.K, s.node.U)
	if err != nil {
		if errors.Is(err, dht.ErrRedirectSucc) {
			// Redirect to the successor if the new ID is not greater than the current predecessor
			logger.Log.Warnf("Redirecting to successor: %v", oldPred)
			st := status.New(codes.FailedPrecondition, "not the real successor")
			stWithInfo, err := st.WithDetails(&pb.RedirectInfo{
				Target: &pb.NodeAddress{
					Address: oldPred,
				},
			})
			if err != nil {
				logger.Log.Errorf("Error creating redirect info: %v", err)
				return nil, status.Errorf(codes.Internal, "error creating redirect info: %v", err)
			}
			return nil, stWithInfo.Err()
		} else {
			logger.Log.Errorf("Error changing predecessor: %v", err)
			return nil, status.Errorf(codes.Internal, "error changing predecessor: %v", err)
		}
	}
	logger.Log.Infof("Changed predecessor to %s for node ID %s", req.NewSuccessor.Address.Address, newId.ToHexString())
	return &emptypb.Empty{}, nil
}
