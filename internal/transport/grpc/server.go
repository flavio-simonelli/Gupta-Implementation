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

	senderID, err := dht.IDFromHexString(senderIDs[0])
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
	newSuccID, err := dht.IDFromHexString(req.GetNewSuccessor().Node.NodeId)
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
		if errors.Is(err, dht.ErrSuccRedirect) {
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
	metadataID, err := dht.IDFromHexString(req.Node.NodeId)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid metadata node ID: %v", err)
	}
	logger.Log.Infof("Received BecomePredecessorRequest for node ID: %s", metadataID.ToHexString())
	// Get the information request
	newPredID, err := dht.IDFromHexString(req.Node.NodeId)
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
		if errors.Is(err, dht.ErrPredRedirect) {
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
	err = s.node.T.IterateRoutingTableChunks(func(entries []dht.TransportEntry) bool {
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
			data, err := dht.ReadChunkFile(file, dht.FileChunkSize)
			if err != nil && err != io.EOF {
				break
			}
			eof := len(data) < dht.FileChunkSize || err == io.EOF

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

/*
// FindPredecessor handles the gRPC call to find the predecessor of a given node ID and returns the entry of the node predecessor of the given ID
func (s *Server) FindPredecessor(, req *pb.FindSuccessorRequest) (*pb.FindSuccessorResponse, error) {
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


/* Questa funzione di become predecessor nel server quando viene chiamata deve fare questo:
controlla se il nodo chiamante ha id più grande del vecchio predecessore, se no rstituisce un messaggio di redirect.
Invia la routing table e le risorse al nodo.
Mette il nodo nello stato di joining, cioè fa partire una goroutine che attende due condizioni: questo nodo riceve un messaggio di notifica dal nodo in joining, o riceve la notifica da un altro nodo che il nodo ha inviato la notifica allo slicer.
Allo scadere del tempo la goroutine elimina il nodo e lo rimuove dalla routing table dichiarando che il nodo è morto allo slice leader. (cosi se aveva contattato anche il predecessore se en accorge anche lui)

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
	for i := 0; i < s.node.T.LenTable(); i += FileChunkSize {
		end := i + FileChunkSize
		if end > s.node.T.LenTable() {
			end = s.node.T.LenTable()
		}
		// Get the entries for the current chunk
		entries, err := s.node.T.ToTransportEntry(i, end)
		if err != nil {
			logger.Log.Errorf("Error getting routing entries chunk: %v", err)
			return status.Errorf(codes.Internal, "error getting routing entries chunk: %v", err)
		}
		logger.Log.Infof("Sending chunk %d to new predecessor with %d entries", i/FileChunkSize+1, len(entries))
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
	resToSend, err := s.node.MainStore.ListResources(newId, s.node.ID)
	if err != nil {
		logger.Log.Errorf("Error listing resources for new predecessor: %v", err)
		return status.Errorf(codes.Internal, "error listing resources for new predecessor: %v", err)
	}
	for _, res := range resToSend {
		metadata, err := s.node.MainStore.GetFileInfo(res.Filename)
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
		rc, err := s.node.MainStore.GetStream(res.Filename)
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

// BecomeSuccessor è una funzione che invece semplicemente cambia il successore del nodo server nel nodo chiamante se quest'ultimo ha id più piccolo rispetto al vecchio predecessore altrimenti restituisce redirect info
// Quando inoltreremo le notidfiche faremo un if che dice se è il predecessore ad iniviarlo invia al successore altrimenti invia al predecessore
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
*/
