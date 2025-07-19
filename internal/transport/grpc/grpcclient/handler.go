package grpcclient

import (
	pb "GuptaDHT/api/gen/node"
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/dht/routingtable"
	"GuptaDHT/internal/logger"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"os"
	"path/filepath"
)

// FindPredecessor finds the predecessor of a given ID in the DHT network by contacting the target node, return an error
func (p *ConnectionPool) FindPredecessor(id id.ID, target string) (id.ID, string, bool, error) {
	logger.Log.Infof("Finding predecessor for ID %s on target %s", id.ToHexString(), target)
	info, err := p.GetConnection(target)
	if err != nil {
		return id., "", false, err
	}
	defer info.release() // ensure the connection is released after use
	// create a gRPC client for the FindSuccessor method
	client := pb.NewJoinServiceClient(info.conn)
	// create metadata with senderID information
	// Crea metadata con il campo "sender-id"
	md := metadata.New(map[string]string{
		"sender-id": id.ToHexString(),
	})
	// create a context with timeout for the gRPC call
	ctx := context.Background() // per dopo se voglio aggiungere un timeout (ricordarci di chiudere il context se uso timeout)
	// insert metadata into the context
	ctx = metadata.NewOutgoingContext(ctx, md)
	// call the remote method
	resp, err := client.FindPredecessor(ctx, &emptypb.Empty{})
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			switch st.Code() {
			case codes.Unavailable:
				return id.ID{}, "", false, ErrServerUnavailable
			case codes.DeadlineExceeded:
				return id.ID{}, "", false, ErrDeadlineExceeded
			default:
				return id.ID{}, "", false, fmt.Errorf("error finding predecessor: %w", err)
			}
		}
	}
	// return the successor addressù
	predID, err := id.IDFromHexString(resp.Node.NodeId)
	if err != nil {
		return id.ID{}, "", false, fmt.Errorf("invalid predecessor ID %s: %w", resp.Node.NodeId, err)
	}
	return predID, resp.Node.Address, resp.Supernode, nil
}

// BecomeSuccessor is the handle for a new node that wants to join the network, it contacts its predecessor to notify him, and returns the new successor ID, address and whether it is a supernode or not.
func (p *ConnectionPool) BecomeSuccessor(id id.ID, addr string, sn bool, target string) (id.ID, string, bool, error) {
	logger.Log.Infof("Node %s is becoming successor on target %s", id.ToHexString(), target)
	// create a context for the gRPC call
	// get a connection to the target node
	info, err := p.GetConnection(target)
	if err != nil {
		return id.ID{}, "", false, err
	}
	defer info.release() // ensure the connection is released after use
	client := pb.NewJoinServiceClient(info.conn)
	// insert metadata into the context (inutile)
	ctx := context.Background() // per dopo se voglio aggiungere un timeout (ricordarci di chiudere il context se uso timeout)
	// create metadata with senderID information
	md := metadata.New(map[string]string{
		"sender-id": id.ToHexString(),
	})
	// insert metadata into the context
	ctx = metadata.NewOutgoingContext(ctx, md)
	// build the request
	req := &pb.BecomeSuccessorRequest{
		NewSuccessor: &pb.NodeInfo{
			Node: &pb.Node{
				NodeId:  id.ToHexString(),
				Address: addr,
			},
			Supernode: sn,
		},
	}
	// call the remote method
	resp, err := client.BecomeSuccessor(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			switch st.Code() {
			case codes.Unavailable:
				return id.ID{}, "", false, ErrServerUnavailable
			case codes.DeadlineExceeded:
				return id.ID{}, "", false, ErrDeadlineExceeded
			case codes.FailedPrecondition:
				for _, detail := range st.Details() {
					if redirect, ok := detail.(*pb.RedirectInfo); ok {
						logger.Log.Errorf("Redirecting to predecessor %s", redirect.Target.Node.Address)
						redirectID, err := id.IDFromHexString(redirect.Target.Node.NodeId)
						if err != nil {
							return id.ID{}, "", false, fmt.Errorf("invalid redirect ID %s: %w", redirect.Target.Node.NodeId, err)
						}
						return redirectID, redirect.Target.Node.Address, redirect.Target.Supernode, ErrRedirect
					}
				}
				return id.ID{}, "", false, fmt.Errorf("error becoming successor from failed precondition: %w", err)
			default:
				return id.ID{}, "", false, fmt.Errorf("error becoming successor: %w", err)
			}
		}
	}
	succID, err := id.IDFromHexString(resp.Node.NodeId)
	if err != nil {
		return id.ID{}, "", false, fmt.Errorf("invalid successor ID %s: %w", resp.Node.NodeId, err)
	}
	return succID, resp.Node.Address, resp.Supernode, nil
}

// BecomePredecessor is the handle for a new node that wants to join the network, it contacts its successor to receive the routing table and resources, if it is not the successor because its ID is smaller than the old predecessor, it receives a redirect message.
func (p *ConnectionPool) BecomePredecessor(id id.ID, addr string, sn bool, table *routingtable.Table, store *storage.Storage, target string) error {
	// get a connection to the target node
	logger.Log.Infof("Node %s is becoming predecessor on target %s", id.ToHexString(), target)
	// create a context for the gRPC call
	// get a connection to the target node
	info, err := p.GetConnection(target)
	if err != nil {
		return err
	}
	defer info.release() // ensure the connection is released after use
	client := pb.NewJoinServiceClient(info.conn)
	// insert metadata into the context (inutile)
	ctx := context.Background() // per dopo se voglio aggiungere un timeout (ricordarci di chiudere il context se uso timeout)
	// create metadata with senderID information
	md := metadata.New(map[string]string{
		"sender-id": id.ToHexString(),
	})
	// insert metadata into the context
	ctx = metadata.NewOutgoingContext(ctx, md)
	// build the request
	newPred := &pb.NodeInfo{
		Node: &pb.Node{
			NodeId:  id.ToHexString(),
			Address: addr,
		},
		Supernode: sn,
	}
	// call the remote method
	stream, err := client.BecomePredecessor(ctx, newPred)
	if err != nil {
		return fmt.Errorf("failed to call BecomePredecessor RPC: %w", err)
	}
	// receive the response stream
	// Variabili locali per la gestione file
	var (
		currentFileMeta *pb.ResourceMetadata
		tempFile        *os.File
		tempFilePath    string
		tempDir         = os.TempDir()
	)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			// Stream ended cleanly
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving from stream: %w", err)
		}
		switch payload := resp.Payload.(type) {
		case *pb.BecomePredecessorResponse_RoutingChunk:
			// Ricevo chunk routing table
			for _, entry := range payload.RoutingChunk.Entries {
				// add the routing entry to the routing table+
				id, err := id.IDFromHexString(entry.Node.Node.NodeId)
				if err != nil {
					return fmt.Errorf("invalid node ID %s: %w", entry.Node.Node.NodeId, err)
				}
				err = table.AddEntry(id, entry.Node.Node.Address, entry.Node.Supernode, entry.IsUnitLeader, entry.IsSliceLeader)
				if err != nil {
					return fmt.Errorf("failed to update routing table: %w", err)
				}
			}
			logger.Log.Infof("Received routing chunk with %d entries", len(payload.RoutingChunk.Entries))

		case *pb.BecomePredecessorResponse_Resource:
			switch resourcePayload := payload.Resource.Payload.(type) {
			case *pb.Resource_ResourceMetadata:
				// Ricevo metadati e creo file temporaneo
				currentFileMeta = resourcePayload.ResourceMetadata
				if currentFileMeta == nil {
					return fmt.Errorf("received nil metadata")
				}

				tempFilePath = filepath.Join(tempDir, "recv_"+currentFileMeta.Filename)
				tempFile, err = os.Create(tempFilePath)
				if err != nil {
					return fmt.Errorf("failed to create temp file: %w", err)
				}
				logger.Log.Infof("Receiving file %s into %s", currentFileMeta.Filename, tempFilePath)

			case *pb.Resource_StoreChunk:
				if currentFileMeta == nil || tempFile == nil {
					return fmt.Errorf("received chunk without metadata or temp file")
				}
				chunk := resourcePayload.StoreChunk
				_, err := tempFile.WriteAt(chunk.Data, int64(chunk.Offset))
				if err != nil {
					return fmt.Errorf("failed to write to temp file: %w", err)
				}
				if chunk.Eof {
					tempFile.Close()

					fileID, err := id.IDFromHexString(currentFileMeta.Filename)
					if err != nil {
						return fmt.Errorf("invalid resource ID %s: %w", currentFileMeta.Filename, err)
					}

					store.SaveMetadata(fileID, currentFileMeta.Filename, currentFileMeta.Size)
					err = store.ImportTempFile(currentFileMeta.Filename, tempFilePath)
					if err != nil {
						return fmt.Errorf("failed to move file into storage: %w", err)
					}

					currentFileMeta = nil
					tempFile = nil
					tempFilePath = ""
				}
			default:
				return fmt.Errorf("received unknown resource payload type")
			}
		default:
			return fmt.Errorf("received unknown BecomePredecessorResponse payload type")
		}
	}
	return nil
}

