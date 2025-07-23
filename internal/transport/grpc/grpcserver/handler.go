package grpcserver

import (
	pb "GuptaDHT/api/gen/node"
	"GuptaDHT/internal/dht/event"
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
	"io"
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
	if err != nil || (!currentSucc.ID.Equals(s.node.T.GetSelf().ID) && !senderId.IsOwnedBy(s.node.T.GetSelf().ID, currentSucc.ID)) {
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
	//send the event to slice leader
	e := event.NewEvent(newSuccessor.ID, newSuccessor.Address, newSuccessor.IsSN, event.JOIN)
	s.node.EventBoard.SendEvent(e)
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

// SLNotify is a gRPC method that allows a node to send notifications to the slice leader, and the slice leader insert the events in the event board
func (s *Server) SLNotify(stream pb.DisseminationService_SLNotifyServer) error {
	// check if the node is a slice leader
	if !s.node.T.IsSliceLeader() {
		logger.Log.Errorf("Node is not a slice leader, cannot process SLNotify")
		return status.Errorf(codes.FailedPrecondition, "node is not a slice leader")
	}
	events := make([]*event.Event, 0)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// stream has been closed by the client
			if err := s.node.EventBoard.AddEvent(events); err != nil {
				logger.Log.Errorf("failed to add events to board: %v", err)
				return status.Errorf(codes.InvalidArgument, "failed to add events to board: %v", err)
			}
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			logger.Log.Errorf("Error recived in chunk: %v", err)
			return err
		}
		// process the received chunk
		for _, pbEvent := range chunk.Events {
			idTarget, err := id.FromHexString(pbEvent.Target.Node.NodeId)
			if err != nil {
				logger.Log.Errorf("Invalid node ID in event: %v", err)
				return status.Errorf(codes.InvalidArgument, "invalid node ID in event: %v", err)
			}
			ev := event.NewEvent(idTarget, pbEvent.Target.Node.Address, pbEvent.Target.Supernode, event.EventType(pbEvent.EventType))
			// insert the event in the slice events
			events = append(events, ev)
			logger.Log.Infof("Ricevuto evento: %v", ev)
		}
	}
}

// FlowNotify is a gRPC method that allows nodes to notify each other about events
func (s *Server) FlowNotify(stream pb.DisseminationService_FlowNotifyServer) error {
	ctx := stream.Context()
	// extract metadata from the context
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.InvalidArgument, "missing metadata")
	}
	senderIDs := md.Get("node-id")
	if len(senderIDs) == 0 {
		return status.Error(codes.InvalidArgument, "missing node-id in metadata")
	}
	senderID, err := id.FromHexString(senderIDs[0])
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid node-id: %v", err)
	}
	var receivedEvents []*event.Event
	// Ricezione stream di chunk
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Log.Errorf("Error receiving chunk: %v", err)
			return err
		}
		for _, pbEv := range chunk.Events {
			targetID, err := id.FromHexString(pbEv.Target.Node.NodeId)
			if err != nil {
				logger.Log.Warnf("Skipping invalid event node-id: %v", err)
				continue
			}
			ev := event.NewEvent(
				targetID,
				pbEv.Target.Node.Address,
				pbEv.Target.Supernode,
				event.EventType(pbEv.EventType),
			)
			receivedEvents = append(receivedEvents, ev)
		}
	}
	// Aggiorna la routing table con gli eventi
	_ = s.node.EventBoard.UpdateTable(receivedEvents)
	// UNIT LEADER: invia agli altri due vicini se nella stessa unità
	if s.node.T.IsUnitLeader() {
		logger.Log.Infof("FlowNotify: node is unit leader, forwarding to both directions")
		if next, err := s.node.T.GetMySuccessor(); err == nil {
			if s.node.T.GetSelf().ID.SameUnit(next.ID) && !s.node.T.GetSelf().ID.Equals(next.ID) {
				go s.node.EventBoard.SendFlowEvents(receivedEvents, next.Address)
			}
		}
		if prev, err := s.node.T.GetMyPredecessor(); err == nil {
			if s.node.T.GetSelf().ID.SameUnit(prev.ID) && !s.node.T.GetSelf().ID.Equals(prev.ID) {
				go s.node.EventBoard.SendFlowEvents(receivedEvents, prev.Address)
			}
		}
	} else {
		// NODO NORMALE: determina direzione in base a chi ha inviato
		next, err := s.node.T.GetMySuccessor()
		if err == nil && !next.ID.Equals(s.node.T.GetSelf().ID) {
			if next.ID.Equals(senderID) {
				logger.Log.Infof("FlowNotify: the node sender is the successor, forwarding to prev direction")
				prev, err := s.node.T.GetMyPredecessor()
				if err == nil && s.node.T.GetSelf().ID.SameUnit(prev.ID) {
					go s.node.EventBoard.SendFlowEvents(receivedEvents, prev.Address)
				}
			} else {
				logger.Log.Infof("FlowNotify: the node sender is not the successor, forwarding to next direction")
				if s.node.T.GetSelf().ID.SameUnit(next.ID) {
					go s.node.EventBoard.SendFlowEvents(receivedEvents, next.Address)
				}
			}
		}
	}
	// Risposta al client
	if err := stream.SendAndClose(&emptypb.Empty{}); err != nil {
		return err
	}
	return nil
}
