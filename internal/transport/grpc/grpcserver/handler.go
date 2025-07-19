package grpcserver

import (
	pb "GuptaDHT/api/gen/node"
	"GuptaDHT/internal/node"
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
