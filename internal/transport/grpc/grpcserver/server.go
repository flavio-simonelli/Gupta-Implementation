package grpcserver

import (
	pb "GuptaDHT/api/gen/node"
	"GuptaDHT/internal/logger"
	"GuptaDHT/internal/node"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"net"
	"strconv"
)

var (
	// ErrNilNode is returned if the DHT node passed to the server is nil
	ErrNilNode = errors.New("cannot run gRPC server: DHT node is nil")
	// ErrNilListener is returned if the TCP listener is nil
	ErrNilListener = errors.New("cannot run gRPC server: listener is nil")
	// ErrCreationListener is returned if the TCP listener cannot be created
	ErrCreationListener = errors.New("failed to create TCP listener")
	// ErrServerFailed is returned if the gRPC server fails to serve
	ErrServerFailed = errors.New("gRPC server failed to serve")
)

// RunServer starts the gRPC server with all services registered
func RunServer(n *node.Node, lis net.Listener) error {
	if n == nil {
		return ErrNilNode
	}
	if lis == nil {
		return ErrNilListener
	}

	logger.Log.Infof("Starting gRPC server for DHT node %s on %s", n.T.GetSelf().ID.ToHexString(), lis.Addr().String())
	defer lis.Close()

	server := NewServer(n)
	grpcServer := createGRPCServer(server)

	logger.Log.Infof("gRPC server ready on %s", lis.Addr().String())
	if err := grpcServer.Serve(lis); err != nil {
		return ErrServerFailed
	}
	return nil
}

// createGRPCServer initializes the gRPC server and registers all services
func createGRPCServer(s *Server) *grpc.Server {
	grpcServer := grpc.NewServer()
	pb.RegisterJoinServiceServer(grpcServer, s)
	pb.RegisterDisseminationServiceServer(grpcServer, s)
	pb.RegisterStorageServiceServer(grpcServer, s)
	pb.RegisterLeaveServiceServer(grpcServer, s)
	return grpcServer
}

// GetListener creates a TCP listener on a given IP and port
func GetListener(ip string, port int) (net.Listener, error) {
	if ip == "" {
		var err error
		ip, err = getLocalIP()
		if err != nil {
			return nil, fmt.Errorf("failed to get local IP address: %w", err)
		}
	}
	if port < 0 {
		return nil, fmt.Errorf("invalid port value: %d (must be >= 0)", port)
	}
	addr := net.JoinHostPort(ip, strconv.Itoa(port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, ErrCreationListener
	}
	return listener, nil
}
