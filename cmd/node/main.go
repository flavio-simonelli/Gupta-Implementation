package main

import (
	"GuptaDHT/internal/config"
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/logger"
	"GuptaDHT/internal/node"
	client "GuptaDHT/internal/transport/grpc/grpcclient"
	server "GuptaDHT/internal/transport/grpc/grpcserver"
	"errors"
)

const (
	DefaultConfigFile = "config.json"
)

func main() {
	// Initialize logger
	logger.Init("info")
	// read configuration from the config file
	configuration, err := config.LoadConfig(DefaultConfigFile)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error loading configuration")
	}
	logger.Log.Infof("Loaded configuration from %s", DefaultConfigFile)
	// initialize identity parameters
	err = id.InitializeIDParameters(configuration.DHT.K, configuration.DHT.U)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error initializing ID (K, U) parameters")
	}
	// initialize the address and port for the node
	listener, err := server.GetListener(configuration.Node.IP, configuration.Node.Port)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error creating listener")
	}
	logger.Log.Infof("The server address is: %s", listener.Addr())
	// generate a pool client for the node
	poolClient, err := client.NewConnectionPool(configuration.Node.MaxConnectionsClient)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error creating gRPC pool connection pool")
	}
	logger.Log.Infof("Create a pool connection pool with max size: %d", configuration.Node.MaxConnectionsClient)
	// create the identity of the node from the configuration file or ip:port if not provided
	identity, err := id.FromHexString(configuration.Node.ID)
	if err != nil {
		if errors.Is(err, id.ErrEmptyHexString) {
			// Generate a new ID if the provided hex string is empty
			identity = id.GenerateID(configuration.Node.IP, configuration.Node.Port)
			logger.Log.Warnf("Provided ID is empty, generated new ID: %s", identity.ToHexString())
		} else {
			logger.Log.WithError(err).Fatal("Error creating node ID from hex string")
		}
	}
	// create a new node with the identity and address
	n, err := node.NewNode(identity, listener.Addr().String(), configuration.Node.Supernode, poolClient, poolClient, configuration.DHT.TimeKeepAlive)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error creating new node")
	}
	// chack if there are a bootstrap node to join the DHT network
	if configuration.DHT.BootstrapNode == "" {
		logger.Log.Info("No bootstrap node provided, creating a new DHT network")
		// create a new DHT network
		err = n.CreateNetwork()
		if err != nil {
			logger.Log.WithError(err).Fatal("Error creating new DHT network")
		}
		logger.Log.Infof("Node %s is now the first node in the DHT network", identity.ToHexString())
	} else {
		logger.Log.Infof("Joining DHT network using bootstrap node: %s", configuration.DHT.BootstrapNode)
		// join the DHT network using the bootstrap node
		err = n.Join(configuration.DHT.BootstrapNode)
		if err != nil {
			logger.Log.WithError(err).Fatal("Error joining DHT network")
		}
	}
	// run the gRPC server
	err = server.RunServer(n, listener)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error running gRPC server")
	}
}
