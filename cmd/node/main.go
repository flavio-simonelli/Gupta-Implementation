package main

import (
	"GuptaDHT/internal/config"
	"GuptaDHT/internal/dht/event"
	"GuptaDHT/internal/dht/id"
	"GuptaDHT/internal/dht/storage"
	"GuptaDHT/internal/logger"
	"GuptaDHT/internal/node"
	transport "GuptaDHT/internal/transport/grpc"
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
	// initialize id parameters
	err = id.InitializeIDParameters(configuration.DHT.K, configuration.DHT.U)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error initializing ID (K, U) parameters")
	}
	// initialize the address and port for the node
	listener, err := transport.GetListener(configuration.Node.IP, configuration.Node.Port)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error creating listener")
		return
	}
	logger.Log.Infof("The server address is: %s", listener.Addr())
	// generate a client handle for the node
	client, err := transport.NewConnectionPool(configuration.Node.MaxConnectionsClient)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error creating gRPC client connection pool")
		return
	}
	logger.Log.Infof("Create a client connection pool with max size: %d", configuration.Node.MaxConnectionsClient)
	// initialize the storage for the node
	mainStore := storage.NewStorage(configuration.Node.MainStorage, configuration.DHT.ChunkFileSize) //TODO: gestire i file entries
	logger.Log.Infof("Initialized node storage at: %s", configuration.Node.MainStorage)
	// initialize the storage backup for the predecessor node
	predecessorStore := storage.NewStorage(configuration.Node.PredecessorStorage, configuration.DHT.ChunkFileSize)
	logger.Log.Infof("Initialized predecessor backup storage at: %s", configuration.Node.PredecessorStorage)
	// initializate the event boards for the node
	normalBoard := event.NewEventDispatcher(client, configuration.DHT.RetryInterval)
	logger.Log.Infof("Initialized normal event board with retry interval: %s", configuration.DHT.RetryInterval)
	// create the id of the node from the configuration file or ip:port if not provided
	id, err := id.IDFromHexString(configuration.Node.ID)
	if err != nil {
		if err == id.ErrEmptyHexString {
			// Generate a new ID if the provided hex string is empty
			id = id.GenerateID(configuration.Node.IP, configuration.Node.Port)
			logger.Log.Warnf("Provided ID is empty, generated new ID: %s", id.ToHexString())
		} else {
			logger.Log.WithError(err).Fatal("Error creating node ID from hex string")
			return
		}
	}
	// create a new node with the configuration
	node := node.NewNode(id, configuration.Node.IP, configuration.Node.Port, configuration.Node.Supernode, client, mainStore, predecessorStore, normalBoard)
	logger.Log.Infof("Create a new struct Node")
	// save the new configuration node in the configuration file
	err = config.SaveNodeInfo(DefaultConfigFile, node.ID.ToHexString(), node.Addr)
	if err != nil {
		logger.Log.WithError(err).Errorf("Error saving node info but continuing")
	}
	// join the DHT network if a bootstrap node is provided or create a new one
	if configuration.DHT.BootstrapNode == "" {
		logger.Log.Info("No bootstrap node provided, creating a new DHT network")
		err := node.CreateDHT()
		if err != nil {
			return
		}
	} else {
		logger.Log.Infof("Joining an existing DHT network using bootstrap node: %s", configuration.DHT.BootstrapNode)
		err := node.Join(configuration.DHT.BootstrapNode)
		if err != nil {
			return
		}
	}
	// run the gRPC server
	err = transport.RunServer(node, listener)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error running gRPC server")
		return
	}
	return
}

//TODO: implement a graceful shutdown mechanism for the node
