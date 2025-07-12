package main

import (
	"GuptaDHT/internal/config"
	"GuptaDHT/internal/dht"
	"GuptaDHT/internal/logger"
	transport "GuptaDHT/internal/transport/grpc"
)

const (
	DefaultConfigFile = "config.json"
)

func main() {
	// Initialize logger
	logger.Init("info")
	// read configuration from config file
	logger.Log.Infof("Loading configuration from %s", DefaultConfigFile)
	loadConfig, err := config.LoadConfig(DefaultConfigFile)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error loading configuration")
		return
	}
	// initialize the global parameters for the DHT
	err = dht.InitializeIDParameters(loadConfig.DHT.K, loadConfig.DHT.U)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error initializing DHT parameters")
		return
	}
	// get listener address and port
	listener, err := transport.GetListener(loadConfig.Node.IP, loadConfig.Node.Port)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error creating listener")
		return
	}
	logger.Log.Infof("The server address is: %s", listener.Addr())
	// generate a client handle for the node
	client, err := transport.NewConnectionPool(loadConfig.Node.MaxConnectionsClient)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error creating gRPC client connection pool")
		return
	}
	logger.Log.Infof("Create a client connection pool with max size: %d", loadConfig.Node.MaxConnectionsClient)
	// initialize the storage for the node
	mainStore := dht.NewStorage(loadConfig.Node.MainStorage, loadConfig.DHT.ChunkSize) //TODO: gestire i file entries
	logger.Log.Infof("Initialized node storage at: %s", loadConfig.Node.MainStorage)
	// initialize the storage backup for the predecessor node
	predecessorStore := dht.NewStorage(loadConfig.Node.PredecessorStorage, loadConfig.DHT.ChunkSize)
	logger.Log.Infof("Initialized predecessor backup storage at: %s", loadConfig.Node.PredecessorStorage)
	// initializate the event boards for the node
	normalBoard := dht.NewNormalBoard(loadConfig.DHT.RetryInterval)
	logger.Log.Infof("Initialized normal event board with retry interval: %s", loadConfig.DHT.RetryInterval)
	// create the id of the node from the configuration file or ip:port if not provided
	id, err := dht.IDFromHexString(loadConfig.Node.ID)
	if err != nil {
		if err == dht.ErrEmptyHexString {
			// Generate a new ID if the provided hex string is empty
			id = dht.GenerateID(loadConfig.Node.IP, loadConfig.Node.Port)
			logger.Log.Warnf("Provided ID is empty, generated new ID: %s", id.ToHexString())
		} else {
			logger.Log.WithError(err).Fatal("Error creating node ID from hex string")
			return
		}
	}
	// create a new node with the configuration
	node := dht.NewNode(id, loadConfig.Node.IP, loadConfig.Node.Port, loadConfig.Node.Supernode, client, mainStore, predecessorStore, normalBoard)
	logger.Log.Infof("Create a new struct Node")
	// save the new configuration node in the configuration file
	err = config.SaveNodeInfo(DefaultConfigFile, node.ID.ToHexString(), node.Addr)
	if err != nil {
		logger.Log.WithError(err).Errorf("Error saving node info but continuing")
	}
	// join the DHT network if a bootstrap node is provided or create a new one
	if loadConfig.DHT.BootstrapNode == "" {
		logger.Log.Info("No bootstrap node provided, creating a new DHT network")
		// create a new DHT network
	} else {
		logger.Log.Infof("Joining an existing DHT network using bootstrap node: %s", loadConfig.DHT.BootstrapNode)
		// join an existing DHT network
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
