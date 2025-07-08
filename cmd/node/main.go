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
	store, _, err := dht.NewNodeStorage(loadConfig.Node.MainStorage) //TODO: gestire i file entries
	if err != nil {
		logger.Log.WithError(err).Fatal("Error creating node storage")
		return
	}
	logger.Log.Infof("Initialized node storage at: %s", loadConfig.Node.MainStorage)
	// create a new node with the configuration
	node, err := dht.NewNode(loadConfig.DHT.U, loadConfig.DHT.K, loadConfig.Node.ID, listener.Addr().String(), client, loadConfig.Node.Supernode, store)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error creating DHT node struct")
		return
	}
	// save the new configuration node in the configuration file
	err = config.SaveNodeInfo(DefaultConfigFile, node.ID.ToHexString(), node.Addr)
	if err != nil {
		logger.Log.WithError(err).Errorf("Error saving node info but continuing")
	}
	// join the DHT network if a bootstrap node is provided or create a new one
	err = node.Join(loadConfig.DHT.BootstrapNode)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error joining DHT network")
		return
	}
	logger.Log.Infof("The DHT node is joined successfully")
	// run the gRPC server
	err = transport.RunServer(node, listener)
	if err != nil {
		logger.Log.WithError(err).Fatal("Error running gRPC server")
		return
	}
	return
}

//TODO: implement a graceful shutdown mechanism for the node
