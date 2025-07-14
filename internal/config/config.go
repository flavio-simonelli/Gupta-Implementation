package config

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// error set
var (
	ErrReadConfigFailed  = errors.New("failed to read configuration file")
	ErrParseConfigFailed = errors.New("failed to parse configuration file")

	// DHTConfig validation errors
	ErrInvalidKValue           = errors.New("invalid K value: must be greater than 0")
	ErrInvalidUValue           = errors.New("invalid U value: must be >= 0")
	ErrInvalidKeepAlive        = errors.New("invalid time_keep_alive: must be greater than 0")
	ErrInvalidTBig             = errors.New("invalid t_big: must be greater than 0")
	ErrInvalidTWait            = errors.New("invalid t_wait: must be greater than 0")
	ErrInvalidRetryInterval    = errors.New("invalid time_retry_send: must be greater than 0")
	ErrInvalidChunkFileSize    = errors.New("invalid chunk_file_size: must be greater than 0")
	ErrInvalidChunkEventSize   = errors.New("invalid chunk_event_size: must be greater than 0")
	ErrInvalidChunkRoutingSize = errors.New("invalid chunk_routing_table_size: must be greater than 0")

	// NodeConfig validation errors
	ErrInvalidPortValue         = errors.New("invalid port value: must be greater than or equal to 0")
	ErrInvalidClientConnections = errors.New("invalid max_connections_client: must be greater than 0")
	ErrEmptyMainStoragePath     = errors.New("main_storage path cannot be empty")
	ErrEmptyPredecessorStorage  = errors.New("predecessor_storage path cannot be empty")
)

// Node‑level sub‑config
type NodeConfig struct {
	ID                   string `json:"id"`                     // Unique identifier for the node, can be empty to generate a new one
	IP                   string `json:"ip"`                     // IP address of the node, can be empty to use the default interface
	Port                 int    `json:"port"`                   // Port of the node, can be 0 to use a random port
	MainStorage          string `json:"main_storage"`           // Path to the main storage directory for the node, can contain ~ or $HOME
	PredecessorStorage   string `json:"predecessor_storage"`    // Path to the predecessor storage directory for the node used for backup, can contain ~ or $HOME
	MaxConnectionsClient int    `json:"max_connections_client"` // Maximum number of concurrent client connections, must be greater than 0
	Supernode            bool   `json:"supernode"`              // true if this node is a supernode
}

// DHT‑level sub‑config
type DHTConfig struct {
	K                     int           `json:"K"`                        // K is the number of slices in the DHT, must be greater than 0
	U                     int           `json:"U"`                        // U is the number of units in each slice, must be greater than or equal to 0
	BootstrapNode         string        `json:"bootstrap_node"`           // Address of the bootstrap node in the format ip:port, can be empty to not use a bootstrap node and create a new DHT network
	TimeKeepAlive         time.Duration `json:"time_keep_alive"`          // time to keep alive a node in the DHT, must be greater than 0
	TBig                  time.Duration `json:"t_big"`                    // t_big is the time to wait for send slice events to other slice leaders, must be greater than 0
	TWait                 time.Duration `json:"t_wait"`                   // t_wait is the time to wait for send all events to unit leaders, must be greater than 0
	RetryInterval         time.Duration `json:"time_retry_send"`          // retry interval for sending events to the slice leader, must be greater than 0
	ChunkFileSize         int           `json:"chunk_file_size"`          // Size of the trasnport chunks for file transfers, must be greater than 0
	ChunkEventSize        int           `json:"chunk_event_size"`         // Size of the transport chunks for events, must be greater than 0
	ChunkRoutingTableSize int           `json:"chunk_routing_table_size"` // Size of the transport chunks for routing table, must be greater than 0
}

// Config is the root object that mirrors the JSON structure.
type Config struct {
	Node NodeConfig `json:"node"`
	DHT  DHTConfig  `json:"dht"`
}

// expandHome($HOME) → absolute path
func expandHome(path string) string {
	if len(path) > 0 && path[0] == '~' {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, path[1:])
		}
	}
	return os.ExpandEnv(path) // gestisce $HOME variabile
}

// Validate checks the NodeConfig for valid values.
func (cfg *NodeConfig) validate() error {
	if cfg.Port < 0 {
		return ErrInvalidPortValue
	}
	if cfg.MaxConnectionsClient <= 0 {
		return ErrInvalidClientConnections
	}
	if cfg.MainStorage == "" {
		return ErrEmptyMainStoragePath
	}
	if cfg.PredecessorStorage == "" {
		return ErrEmptyPredecessorStorage
	}
	return nil
}

// Validate checks the DHTConfig for valid values.
func (cfg *DHTConfig) validate() error {
	if cfg.K <= 0 {
		return ErrInvalidKValue
	}
	if cfg.U < 0 {
		return ErrInvalidUValue
	}
	if cfg.TimeKeepAlive <= 0 {
		return ErrInvalidKeepAlive
	}
	if cfg.TBig <= 0 {
		return ErrInvalidTBig
	}
	if cfg.TWait <= 0 {
		return ErrInvalidTWait
	}
	if cfg.RetryInterval <= 0 {
		return ErrInvalidRetryInterval
	}
	if cfg.ChunkFileSize <= 0 {
		return ErrInvalidChunkFileSize
	}
	if cfg.ChunkEventSize <= 0 {
		return ErrInvalidChunkEventSize
	}
	if cfg.ChunkRoutingTableSize <= 0 {
		return ErrInvalidChunkRoutingSize
	}
	return nil
}

// LoadConfig read, deserialize and validate the configuration file at the given path.
func LoadConfig(filePath string) (Config, error) {
	// Check if the file exists
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return Config{}, ErrReadConfigFailed
	}

	var cfg Config
	// Deserialize the JSON configuration
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, ErrParseConfigFailed
	}

	// validate the DHT configuration
	if err := cfg.DHT.validate(); err != nil {
		return Config{}, err
	}
	// validate the Node configuration
	if err := cfg.Node.validate(); err != nil {
		return Config{}, err
	}

	// expand the paths with $HOME in the NodeConfig
	cfg.Node.MainStorage = expandHome(cfg.Node.MainStorage)
	cfg.Node.PredecessorStorage = expandHome(cfg.Node.PredecessorStorage)

	return cfg, nil
}

// SaveNodeInfo save the node information to the configuration file for restart use
func SaveNodeInfo(filePath, id, addr string) error {
	// split the address into IP and port
	ip, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}
	// write the actual configuration of the node for the next restart
	cfg, err := LoadConfig(filePath)
	if err != nil {
		return err
	}
	// update the node information
	cfg.Node.ID = id
	cfg.Node.IP = ip
	cfg.Node.Port = port
	// serialize the updated configuration
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	// 0644: rw‑r‑–‑–
	return os.WriteFile(filePath, data, 0o644)
}
