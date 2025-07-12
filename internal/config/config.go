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
	ErrInvalidKValue     = errors.New("invalid K value: must be greater than 0")
	ErrInvalidUValue     = errors.New("invalid U value: must be >= 0")
	ErrInvalidPortValue  = errors.New("invalid port value: must be greater than 0")
)

// Node‑level sub‑config
type NodeConfig struct {
	ID                   string `json:"id"`
	IP                   string `json:"ip"`
	Port                 int    `json:"port"`
	MainStorage          string `json:"main_storage"`
	PredecessorStorage   string `json:"predecessor_storage"`
	MaxConnectionsClient int    `json:"max_connections_client"`
	Supernode            bool   `json:"supernode"` // true if this node is a supernode
}

// DHT‑level sub‑config
type DHTConfig struct {
	K             int           `json:"K"`
	U             int           `json:"U"`
	BootstrapNode string        `json:"bootstrap_node"`
	TimeKA        time.Duration `json:"timeKeepAlive"` // Time to wait before sending the K events to the slice leaders
	TBig          time.Duration `json:"tBig"`          // Time to wait before sending the events to the slice leaders
	TWait         time.Duration `json:"tWait"`         // Time to wait before retrying to send the events to the unit leaders
	RetryInterval time.Duration `json:"retryInterval"` // Time to wait before retrying to send the events to the slice leaders
	ChunkSize     int           `json:"chunkSize"`     // Size of each chunk in bytes for file transfers
}

// Config is the root object that mirrors the JSON structure.
type Config struct {
	Node NodeConfig `json:"node"`
	DHT  DHTConfig  `json:"dht"`
}

// expandHome($HOME) → percorso assoluto
func expandHome(path string) string {
	if len(path) > 0 && path[0] == '~' {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, path[1:])
		}
	}
	return os.ExpandEnv(path) // gestisce $HOME variabile
}

// LoadConfig legge, deserializza e valida il file JSON.
func LoadConfig(filePath string) (Config, error) {
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return Config{}, ErrReadConfigFailed
	}

	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, ErrParseConfigFailed
	}

	// Validazione valori DHT
	if cfg.DHT.K <= 0 {
		return Config{}, ErrInvalidKValue
	}
	if cfg.DHT.U <= 0 {
		return Config{}, ErrInvalidUValue
	}
	if cfg.Node.Port < 0 {
		return Config{}, ErrInvalidPortValue
	}

	// Espansione percorsi che contengono ~ o $HOME
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
