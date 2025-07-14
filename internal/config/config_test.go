package config

import (
	"os"
	"path/filepath"
	"testing"
)

// helper: genera un file di configurazione temporaneo
func createTempConfigFile(t *testing.T, content string) string {
	tmpFile, err := os.CreateTemp("", "config_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	_, err = tmpFile.WriteString(content)
	if err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()
	return tmpFile.Name()
}

func TestExpandHome(t *testing.T) {
	home, _ := os.UserHomeDir()

	result := expandHome("~/testdir")
	expected := filepath.Join(home, "testdir")
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}

	os.Setenv("HOME", home)
	result = expandHome("$HOME/testdir")
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestLoadConfig_Valid(t *testing.T) {
	jsonContent := `{
		"node": {
			"id": "node123",
			"ip": "127.0.0.1",
			"port": 8080,
			"main_storage": "~/data",
			"predecessor_storage": "~/backup",
			"max_connections_client": 10,
			"supernode": true
		},
		"dht": {
			"K": 3,
			"U": 5,
			"bootstrap_node": "127.0.0.1:8000",
			"time_keep_alive": 5000000000,
			"t_big": 2000000000,
			"t_wait": 1000000000,
			"time_retry_send": 500000000,
			"chunk_file_size": 1024,
			"chunk_event_size": 256,
			"chunk_routing_table_size": 128
		}
	}`

	filePath := createTempConfigFile(t, jsonContent)
	defer os.Remove(filePath)

	cfg, err := LoadConfig(filePath)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if cfg.Node.ID != "node123" {
		t.Errorf("Expected ID to be node123, got %s", cfg.Node.ID)
	}
	if cfg.DHT.K != 3 || cfg.DHT.U != 5 {
		t.Errorf("Unexpected DHT values: K=%d, U=%d", cfg.DHT.K, cfg.DHT.U)
	}
}

func TestLoadConfig_InvalidDHT(t *testing.T) {
	jsonContent := `{
		"node": {
			"id": "",
			"ip": "",
			"port": 8080,
			"main_storage": "~/data",
			"predecessor_storage": "~/backup",
			"max_connections_client": 10,
			"supernode": false
		},
		"dht": {
			"K": 0,
			"U": 5,
			"time_keep_alive": 1000000000,
			"t_big": 1000000000,
			"t_wait": 1000000000,
			"time_retry_send": 1000000000,
			"chunk_file_size": 1,
			"chunk_event_size": 1,
			"chunk_routing_table_size": 1
		}
	}`
	filePath := createTempConfigFile(t, jsonContent)
	defer os.Remove(filePath)

	_, err := LoadConfig(filePath)
	if err != ErrInvalidKValue {
		t.Fatalf("Expected ErrInvalidKValue, got %v", err)
	}
}

func TestSaveNodeInfo(t *testing.T) {
	jsonContent := `{
		"node": {
			"id": "",
			"ip": "",
			"port": 0,
			"main_storage": "~/data",
			"predecessor_storage": "~/backup",
			"max_connections_client": 10,
			"supernode": false
		},
		"dht": {
			"K": 1,
			"U": 0,
			"time_keep_alive": 1000000000,
			"t_big": 1000000000,
			"t_wait": 1000000000,
			"time_retry_send": 1000000000,
			"chunk_file_size": 1,
			"chunk_event_size": 1,
			"chunk_routing_table_size": 1
		}
	}`
	filePath := createTempConfigFile(t, jsonContent)
	defer os.Remove(filePath)

	err := SaveNodeInfo(filePath, "node999", "192.168.1.1:9090")
	if err != nil {
		t.Fatalf("SaveNodeInfo failed: %v", err)
	}

	cfg, err := LoadConfig(filePath)
	if err != nil {
		t.Fatalf("Reloading config failed: %v", err)
	}

	if cfg.Node.ID != "node999" || cfg.Node.IP != "192.168.1.1" || cfg.Node.Port != 9090 {
		t.Errorf("Expected updated node info, got %+v", cfg.Node)
	}
}

func TestLoadConfig_InvalidJSON(t *testing.T) {
	badJSON := `{"node":{`
	filePath := createTempConfigFile(t, badJSON)
	defer os.Remove(filePath)

	_, err := LoadConfig(filePath)
	if err != ErrParseConfigFailed {
		t.Fatalf("Expected ErrParseConfigFailed, got %v", err)
	}
}

func TestLoadConfig_FileMissing(t *testing.T) {
	_, err := LoadConfig("nonexistent_file.json")
	if err != ErrReadConfigFailed {
		t.Fatalf("Expected ErrReadConfigFailed, got %v", err)
	}
}
