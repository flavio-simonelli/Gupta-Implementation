package storage

import "encoding/json"

// FileManifest represents the metadata of a file stored in the DHT.
type FileManifest struct {
	Filename string   `json:"filename"`  // Name of the file
	Filesize int64    `json:"filesize"`  // Size of the file in bytes
	ChunkKey []string `json:"chunk_key"` // List of hashes for each chunk of the file
}

// CreateManifest creates a new FileManifest with the given filename, filesize, and chunk keys.
func CreateManifest(filename string, filesize int64, chunkKeys []string) *FileManifest {
	return &FileManifest{
		Filename: filename,
		Filesize: filesize,
		ChunkKey: chunkKeys,
	}
}

// MarshalJSON marshals the FileManifest to JSON format.
func (fm *FileManifest) MarshalJSON() ([]byte, error) {
	marshal, err := json.Marshal(fm)
	if err != nil {
		return nil, err
	}
	return marshal, nil
}

// UnmarshalJSON unmarshal the JSON data into a FileManifest.
func UnmarshalJSON(data []byte) (*FileManifest, error) {
	var manifest FileManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}
