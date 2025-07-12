package dht

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Error in storage operations
var (
	// Global Parameters for the storage
	FileChunkSize int // Size of each chunk in bytes, used for reading/writing files

	// errors

	ErrCreateFile       = errors.New("error creating file in storage")
	ErrResourceNotFound = errors.New("resource not found in storage")
)

// MetadataResource represents a resource in the storage with its metadata.
type MetadataResource struct {
	Filename string       // Name of the file in the storage
	Size     uint64       // Size of the file in bytes
	mu       sync.RWMutex // Mutex to protect access to the resource
}

// Storage provides a simple file-based storage for DHT nodes.
type Storage struct {
	infoData map[ID]*MetadataResource
	dir      string
	mu       sync.RWMutex // Mutex to protect access at the map of resources
}

// ----- Resources Operations -----

// NewStorage creates a new Storage instance with the given directory.
func NewStorage(dir string, chunkSize int) *Storage {
	FileChunkSize = chunkSize // Set the global chunk size
	return &Storage{
		infoData: make(map[ID]*MetadataResource),
		dir:      dir,
	}
}

// CreateFile creates a new file in the storage with the given filename and return the file open in write.
func (s *Storage) CreateFile(filename string) (*os.File, error) {
	path := filepath.Join(s.dir, filename)
	file, err := os.Create(path)
	if err != nil {
		return nil, ErrCreateFile
	}
	return file, nil
}

//TODO: gestire caso in cui il file esiste già

// SaveMetadata saves the metadata of a resource in the storage.
func (s *Storage) SaveMetadata(id ID, filename string, size uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a new MetadataResource
	resource := &MetadataResource{
		Filename: filename,
		Size:     size,
	}

	// Save the resource in the map
	s.infoData[id] = resource
}

// GetMetadata retrieves the metadata of a resource by its ID.
func (s *Storage) GetMetadata(id ID) (*MetadataResource, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	resource, exists := s.infoData[id]
	if !exists {
		return nil, false
	}
	return resource, true
}

// OpenFile opens a file in the storage by its ID and returns the file handle. (attention the file is read locked if error is nil)
func (s *Storage) OpenFile(id ID) (*os.File, error) {
	metadata, exist := s.GetMetadata(id)
	if exist != true {
		return nil, ErrResourceNotFound
	}
	// read lock the resource
	metadata.mu.RLock()
	filePath := filepath.Join(s.dir, metadata.Filename)
	file, err := os.Open(filePath)
	if err != nil {
		metadata.mu.RUnlock()
		return nil, ErrResourceNotFound
	}
	return file, nil
}

// CloseFile closes the file handle and unlocks the resource.
func (s *Storage) CloseFile(file *os.File, resource *MetadataResource) error {
	if file == nil {
		return nil
	}
	defer resource.mu.RUnlock() // Ensure the resource is unlocked after closing the file
	err := file.Close()
	if err != nil {
		return err
	}
	return nil
}

// DeleteFile deletes a file from the storage by its ID and removes its metadata.
func (s *Storage) DeleteFile(id ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	resource, exists := s.infoData[id]
	if !exists {
		return ErrResourceNotFound
	}

	// Remove the file from the filesystem
	filePath := filepath.Join(s.dir, resource.Filename)
	// Lock write the file resource
	resource.mu.Lock()
	err := os.Remove(filePath)
	if err != nil {
		resource.mu.Unlock() // Unlock the resource if there is an error
		return err
	}
	resource.mu.Unlock()

	// Remove the metadata from the map
	delete(s.infoData, id)

	return nil
}

// PutChunkFile saves a chunk of data to a file in the storage.
func PutChunkFile(file *os.File, data []byte) error {
	if file == nil {
		return ErrResourceNotFound
	}
	// Write the data to the file
	_, err := file.Write(data)
	if err != nil {
		return err
	}
	return nil
}

// GetChunkFile reads a chunk of data from a file in the storage.
func ReadChunkFile(file *os.File, maxBytes int) ([]byte, error) {
	buf := make([]byte, maxBytes)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
}

// L'utilizzo di queste funzioni è semplice, si crea un file con CreateFile, si scrive il chunk con PutChunkFile, si legge il chunk con ReadChunkFile e si chiude il file con CloseFile. Se si vuole eliminare il file, si usa DeleteFile.
// Se si sta creando il file dobbiamo anche alla fine salvare i metadati con SaveMetadata, in modo da poter recuperare il file in seguito. Se si vuole leggere il file, si usa OpenFile per ottenere il file handle e poi si legge il chunk con ReadChunkFile. Alla fine si chiude il file con CloseFile.

// ListStoredIDs returns a slice of all IDs currently stored.
func (s *Storage) ListStoredIDs() []ID {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]ID, 0, len(s.infoData))
	for id := range s.infoData {
		ids = append(ids, id)
	}
	return ids
}

// ImportTempFile moves a file from a temporary path to the permanent storage location using the file name.
func (s *Storage) ImportTempFile(filename string, tempPath string) error {
	// Determina il path di destinazione finale nella tua storage
	destPath := filepath.Join(s.dir, filename)
	// Assicurati che la directory esista
	err := os.MkdirAll(filepath.Dir(destPath), 0755)
	if err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}
	// Sposta il file
	err = os.Rename(tempPath, destPath)
	if err != nil {
		return fmt.Errorf("failed to move file from temp to storage: %w", err)
	}
	return nil
}
