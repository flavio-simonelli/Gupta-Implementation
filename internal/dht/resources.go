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
	ErrCreateDirectory = errors.New("failed to create directory")
	ErrReadDirectory   = errors.New("failed to read directory")
)

// Resource represents a resource in the DHT network.
type Resource struct {
	ID       ID
	Filename string
}

// Storage provides a simple file-based storage for DHT nodes.
type Storage struct {
	mu  sync.RWMutex
	dir string
}

// NewNodeStorage initializes a new Storage instance for the given directory and returns a list of existing resources
func NewNodeStorage(dir string) (*Storage, []Resource, error) {
	// check if the directory exists and create it if it doesn't
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrCreateDirectory, err)
	}
	// create a new Storage instance
	s := &Storage{dir: dir}
	// if the directory is not empty return a list of id files
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrReadDirectory, err)
	}
	var ids []Resource
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		// insert the resource in ids slice
		id := GenerateID(e.Name())
		ids = append(ids, Resource{
			ID:       id,
			Filename: e.Name(),
		})
	}
	return s, ids, nil
}

// PutStream salva un file leggendo da r (tipicamente un gRPC stream wrapper).
func (s *Storage) PutStream(filename string, r io.Reader) error {
	finalPath := filepath.Join(s.dir, filename)
	tmpPath := finalPath + ".tmp"

	// se esiste già un upload in progress con lo stesso tmp, rimuovilo
	// lock breve per evitare collisioni sul nome
	s.mu.Lock()
	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		s.mu.Unlock()
		return err
	}
	file, err := os.Create(tmpPath)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	defer file.Close()

	// copia a blocchi (io.Copy usa buffer interno 32KB)
	if _, err := io.Copy(file, r); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// chiudi prima del rename per garantire flush
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// rename atomico sotto lock in modo che i lettori vedano tutto o niente
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

// GetStream apre il file per la lettura in streaming.
func (s *Storage) GetStream(filename string) (io.ReadCloser, error) {
	// rimuove eventuali path separator malevoli
	filename = filepath.Base(filename)

	s.mu.RLock()
	full := filepath.Join(s.dir, filename)
	s.mu.RUnlock()

	return os.Open(full)
}

// DeleteResource removes a resource from the storage directory by its filename.
func (s *Storage) DeleteResource(filename string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	filePath := filepath.Join(s.dir, filename)
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete file %s: %w", filename, err)
	}
	return nil
}
