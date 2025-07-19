package storage

import (
	"GuptaDHT/internal/dht/id"
	"io"
	"os"
)

// SplitFile splits a file into chunks of the specified size and returns a slice of chunk keys.
func SplitFile(path string, chunkSize int) ([][]byte, []id.ID, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, 0, err
	}
	defer file.Close()

	var chunks [][]byte
	var hashes []id.ID
	buffer := make([]byte, chunkSize)
	var totalSize int64 = 0

	for {
		n, err := file.Read(buffer)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buffer[:n])
			chunks = append(chunks, chunk)
			hashes = append(hashes, id.ChunkToID(chunk))
			totalSize += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, 0, err
		}
	}

	return chunks, hashes, totalSize, nil
}

// JoinChunks write the provided byte slices into a single file at the specified output path.
// the file is created if it does not exist, and overwritten if it does.
func JoinChunks(chunks [][]byte, outputPath string) error {
	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	for _, chunk := range chunks {
		_, err := outFile.Write(chunk)
		if err != nil {
			return err
		}
	}
	return nil
}
