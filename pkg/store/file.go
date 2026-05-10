package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/VA-ibh-AV/go-schemadrift/pkg/drift"
)

// FileStore persists a Baseline to disk using an atomic write (tmp + rename).
type FileStore struct {
	path string
}

// NewFileStore creates a FileStore at path, creating any missing parent directories.
func NewFileStore(path string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("store: mkdir: %w", err)
	}
	return &FileStore{path: path}, nil
}

// Load reads the baseline from disk. If the file does not exist, returns a
// fresh unfrozen Baseline using the supplied config.
func (s *FileStore) Load(cfg drift.BaselineConfig) (*drift.Baseline, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return drift.NewBaseline(cfg), nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: read %s: %w", s.path, err)
	}
	b := drift.NewBaseline(cfg)
	if err := json.Unmarshal(data, b); err != nil {
		return nil, fmt.Errorf("store: unmarshal %s: %w", s.path, err)
	}
	return b, nil
}

// Save atomically writes the baseline to disk.
func (s *FileStore) Save(b *drift.Baseline) error {
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return fmt.Errorf("store: marshal: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("store: write tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("store: rename: %w", err)
	}
	return nil
}

// Path returns the filesystem path used by this store.
func (s *FileStore) Path() string { return s.path }
