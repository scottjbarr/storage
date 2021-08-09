package storage

import (
	"context"
	"strings"
)

// MemoryStorage is a store that only resides in memory, with no long term persistence.
type MemoryStorage struct {
	Data map[string][]byte
}

// NewMemoryStore returns a new MemoryStorage
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		Data: map[string][]byte{},
	}
}

// Read fetches data from a key.
func (m *MemoryStorage) Read(ctx context.Context, key string) ([]byte, error) {
	b, ok := m.Data[key]
	if !ok {
		return nil, ErrNotFound
	}

	return b, nil
}

// Write writes data to a key
func (m *MemoryStorage) Write(ctx context.Context, key string, b []byte, _ *Options) error {
	m.Data[key] = b

	return nil
}

// Keys returns all keys in the store.
func (m *MemoryStorage) Keys(path string) ([]string, error) {
	keys := []string{}

	for k := range m.Data {
		if strings.HasPrefix(k, path) {
			keys = append(keys, k)
		}
	}

	return keys, nil
}

// All returns all objects in the store.
func (m *MemoryStorage) All(path string) ([][]byte, error) {
	keys, err := m.Keys(path)
	if err != nil {
		return nil, err
	}

	objs := [][]byte{}

	for _, k := range keys {
		b := m.Data[k]

		objs = append(objs, b)
	}

	return objs, nil
}
