package storage

import (
	"context"
	"errors"
	"io/fs"
	"io/ioutil"
	"os"
	"strings"
)

const (
	// DefaultFileMode dafaults to read/write for the user, only.
	DefaultFileMode = os.FileMode(0600)
)

// FilesystemStorage implements the Storage interface for interacting with the filesystem.
type FilesystemStorage struct {
	Root string
}

// NewFilesystemStorage creates a new FilesystemStorage.
func NewFilesystemStorage(root string) FilesystemStorage {
	return FilesystemStorage{
		Root: root,
	}
}

// Write writes the data to the key a file, with Options applied.
func (s FilesystemStorage) Write(ctx context.Context,
	key string,
	body []byte,
	options *Options) error {

	filename := s.buildFilename(key)

	// check options and file mode
	if options == nil {
		options = &Options{
			Mode: DefaultFileMode,
		}
	} else if options.Mode == 0 {
		// file mode, defaulting to 0600
		options.Mode = DefaultFileMode
	}

	return ioutil.WriteFile(filename, body, options.Mode)
}

// Read will read a file relative to the root of the store.
func (s FilesystemStorage) Read(ctx context.Context, key string) ([]byte, error) {
	filename := s.buildFilename(key)

	b, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) || errors.Is(err, fs.ErrNotExist) {
			// specifically the file was not found
			return nil, ErrNotFound
		}

		// some other error
		return nil, err
	}

	return b, nil
}

// Remove removes a file, relative to the root of the store.
func (s FilesystemStorage) Remove(ctx context.Context, key string) error {
	filename := s.buildFilename(key)

	return os.Remove(filename)
}

func (s FilesystemStorage) buildFilename(key string) string {
	parts := []string{
		s.Root,
		key,
	}

	return strings.Join(parts, "/")
}
