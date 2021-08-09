package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"strings"
)

const (
	// DefaultFileMode dafaults to read/write for the user, only.
	DefaultFileMode = os.FileMode(0600)
	DefaultDirMode  = os.FileMode(0750)
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
			Mode:    DefaultFileMode,
			DirMode: DefaultDirMode,
		}
	} else if options.Mode == 0 {
		// file mode, defaulting to 0600
		options.Mode = DefaultFileMode
	}

	// make sure the parent exists
	path := getParentDirectory(filename)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, options.DirMode); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, options.Mode)
	if err != nil {
		return err
	}

	defer f.Close()

	if _, err := f.Write(body); err != nil {
		return err
	}

	return nil
}

func getParentDirectory(key string) string {
	// split the key into parts with the filename being the last
	parts := strings.Split(key, "/")

	// put the parts back together leaving just the full directory path
	return strings.Join(parts[:len(parts)-1], "/")
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

func (s FilesystemStorage) Keys(path string) ([]string, error) {
	keys := []string{}

	c, err := ioutil.ReadDir(s.Root + "/" + path)
	if err != nil {
		return nil, err
	}

	for _, entry := range c {
		if entry.IsDir() {
			continue
		}

		filename := fmt.Sprintf("%s/%s/%s", s.Root, path, entry.Name())

		keys = append(keys, filename)
	}

	return keys, nil
}

func (s FilesystemStorage) All(path string) ([][]byte, error) {
	keys, err := s.Keys(path)
	if err != nil {
		return nil, err
	}

	objs := [][]byte{}

	for _, filename := range keys {
		// filename := fmt.Sprintf("%s/%s/%s", s.Root, path, entry.Name())

		b, err := ioutil.ReadFile(filename)
		if err != nil {
			return nil, err
		}

		objs = append(objs, b)
	}

	return objs, nil
}
