package storage

import (
	"context"
	"errors"
	"os"
)

var (
	// ErrNotFound should be returned if the requested item was not found.
	//
	// This error should not be returned from a general search that found zero matching items.
	ErrNotFound = errors.New("Not found")
)

// Storage is the interface combining all storage interfaces.
type Storage interface {
	ReadWriter
}

// ReadWriter interface combines the Reader and Writer interface.
type ReadWriter interface {
	Reader
	Writer
}

type ReadWriteLister interface {
	ReadWriter
	Lister
}

type Lister interface {
	All(string) ([][]byte, error)
}

// Reader interface is for reading an item from the store.
type Reader interface {
	Read(context.Context, string) ([]byte, error)
}

// Writer interface is for adding or updating an item in the store.
type Writer interface {
	Write(context.Context, string, []byte, *Options) error
}

// Options for writing data. Not all Storage implementations will support all options.
//
// For example, writing a file wouldn't support TTL.
type Options struct {
	TTL     int64
	Mode    os.FileMode
	DirMode os.FileMode
}

// NewOptions returns an Options struct with sane defaults set.
//
// TTL with zero value means never expire.
func NewOptions() Options {
	return Options{
		TTL:     0,
		Mode:    0644,
		DirMode: 0755,
	}
}
