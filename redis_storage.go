package storage

import (
	"context"
	"errors"
	"fmt"

	redigo "github.com/gomodule/redigo/redis"
)

const (
	redisCmdSet = "SET"

	redisCmdGet = "GET"

	redisCmdDelete = "DEL"
)

var (
	// ErrRedisResponseNotByte is returned if the response structure from Redis is not what was
	// expected.
	ErrRedisResponseNotByte = errors.New("response not []byte")
)

// RedisStorage implements the Storage interface for interacting with a Redis instance.
type RedisStorage struct {
	Root string
	Conn redigo.Conn
}

// NewRedisStorage creates a RedisStorage with a connection.
func NewRedisStorage(conn redigo.Conn, root string) RedisStorage {
	return RedisStorage{
		Root: root,
		Conn: conn,
	}
}

// Write writes the data to the key in Redis instance.
//
// Options are ignored.
func (s RedisStorage) Write(ctx context.Context, key string, body []byte, _ *Options) error {
	k := s.buildKey(key)

	_, err := s.Conn.Do(redisCmdSet, k, body)

	return err
}

// Read will read the data from the Redis instance.
func (s RedisStorage) Read(ctx context.Context, key string) ([]byte, error) {
	k := s.buildKey(key)

	resp, err := s.Conn.Do(redisCmdGet, k)
	if err != nil {
		return nil, err
	}

	if resp == nil {
		return nil, ErrNotFound
	}

	b, ok := resp.([]byte)
	if !ok {
		return nil, ErrRedisResponseNotByte
	}

	return b, nil
}

// Remove removes the object stored at key in the Redis instance.
func (s RedisStorage) Remove(ctx context.Context, key string) error {
	k := s.buildKey(key)

	if _, err := s.Conn.Do(redisCmdDelete, k); err != nil {
		return err
	}

	return nil
}

func (s RedisStorage) buildKey(key string) string {
	if len(s.Root) == 0 {
		return key
	}

	v := fmt.Sprintf("%s/%s", s.Root, key)

	return v
}
