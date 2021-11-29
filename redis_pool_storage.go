package storage

import (
	"context"
	"fmt"

	redigo "github.com/gomodule/redigo/redis"
)

// RedisPoolStorage implements the Storage interface for interacting with a Redis instance.
type RedisPoolStorage struct {
	Root string
	Pool *redigo.Pool
}

// NewRedisPoolStorage creates a RedisPoolStorage with a connection.
func NewRedisPoolStorage(pool *redigo.Pool, root string) RedisPoolStorage {
	return RedisPoolStorage{
		Root: root,
		Pool: pool,
	}
}

// Write writes the data to the key in Redis instance.
//
// Options are ignored.
func (s RedisPoolStorage) Write(ctx context.Context, key string, body []byte, _ *Options) error {
	k := s.buildKey(key)

	conn := s.Pool.Get()
	defer conn.Close()

	_, err := conn.Do(redisCmdSet, k, body)

	return err
}

// Read will read the data from the Redis instance.
func (s RedisPoolStorage) Read(ctx context.Context, key string) ([]byte, error) {
	k := s.buildKey(key)

	conn := s.Pool.Get()
	defer conn.Close()

	resp, err := conn.Do(redisCmdGet, k)
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
func (s RedisPoolStorage) Remove(ctx context.Context, key string) error {
	k := s.buildKey(key)

	conn := s.Pool.Get()
	defer conn.Close()

	if _, err := conn.Do(redisCmdDelete, k); err != nil {
		return err
	}

	return nil
}

func (s RedisPoolStorage) buildKey(key string) string {
	if len(s.Root) == 0 {
		return key
	}

	v := fmt.Sprintf("%s/%s", s.Root, key)

	return v
}
