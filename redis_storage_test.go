package storage

import (
	"context"
	"os"
	"testing"

	"github.com/scottjbarr/redis"
)

func TestRedisStorage_Write(t *testing.T) {
	url := os.Getenv("REDIS_URL")
	if len(url) == 0 {
		t.Skip("REDIS_URL not provided")
	}

	pool, err := redis.NewPool(url)
	if err != nil {
		t.Fatal(err)
	}

	// make sure the pool is valid
	conn := pool.Get()

	s := NewRedisStorage(conn, "")

	ctx := context.Background()

	key := "foo"
	data := "bar"

	if err := s.Write(ctx, key, []byte(data), nil); err != nil {
		t.Fatal(err)
	}

	got, err := s.Read(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if string(got) != data {
		t.Fatalf("got %v want %v", string(got), data)
	}

	if err := s.Remove(ctx, key); err != nil {
		t.Fatal(err)
	}
}
