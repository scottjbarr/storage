package storage

import (
	"context"
	"sync"
	"time"
)

type Entry struct {
	Expiry int64
	Object []byte
}

func NewEntry(b []byte, ttl int64) Entry {
	return Entry{
		Expiry: time.Now().Unix() + ttl,
		Object: b,
	}
}

type TTLCache struct {
	Store ReadWriter
	Cache map[string]Entry
	TTL   int64
	mu    *sync.RWMutex
}

func NewTTLCache(store ReadWriter, ttl int64) *TTLCache {
	return &TTLCache{
		Store: store,
		Cache: map[string]Entry{},
		TTL:   ttl,
		mu:    &sync.RWMutex{},
	}
}

func (c *TTLCache) Read(ctx context.Context, key string) ([]byte, error) {
	// lock for the read
	e, ok := c.Get(key)
	if ok && e.Expiry > time.Now().Unix() {
		// item was found and the expiry is in the future
		return e.Object, nil
	}

	// item was not found, or has expired
	data, err := c.Store.Read(ctx, key)
	if err != nil {
		return nil, err
	}

	// write the item to cache, locking the map
	c.Set(key, data)

	return data, nil
}

func (c *TTLCache) Write(ctx context.Context,
	key string,
	body []byte,
	options *Options) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.Store.Write(ctx, key, body, options); err != nil {
		return err
	}

	// write the item to cache, locking the map.
	//
	// We already have a lock.
	c.Cache[key] = NewEntry(body, c.TTL)

	return nil
}

func (c *TTLCache) Get(key string) (Entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.Cache[key]

	return e, ok
}

func (c *TTLCache) Set(key string, b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Cache[key] = NewEntry(b, c.TTL)
}
