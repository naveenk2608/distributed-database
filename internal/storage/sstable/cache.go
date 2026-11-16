package sstable

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// Cache is an LRU cache of decoded data blocks, bounded in bytes. One cache
// is meant to be shared by every table on a node. It is safe for concurrent
// use; a nil *Cache caches nothing.
type Cache struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	lru      *list.List // front = most recently used
	items    map[cacheKey]*list.Element

	hits, misses atomic.Int64
}

type cacheKey struct {
	table  uint64 // Reader.cacheID, unique per open table in the process
	offset uint64
}

type cacheEntry struct {
	key   cacheKey
	block []byte
}

var nextCacheID atomic.Uint64

// NewCache returns a cache holding up to capacity bytes of blocks.
func NewCache(capacity int64) *Cache {
	return &Cache{capacity: capacity, lru: list.New(), items: map[cacheKey]*list.Element{}}
}

func (c *Cache) get(k cacheKey) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[k]
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	c.hits.Add(1)
	c.lru.MoveToFront(e)
	return e.Value.(*cacheEntry).block, true
}

func (c *Cache) put(k cacheKey, block []byte) {
	if c == nil || int64(len(block)) > c.capacity {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[k]; ok {
		return
	}
	c.items[k] = c.lru.PushFront(&cacheEntry{key: k, block: block})
	c.used += int64(len(block))
	for c.used > c.capacity {
		last := c.lru.Back()
		ent := last.Value.(*cacheEntry)
		c.lru.Remove(last)
		delete(c.items, ent.key)
		c.used -= int64(len(ent.block))
	}
}

// evictTable drops every block of one table.
func (c *Cache) evictTable(table uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for e := c.lru.Front(); e != nil; {
		next := e.Next()
		if ent := e.Value.(*cacheEntry); ent.key.table == table {
			c.lru.Remove(e)
			delete(c.items, ent.key)
			c.used -= int64(len(ent.block))
		}
		e = next
	}
}

// CacheStats reports cache usage.
type CacheStats struct {
	Capacity, Used int64
	Hits, Misses   int64
}

// Stats returns current usage and hit counts.
func (c *Cache) Stats() CacheStats {
	if c == nil {
		return CacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{Capacity: c.capacity, Used: c.used, Hits: c.hits.Load(), Misses: c.misses.Load()}
}
