package api

import (
	"container/list"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

// objCache is a process-local LRU of small, frequently-read objects served
// straight from RAM — no erasure decode, no disk, no peer round trip. MinIO
// leans entirely on the OS page cache; this gives predictable hot-path
// latency for assets, thumbnails and config blobs.
//
// Staleness: entries are evicted on any local write to the key and expire
// after a short TTL (GOSTORE_OBJ_CACHE_TTL, default 10s) so a write on another
// cluster node is picked up quickly.
type objCache struct {
	mu   sync.Mutex
	max  int64 // total byte budget; 0 disables the cache
	obj  int64 // per-object ceiling
	ttl  time.Duration
	cur  int64
	ll   *list.List // front = most recently used
	m    map[string]*list.Element
	hits uint64
	miss uint64
}

type cacheEntry struct {
	key    string
	data   []byte
	info   object.ObjectInfo
	stored time.Time
}

func newObjCache() *objCache {
	max := int64(128 << 20) // 128 MiB
	if v := os.Getenv("GOSTORE_OBJ_CACHE"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			max = n
		}
	}
	perObj := int64(1 << 20) // 1 MiB
	if v := os.Getenv("GOSTORE_OBJ_CACHE_MAX_OBJ"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			perObj = n
		}
	}
	ttl := 10 * time.Second
	if v := os.Getenv("GOSTORE_OBJ_CACHE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			ttl = d
		}
	}
	return &objCache{max: max, obj: perObj, ttl: ttl, ll: list.New(), m: map[string]*list.Element{}}
}

func cacheKey(bucket, key, versionID string) string {
	return bucket + "\x00" + key + "\x00" + versionID
}

// enabled reports whether caching is on and s is eligible for it.
func (c *objCache) enabled() bool { return c != nil && c.max > 0 }

func (c *objCache) eligibleSize(n int64) bool { return c.enabled() && n >= 0 && n <= c.obj }

// get returns a live entry, moving it to the front. A stale entry is dropped.
func (c *objCache) get(k string) (*cacheEntry, bool) {
	if !c.enabled() {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.m[k]
	if !ok {
		c.miss++
		return nil, false
	}
	e := el.Value.(*cacheEntry)
	if time.Since(e.stored) > c.ttl {
		c.removeLocked(el)
		c.miss++
		return nil, false
	}
	c.ll.MoveToFront(el)
	c.hits++
	return e, true
}

func (c *objCache) put(k string, data []byte, info object.ObjectInfo) {
	if !c.enabled() || int64(len(data)) > c.obj {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[k]; ok {
		old := el.Value.(*cacheEntry)
		c.cur -= int64(len(old.data))
		old.data, old.info, old.stored = data, info, time.Now()
		c.cur += int64(len(data))
		c.ll.MoveToFront(el)
	} else {
		e := &cacheEntry{key: k, data: data, info: info, stored: time.Now()}
		c.m[k] = c.ll.PushFront(e)
		c.cur += int64(len(data))
	}
	for c.cur > c.max {
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.removeLocked(back)
	}
}

func (c *objCache) evict(bucket, key, versionID string) {
	if !c.enabled() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Evict every version entry for the key (versionID "" plus any specific).
	prefix := bucket + "\x00" + key + "\x00"
	for k, el := range c.m {
		if k == cacheKey(bucket, key, versionID) || strings.HasPrefix(k, prefix) {
			c.removeLocked(el)
		}
	}
}

func (c *objCache) evictBucket(bucket string) {
	if !c.enabled() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := bucket + "\x00"
	for k, el := range c.m {
		if strings.HasPrefix(k, prefix) {
			c.removeLocked(el)
		}
	}
}

func (c *objCache) removeLocked(el *list.Element) {
	e := el.Value.(*cacheEntry)
	c.cur -= int64(len(e.data))
	delete(c.m, e.key)
	c.ll.Remove(el)
}

// stats is used by the monitoring endpoint.
func (c *objCache) stats() (entries int, bytes int64, hits, miss uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m), c.cur, c.hits, c.miss
}
