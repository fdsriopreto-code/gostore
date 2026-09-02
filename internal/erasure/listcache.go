package erasure

import (
	"context"
	"sort"
	"sync"
	"time"
)

// metacache-lite: listing a bucket means walking the whole namespace tree
// across every disk and sorting it. Doing that once per ListObjects page is
// O(bucket) per page. Instead we cache the sorted key set (and its key->set
// ownership map) per bucket for a short TTL, so continuation pages are served
// from memory. Local writes invalidate the bucket's entry immediately; other
// nodes' writes show up after the TTL — the same eventually-consistent
// listing guarantee MinIO's metacache gives.

var listCacheTTL = 15 * time.Second

// SetListCacheTTL overrides the namespace-listing cache lifetime. 0 disables
// caching (every page re-walks). Call at startup.
func SetListCacheTTL(d time.Duration) { listCacheTTL = d }

type listCacheEntry struct {
	keys    []string
	owner   map[string]*Set
	created time.Time
}

type listCache struct {
	mu sync.Mutex
	m  map[string]*listCacheEntry
}

func newListCache() *listCache { return &listCache{m: map[string]*listCacheEntry{}} }

func (c *listCache) get(bucket string) *listCacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.m[bucket]
	if e == nil || time.Since(e.created) > listCacheTTL {
		return nil
	}
	return e
}

func (c *listCache) put(bucket string, e *listCacheEntry) {
	c.mu.Lock()
	c.m[bucket] = e
	c.mu.Unlock()
}

func (c *listCache) invalidate(bucket string) {
	c.mu.Lock()
	delete(c.m, bucket)
	c.mu.Unlock()
}

// namespaceKeys returns the sorted object keys of a bucket and their owning
// sets, from cache when warm.
func (p *Pool) namespaceKeys(ctx context.Context, bucket string) ([]string, map[string]*Set, error) {
	if listCacheTTL > 0 && p.lcache != nil {
		if e := p.lcache.get(bucket); e != nil {
			return e.keys, e.owner, nil
		}
	}
	owner := map[string]*Set{}
	for _, set := range p.sets {
		ks, err := set.walkKeys(ctx, bucket)
		if err != nil {
			return nil, nil, err
		}
		for _, k := range ks {
			owner[k] = set
		}
	}
	keys := make([]string, 0, len(owner))
	for k := range owner {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if listCacheTTL > 0 && p.lcache != nil {
		p.lcache.put(bucket, &listCacheEntry{keys: keys, owner: owner, created: time.Now()})
	}
	return keys, owner, nil
}

// invalidateList drops a bucket's cached namespace after a local write so the
// next listing reflects it without waiting for the TTL.
func (p *Pool) invalidateList(bucket string) {
	if p.lcache != nil {
		p.lcache.invalidate(bucket)
	}
}
