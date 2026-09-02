// Package nslock is a fixed-size striped namespace lock. The previous
// per-backend implementation kept a map[string]*sync.RWMutex that grew one
// entry per bucket/key ever touched and never shrank — an unbounded leak for
// workloads with many distinct keys. A fixed array of mutexes, key hashed to
// a stripe, uses constant memory. Distinct keys occasionally share a stripe
// (1-in-`stripes`), which only means rare false contention.
package nslock

import (
	"context"
	"hash/fnv"
	"sync"
	"time"
)

// stripes is the number of independent locks. 4096 * sizeof(sync.RWMutex)
// (~24 B) ≈ 96 KiB, fixed for the process lifetime.
const stripes = 4096

// Striped hands out RWLockers backed by one of a fixed set of mutexes.
type Striped struct {
	mu [stripes]sync.RWMutex
}

// New returns a ready striped lock.
func New() *Striped { return &Striped{} }

func stripeFor(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % stripes
}

// For returns a lock for bucket[/object]. The returned value implements the
// object.RWLocker shape (GetLock/Unlock/GetRLock/RUnlock).
func (s *Striped) For(bucket string, objects ...string) *Lock {
	key := bucket
	if len(objects) > 0 {
		key = bucket + "/" + objects[0]
	}
	return &Lock{mu: &s.mu[stripeFor(key)]}
}

// Lock is a single acquired-or-not handle over one stripe.
type Lock struct{ mu *sync.RWMutex }

func (l *Lock) GetLock(ctx context.Context, _ time.Duration) (context.Context, error) {
	l.mu.Lock()
	return ctx, nil
}
func (l *Lock) Unlock(context.Context) { l.mu.Unlock() }
func (l *Lock) GetRLock(ctx context.Context, _ time.Duration) (context.Context, error) {
	l.mu.RLock()
	return ctx, nil
}
func (l *Lock) RUnlock(context.Context) { l.mu.RUnlock() }
