package api

import (
	"os"
	"strconv"
	"sync"
	"time"
)

// Per-identity request rate limiting. MinIO's community build has no request
// QoS at all; gostore does a small token bucket per access key (and per IP
// for anonymous callers) so one client can't hammer a public bucket.
//
//	GOSTORE_RATE_LIMIT   requests/sec per identity (0 or unset = off)
//	GOSTORE_RATE_BURST   bucket size (default = 2x the rate, min 5)

type tokenBucket struct {
	tokens float64
	last   time.Time
}

type rateLimiter struct {
	rate, burst float64
	mu          sync.Mutex
	m           map[string]*tokenBucket
	calls       uint64
}

func newRateLimiter() *rateLimiter {
	rate, _ := strconv.ParseFloat(os.Getenv("GOSTORE_RATE_LIMIT"), 64)
	if rate <= 0 {
		return nil
	}
	burst, _ := strconv.ParseFloat(os.Getenv("GOSTORE_RATE_BURST"), 64)
	if burst <= 0 {
		burst = rate * 2
	}
	if burst < 5 {
		burst = 5
	}
	return &rateLimiter{rate: rate, burst: burst, m: make(map[string]*tokenBucket)}
}

// allow consumes one token for key; false means the caller is over its rate.
func (l *rateLimiter) allow(key string) bool {
	if l == nil {
		return true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	l.calls++
	if l.calls%4096 == 0 && len(l.m) > 4096 {
		for k, b := range l.m { // drop idle (full) buckets
			if now.Sub(b.last) > 10*time.Minute {
				delete(l.m, k)
			}
		}
	}

	b := l.m[key]
	if b == nil {
		b = &tokenBucket{tokens: l.burst, last: now}
		l.m[key] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
