package cluster

import (
	"errors"
	"sync"
	"time"
)

// breaker is a per-peer circuit breaker. The erasure layer fans every read and
// write out to all disks and waits for the slowest; when a peer node dies,
// each of those calls otherwise pays a full dial/timeout. After
// breakerThreshold consecutive transport failures the breaker trips and
// fast-fails calls for breakerCooldown, letting a single probe through per
// window (half-open). One success closes it again.
type breaker struct {
	mu        sync.Mutex
	fails     int
	trippedAt time.Time
	last      error
}

const (
	breakerThreshold = 4
	breakerCooldown  = 5 * time.Second
)

var errBreakerOpen = errors.New("cluster: peer circuit breaker open (recent failures)")

// allow reports whether a call may proceed. When tripped it lets one probe
// through per cooldown window.
func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fails < breakerThreshold {
		return true
	}
	if time.Since(b.trippedAt) >= breakerCooldown {
		b.trippedAt = time.Now() // reserve this window for the probe
		return true
	}
	return false
}

// ok records a reachable peer (even one that answered with an application
// error) and closes the breaker.
func (b *breaker) ok() {
	b.mu.Lock()
	b.fails = 0
	b.last = nil
	b.mu.Unlock()
}

// fail records a transport failure and trips the breaker at the threshold.
func (b *breaker) fail(err error) {
	b.mu.Lock()
	b.fails++
	b.last = err
	if b.fails == breakerThreshold {
		b.trippedAt = time.Now()
	}
	b.mu.Unlock()
}

// reason returns the error to fast-fail with while the breaker is open.
func (b *breaker) reason() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.last != nil {
		return b.last
	}
	return errBreakerOpen
}
