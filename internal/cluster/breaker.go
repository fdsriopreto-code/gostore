package cluster

import (
	"context"
	"errors"
	"sync"
	"time"
)

// StartPeerMonitor keeps every peer's circuit breaker current even when there
// is no inter-node traffic, by pinging each peer on an interval. This backs
// the cluster health view (PeerHealth).
func StartPeerMonitor(ctx context.Context, peerBases []string, secret string) {
	if len(peerBases) == 0 {
		return
	}
	disks := make([]*RemoteDisk, len(peerBases))
	for i, b := range peerBases {
		disks[i] = NewRemoteDisk(b, 0, secret)
	}
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			for _, d := range disks {
				d.IsOnline() // updates the shared breaker as a side effect
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

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

func (b *breaker) snapshot() (up bool, fails int, lastErr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	up = b.fails < breakerThreshold
	fails = b.fails
	if b.last != nil {
		lastErr = b.last.Error()
	}
	return
}

// PeerStatus is one peer node's connectivity as seen from this node.
type PeerStatus struct {
	Base             string `json:"base"`
	Up               bool   `json:"up"`
	ConsecutiveFails int    `json:"consecutiveFails"`
	LastError        string `json:"lastError,omitempty"`
}

// PeerHealth reports the circuit-breaker view of every known peer. The
// breakers are kept current by real inter-node traffic and the peer monitor.
func PeerHealth() []PeerStatus {
	breakerMu.Lock()
	bases := make([]string, 0, len(breakerFor))
	for base := range breakerFor {
		bases = append(bases, base)
	}
	brk := make([]*breaker, len(bases))
	for i, base := range bases {
		brk[i] = breakerFor[base]
	}
	breakerMu.Unlock()

	out := make([]PeerStatus, len(bases))
	for i, base := range bases {
		up, fails, lerr := brk[i].snapshot()
		out[i] = PeerStatus{Base: base, Up: up, ConsecutiveFails: fails, LastError: lerr}
	}
	return out
}
