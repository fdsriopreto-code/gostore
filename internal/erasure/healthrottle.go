package erasure

import (
	"os"
	"strconv"
	"sync"
	"time"
)

// Background repair (scanner sample, MRF queue, admin full heal) reads and
// rewrites shards. Left unbounded, a burst of damaged objects makes it hammer
// every disk and starve client I/O. healGate bounds the concurrency and can
// insert a cooldown after each object so foreground traffic always has room.
//
//	GOSTORE_HEAL_CONCURRENCY  max objects healed at once (default 2, 0 = unlimited)
//	GOSTORE_HEAL_SLEEP        pause after each healed object (default 0)
var (
	healGate     chan struct{}
	healSleep    time.Duration
	healGateOnce sync.Once
)

func initHealGate() {
	conc := 2
	if v := os.Getenv("GOSTORE_HEAL_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			conc = n
		}
	}
	if conc > 0 {
		healGate = make(chan struct{}, conc)
	}
	if v := os.Getenv("GOSTORE_HEAL_SLEEP"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			healSleep = d
		}
	}
}

// healThrottle acquires a heal slot; the returned func releases it (and sleeps
// the configured cooldown). Safe to call with a nil gate (no-op).
func healThrottle() func() {
	healGateOnce.Do(initHealGate)
	if healGate != nil {
		healGate <- struct{}{}
	}
	return func() {
		if healGate != nil {
			<-healGate
		}
		if healSleep > 0 {
			time.Sleep(healSleep)
		}
	}
}
