package api

import (
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Admission control caps the total bytes of request body in flight and, when
// process RSS crosses a ceiling, sheds new writes with 503 SlowDown. The
// hot-object cache has its own budget; this covers the unbounded io.ReadAll
// paths (append RMW, POST-form, DeleteObjects body, multipart buffering) that
// otherwise add up under load with no global limit.
type admissionControl struct {
	maxInFlight int64 // 0 = disabled
	inFlight    atomic.Int64

	memLimit  uint64 // bytes of heap-in-use to start shedding at; 0 = disabled
	shedUntil atomic.Int64

	mu   sync.Mutex
	last time.Time
}

func newAdmissionControl() *admissionControl {
	a := &admissionControl{maxInFlight: 512 << 20} // 512 MiB of concurrent bodies
	if v := os.Getenv("GOSTORE_MAX_INFLIGHT_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			a.maxInFlight = n
		}
	}
	if v := os.Getenv("GOSTORE_MEM_LIMIT_BYTES"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			a.memLimit = n
		}
	}
	return a
}

// admit reserves n bytes for a mutating request. ok=false means reject with
// 503; when ok, the returned func releases the reservation.
func (a *admissionControl) admit(n int64) (release func(), ok bool) {
	if a == nil {
		return func() {}, true
	}
	if a.memShedding() {
		return nil, false
	}
	if a.maxInFlight > 0 && n > 0 {
		if a.inFlight.Add(n) > a.maxInFlight {
			a.inFlight.Add(-n)
			return nil, false
		}
		return func() { a.inFlight.Add(-n) }, true
	}
	return func() {}, true
}

// memShedding samples heap use at most every 2s and reports whether it is
// above the configured limit.
func (a *admissionControl) memShedding() bool {
	if a.memLimit == 0 {
		return false
	}
	now := time.Now()
	a.mu.Lock()
	stale := now.Sub(a.last) > 2*time.Second
	if stale {
		a.last = now
	}
	a.mu.Unlock()
	if stale {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.HeapInuse > a.memLimit {
			a.shedUntil.Store(now.Add(5 * time.Second).UnixNano())
		}
	}
	return time.Now().UnixNano() < a.shedUntil.Load()
}

func (a *admissionControl) stats() (inFlight, max int64) {
	if a == nil {
		return 0, 0
	}
	return a.inFlight.Load(), a.maxInFlight
}

// admissionBytes is the byte size to charge a request (its declared body).
func admissionBytes(r *http.Request) int64 {
	if r.ContentLength > 0 {
		return r.ContentLength
	}
	return 0
}
