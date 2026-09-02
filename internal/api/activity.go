package api

import (
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Recent request activity, in memory. MinIO makes you wire an external audit
// sink to see this; gostore keeps the last few hundred requests so the
// console can show a live "who did what" feed with zero setup.

const activityCap = 400

type activityEntry struct {
	Time     time.Time `json:"time"`
	Method   string    `json:"method"`
	Path     string    `json:"path"`
	Status   int       `json:"status"`
	Bytes    int       `json:"bytes"`
	DurMS    int64     `json:"durMs"`
	IP       string    `json:"ip"`
	Access   string    `json:"accessKey,omitempty"`
	ReqID    string    `json:"reqId,omitempty"`
	S3Action string    `json:"action,omitempty"`
}

type activityRing struct {
	mu   sync.Mutex
	buf  []activityEntry
	next int
	n    int
}

var activity = &activityRing{buf: make([]activityEntry, activityCap)}

func (a *activityRing) add(e activityEntry) {
	a.mu.Lock()
	a.buf[a.next] = e
	a.next = (a.next + 1) % activityCap
	if a.n < activityCap {
		a.n++
	}
	a.mu.Unlock()
}

// snapshot returns up to limit entries, newest first.
func (a *activityRing) snapshot(limit int) []activityEntry {
	a.mu.Lock()
	out := make([]activityEntry, 0, a.n)
	for i := 0; i < a.n; i++ {
		idx := (a.next - 1 - i + activityCap*2) % activityCap
		out = append(out, a.buf[idx])
	}
	a.mu.Unlock()
	sort.SliceStable(out, func(i, j int) bool { return out[i].Time.After(out[j].Time) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// handleActivity serves GET /gostore/admin/v1/activity?limit=N (admin auth).
func (s *Server) handleActivity(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n := atoiClamp(v, 1, activityCap); n > 0 {
			limit = n
		}
	}
	writeJSON(w, http.StatusOK, activity.snapshot(limit))
}

func atoiClamp(s string, lo, hi int) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
		if n > hi {
			return hi
		}
	}
	if n < lo {
		return lo
	}
	return n
}

// isInternalPath drops health/metrics/console noise from the feed.
func isInternalPath(p string) bool {
	return strings.HasPrefix(p, "/gostore/health/") ||
		p == "/gostore/metrics" ||
		strings.HasPrefix(p, "/gostore/console") ||
		strings.HasPrefix(p, "/gostore/internal/")
}
