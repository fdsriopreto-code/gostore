package api

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lojadopocket/gostore/internal/logger"
	"github.com/lojadopocket/gostore/internal/object"
)

// Read-only mode rejects every mutating S3 operation with 503 while still
// serving reads. It can be flipped by an operator (POST
// /gostore/admin/v1/readonly) and is entered automatically when the backend
// reports write quorum is impossible — so a partially-failed cluster keeps
// serving data instead of erroring on everything. Auto mode clears itself
// once quorum returns; a manual hold does not.
type readOnlyState struct {
	manual atomic.Bool
	auto   atomic.Bool
}

func (s *readOnlyState) on() bool { return s.manual.Load() || s.auto.Load() }

func (s *readOnlyState) why() string {
	switch {
	case s.manual.Load():
		return "manual"
	case s.auto.Load():
		return "auto (write quorum unavailable)"
	default:
		return ""
	}
}

// watchQuorum flips auto read-only based on backend health.
func (s *Server) watchQuorum(ctx context.Context) {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h := s.obj.Health(ctx, object.HealthOptions{})
			was := s.ro.auto.Load()
			bad := !h.Healthy && h.WriteQuorum > 0
			if bad != was {
				s.ro.auto.Store(bad)
				if bad {
					logger.Warn("entering read-only mode automatically", "reason", h.Reason)
				} else {
					logger.Info("write quorum restored — leaving auto read-only mode")
				}
			}
		}
	}
}

// isMutating reports whether the request would change stored state.
func isMutatingRequest(r *http.Request, q map[string][]string) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	case http.MethodPost:
		// POST is used for reads too: multipart complete/initiate mutate;
		// ?delete mutates; STS AssumeRole and SelectObjectContent don't.
		if _, ok := q["delete"]; ok {
			return true
		}
		if _, ok := q["uploads"]; ok {
			return true
		}
		if _, ok := q["uploadId"]; ok {
			return true
		}
		// bucket-root POST form upload
		return strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data")
	default: // PUT, DELETE, PATCH
		return true
	}
}

func (s *Server) handleAdminReadOnly(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]any{
			"readOnly": s.ro.on(), "mode": s.ro.why(),
		})
		return
	}
	var body struct {
		Enabled bool `json:"enabled"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	s.ro.manual.Store(body.Enabled)
	logger.Warn("read-only mode toggled by admin", "enabled", body.Enabled)
	writeJSON(w, http.StatusOK, map[string]any{"readOnly": s.ro.on(), "mode": s.ro.why()})
}
