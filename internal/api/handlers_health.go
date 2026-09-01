package api

import (
	"encoding/json"
	"net/http"

	"github.com/lojadopocket/gostore/internal/object"
)

// handleHealthLive is a liveness probe: the process is up and serving.
func (s *Server) handleHealthLive(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleHealthReady is a readiness probe: storage has enough quorum to serve
// reads/writes. In M0 the object layer is a stub, so this reports the stub's
// self-assessment.
func (s *Server) handleHealthReady(w http.ResponseWriter, r *http.Request) {
	res := s.obj.Health(r.Context(), object.HealthOptions{})
	code := http.StatusOK
	if !res.Healthy {
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"healthy":       res.Healthy,
		"writeQuorum":   res.WriteQuorum,
		"healingDrives": res.HealingDrives,
		"reason":        res.Reason,
	})
}
