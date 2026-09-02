package api_test

import (
	"net/http"
	"testing"
)

func TestReadOnlyModeRejectsWrites(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/robucket", nil, nil)
	do(t, srv, http.MethodPut, "/robucket/a", []byte("hi"), nil)

	// Flip read-only on.
	r := do(t, srv, http.MethodPost, "/gostore/admin/v1/readonly", []byte(`{"enabled":true}`),
		map[string]string{"Content-Type": "application/json"})
	if r.StatusCode != 200 {
		t.Fatalf("toggle: %d %s", r.StatusCode, readBody(t, r))
	}

	// Reads still work.
	if g := do(t, srv, http.MethodGet, "/robucket/a", nil, nil); g.StatusCode != 200 {
		t.Fatalf("read during read-only: %d", g.StatusCode)
	}
	// Writes are rejected with 503.
	if p := do(t, srv, http.MethodPut, "/robucket/b", []byte("x"), nil); p.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("write during read-only: want 503, got %d", p.StatusCode)
	}
	if d := do(t, srv, http.MethodDelete, "/robucket/a", nil, nil); d.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("delete during read-only: want 503, got %d", d.StatusCode)
	}

	// Flip it back off; writes resume.
	do(t, srv, http.MethodPost, "/gostore/admin/v1/readonly", []byte(`{"enabled":false}`),
		map[string]string{"Content-Type": "application/json"})
	if p := do(t, srv, http.MethodPut, "/robucket/b", []byte("x"), nil); p.StatusCode != 200 {
		t.Fatalf("write after clearing read-only: %d", p.StatusCode)
	}
}
