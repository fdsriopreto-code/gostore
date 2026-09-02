package api_test

import (
	"net/http"
	"testing"
	"time"
)

func TestPerObjectTTLExpiresOnRead(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	if r := do(t, srv, http.MethodPut, "/ttlbucket", nil, nil); r.StatusCode != 200 {
		t.Fatalf("create bucket: %d", r.StatusCode)
	}

	// Object that already expired a minute ago.
	past := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	if r := do(t, srv, http.MethodPut, "/ttlbucket/gone", []byte("bye"),
		map[string]string{"X-Gostore-Expires": past}); r.StatusCode != 200 {
		t.Fatalf("put expiring object: %d", r.StatusCode)
	}
	if r := do(t, srv, http.MethodGet, "/ttlbucket/gone", nil, nil); r.StatusCode != http.StatusNotFound {
		t.Fatalf("expired object GET: want 404, got %d", r.StatusCode)
	}

	// Object with a future TTL is served normally.
	if r := do(t, srv, http.MethodPut, "/ttlbucket/keep", []byte("hi"),
		map[string]string{"X-Gostore-Expire-After": "30d"}); r.StatusCode != 200 {
		t.Fatalf("put future-ttl object: %d", r.StatusCode)
	}
	r := do(t, srv, http.MethodGet, "/ttlbucket/keep", nil, nil)
	if r.StatusCode != 200 {
		t.Fatalf("future-ttl object GET: want 200, got %d", r.StatusCode)
	}
	if got := readBody(t, r); got != "hi" {
		t.Fatalf("body = %q", got)
	}
}
