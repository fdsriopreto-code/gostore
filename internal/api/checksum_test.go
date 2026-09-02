package api_test

import (
	"encoding/base64"
	"hash/crc32"
	"net/http"
	"testing"
)

func crc32cB64(b []byte) string {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	h.Write(b)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func TestAdditionalChecksumStoredAndVerified(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/cks", nil, nil)

	body := []byte("checksum me please")
	good := crc32cB64(body)

	// Correct checksum: stored and echoed on GET/HEAD.
	r := do(t, srv, http.MethodPut, "/cks/a", body, map[string]string{"x-amz-checksum-crc32c": good})
	if r.StatusCode != 200 {
		t.Fatalf("put with good checksum: %d %s", r.StatusCode, readBody(t, r))
	}
	h := do(t, srv, http.MethodHead, "/cks/a", nil, nil)
	if h.Header.Get("x-amz-checksum-crc32c") != good {
		t.Fatalf("HEAD checksum = %q, want %q", h.Header.Get("x-amz-checksum-crc32c"), good)
	}
	g := do(t, srv, http.MethodGet, "/cks/a", nil, nil)
	if g.Header.Get("x-amz-checksum-crc32c") != good {
		t.Fatalf("GET checksum header missing")
	}
	if readBody(t, g) != string(body) {
		t.Fatal("body mismatch")
	}

	// Wrong checksum: rejected, object not created.
	bad := crc32cB64([]byte("different bytes"))
	r = do(t, srv, http.MethodPut, "/cks/b", body, map[string]string{"x-amz-checksum-crc32c": bad})
	if r.StatusCode != http.StatusBadRequest {
		t.Fatalf("put with bad checksum: want 400, got %d", r.StatusCode)
	}
	if g := do(t, srv, http.MethodGet, "/cks/b", nil, nil); g.StatusCode != http.StatusNotFound {
		t.Fatalf("object with bad checksum should not exist, GET got %d", g.StatusCode)
	}
}
