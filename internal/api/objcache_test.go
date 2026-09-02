package api_test

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
)

func TestHotObjectCacheServesAndInvalidates(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/cachebucket", nil, nil)

	// First GET: miss (populates the cache).
	do(t, srv, http.MethodPut, "/cachebucket/a.txt", []byte("first"), nil)
	r1 := do(t, srv, http.MethodGet, "/cachebucket/a.txt", nil, nil)
	if r1.Header.Get("x-gostore-cache") == "HIT" {
		t.Fatal("first GET should not be a cache hit")
	}
	if b := readBody(t, r1); b != "first" {
		t.Fatalf("body = %q", b)
	}

	// Second GET: hit, same bytes.
	r2 := do(t, srv, http.MethodGet, "/cachebucket/a.txt", nil, nil)
	if r2.Header.Get("x-gostore-cache") != "HIT" {
		t.Fatal("second GET should be served from the hot-object cache")
	}
	if b := readBody(t, r2); b != "first" {
		t.Fatalf("cached body = %q", b)
	}

	// Overwrite invalidates the entry.
	do(t, srv, http.MethodPut, "/cachebucket/a.txt", []byte("second"), nil)
	r3 := do(t, srv, http.MethodGet, "/cachebucket/a.txt", nil, nil)
	if r3.Header.Get("x-gostore-cache") == "HIT" {
		t.Fatal("GET after overwrite must not hit the stale cache entry")
	}
	if b := readBody(t, r3); b != "second" {
		t.Fatalf("post-overwrite body = %q", b)
	}

	// Delete invalidates too.
	do(t, srv, http.MethodGet, "/cachebucket/a.txt", nil, nil) // re-populate
	do(t, srv, http.MethodDelete, "/cachebucket/a.txt", nil, nil)
	r4 := do(t, srv, http.MethodGet, "/cachebucket/a.txt", nil, nil)
	if r4.StatusCode != http.StatusNotFound {
		t.Fatalf("deleted object should 404, got %d", r4.StatusCode)
	}
}

func TestHotObjectCacheBypassedForRange(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/cachebucket", nil, nil)
	do(t, srv, http.MethodPut, "/cachebucket/r.txt", []byte("0123456789"), nil)
	do(t, srv, http.MethodGet, "/cachebucket/r.txt", nil, nil) // populate

	r := do(t, srv, http.MethodGet, "/cachebucket/r.txt", nil, map[string]string{"Range": "bytes=2-5"})
	if r.StatusCode != http.StatusPartialContent {
		t.Fatalf("range GET status = %d", r.StatusCode)
	}
	if r.Header.Get("x-gostore-cache") == "HIT" {
		t.Fatal("range request must not be answered from the whole-object cache")
	}
	if b := readBody(t, r); b != "2345" {
		t.Fatalf("range body = %q", b)
	}
}

func TestHotObjectCacheSkipsLargeObjects(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/cachebucket", nil, nil)
	big := bytes.Repeat([]byte("x"), 2<<20) // 2 MiB > 1 MiB per-object ceiling
	do(t, srv, http.MethodPut, "/cachebucket/big.bin", big, nil)
	do(t, srv, http.MethodGet, "/cachebucket/big.bin", nil, nil)
	r := do(t, srv, http.MethodGet, "/cachebucket/big.bin", nil, nil)
	if r.Header.Get("x-gostore-cache") == "HIT" {
		t.Fatal("object larger than the per-object ceiling should not be cached")
	}
	if n := len(readBody(t, r)); n != len(big) {
		t.Fatalf("big body length = %d", n)
	}
}

func TestMetricsExposesObjCache(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/cachebucket", nil, nil)
	do(t, srv, http.MethodPut, "/cachebucket/m.txt", []byte("hi"), nil)
	do(t, srv, http.MethodGet, "/cachebucket/m.txt", nil, nil)
	do(t, srv, http.MethodGet, "/cachebucket/m.txt", nil, nil)

	resp, err := srv.Client().Get(srv.URL + "/gostore/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "gostore_objcache_requests_total{result=\"hit\"}") {
		t.Fatalf("metrics missing objcache counters:\n%s", body)
	}
}
