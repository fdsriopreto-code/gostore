package api_test

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"testing"
)

func TestAppendObject(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/logs", nil, nil)

	// Create via append at offset 0.
	r := do(t, srv, http.MethodPut, "/logs/app.log", []byte("line1\n"),
		map[string]string{"x-amz-write-offset-bytes": "0"})
	if r.StatusCode != 200 {
		t.Fatalf("append create: %d %s", r.StatusCode, readBody(t, r))
	}
	if r.Header.Get("x-amz-object-size") != "6" {
		t.Fatalf("x-amz-object-size = %q, want 6", r.Header.Get("x-amz-object-size"))
	}

	// Append at the correct offset.
	r = do(t, srv, http.MethodPut, "/logs/app.log", []byte("line2\n"),
		map[string]string{"x-amz-write-offset-bytes": "6"})
	if r.StatusCode != 200 || r.Header.Get("x-amz-object-size") != "12" {
		t.Fatalf("append @6: %d size=%s", r.StatusCode, r.Header.Get("x-amz-object-size"))
	}

	// Wrong offset -> 409 InvalidWriteOffset, and it echoes the real size.
	r = do(t, srv, http.MethodPut, "/logs/app.log", []byte("x"),
		map[string]string{"x-amz-write-offset-bytes": "5"})
	if r.StatusCode != http.StatusConflict {
		t.Fatalf("bad offset: want 409, got %d", r.StatusCode)
	}
	if r.Header.Get("x-amz-object-size") != "12" {
		t.Fatalf("409 should report current size 12, got %q", r.Header.Get("x-amz-object-size"))
	}

	// Full object is the concatenation.
	g := do(t, srv, http.MethodGet, "/logs/app.log", nil, nil)
	if b := readBody(t, g); b != "line1\nline2\n" {
		t.Fatalf("assembled body = %q", b)
	}
}

func TestAppendObjectConcurrent(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/logs", nil, nil)
	do(t, srv, http.MethodPut, "/logs/c.log", []byte("0"),
		map[string]string{"x-amz-write-offset-bytes": "0"})

	// 20 writers all try to append " x" at offset 1. Exactly one wins each
	// round; losers get 409 and would retry. Fire them in waves and re-read
	// the size between waves so some succeed.
	var mu sync.Mutex
	ok := 0
	for wave := 0; wave < 20; wave++ {
		// current size
		g := do(t, srv, http.MethodGet, "/logs/c.log", nil, nil)
		sz := len(readBody(t, g))
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := do(t, srv, http.MethodPut, "/logs/c.log", []byte("x"),
					map[string]string{"x-amz-write-offset-bytes": strconv.Itoa(sz)})
				if r.StatusCode == 200 {
					mu.Lock()
					ok++
					mu.Unlock()
				} else if r.StatusCode != http.StatusConflict {
					t.Errorf("unexpected status %d", r.StatusCode)
				}
				r.Body.Close()
			}()
		}
		wg.Wait()
	}
	// No append was lost or double-counted: final size == 1 + (#successful).
	g := do(t, srv, http.MethodGet, "/logs/c.log", nil, nil)
	final := len(readBody(t, g))
	if final != 1+ok {
		t.Fatalf("final size %d != 1 + successful appends %d (lost/torn write)", final, ok)
	}
	if ok < 1 {
		t.Fatal("expected at least one append to succeed")
	}
	fmt.Printf("append race: %d succeeded, final size %d\n", ok, final)
}
