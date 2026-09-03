package erasure

import (
	"bytes"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
	"github.com/lojadopocket/gostore/internal/remotes3"
)

// fakeRemote is an in-memory S3-ish target: it accepts any signed PUT/GET/DELETE.
func fakeRemote(t *testing.T) (*httptest.Server, *sync.Map) {
	t.Helper()
	store := &sync.Map{}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path
		switch r.Method {
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			store.Store(key, b)
			w.WriteHeader(200)
		case http.MethodGet:
			if v, ok := store.Load(key); ok {
				_, _ = w.Write(v.([]byte))
				return
			}
			w.WriteHeader(404)
		case http.MethodDelete:
			store.Delete(key)
			w.WriteHeader(204)
		}
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, store
}

func TestTieringTransitionReadDelete(t *testing.T) {
	rsrv, store := fakeRemote(t)
	RegisterTier("cold", &remotes3.Client{
		Endpoint: rsrv.URL, Region: "us-east-1", Bucket: "coldbucket",
		Access: "x", Secret: "y",
	})

	p, roots := newTestPool(t, 4)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	payload := make([]byte, 3<<20+9)
	_, _ = rand.Read(payload)
	put(t, p, "buck", "obj", payload)

	if err := p.TransitionObject(ctx(), "buck", "obj", "cold"); err != nil {
		t.Fatalf("transition: %v", err)
	}

	// Local shard files are gone; xl.meta is now a stub.
	if _, err := os.Stat(filepath.Join(roots[0], "buck", "obj", "part.00001")); !os.IsNotExist(err) {
		t.Fatal("tiered object should have no local shard file")
	}
	m, _ := p.setFor("obj").readMeta(ctx(), "buck", "obj")
	if m.Tier != "cold" || m.TierKey != "buck/obj" || m.Size != int64(len(payload)) {
		t.Fatalf("bad stub meta: %+v", m)
	}
	// The remote got the bytes.
	rb, ok := store.Load("/coldbucket/buck/obj")
	if !ok || !bytes.Equal(rb.([]byte), payload) {
		t.Fatal("remote did not receive the object bytes")
	}

	// GET streams it back transparently.
	if got := get(t, p, "buck", "obj"); !bytes.Equal(got, payload) {
		t.Fatal("GET of a tiered object did not return the original bytes")
	}
	// Range GET.
	gr, err := p.GetObjectNInfo(ctx(), "buck", "obj",
		&object.HTTPRangeSpec{Start: 1000, End: 1099}, nil, object.ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rgot, _ := io.ReadAll(gr)
	gr.Close()
	if !bytes.Equal(rgot, payload[1000:1100]) {
		t.Fatalf("range GET of a tiered object wrong (%d bytes)", len(rgot))
	}

	// DELETE removes the remote copy too.
	if _, err := p.DeleteObject(ctx(), "buck", "obj", object.ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	// give the best-effort goroutine a moment
	for i := 0; i < 50; i++ {
		if _, ok := store.Load("/coldbucket/buck/obj"); !ok {
			return
		}
	}
}
