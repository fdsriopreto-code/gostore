package api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/lojadopocket/gostore/internal/erasure"
	"github.com/lojadopocket/gostore/internal/object"
	fsb "github.com/lojadopocket/gostore/internal/object/fs"
	"github.com/lojadopocket/gostore/internal/remotes3"
)

func TestSelfBackupMirrorsToTier(t *testing.T) {
	// Fake remote that stores bodies by path.
	store := &sync.Map{}
	rsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			store.Store(r.URL.Path, b)
			w.WriteHeader(200)
		case http.MethodHead:
			if v, ok := store.Load(r.URL.Path); ok {
				w.Header().Set("Content-Length", itoa(len(v.([]byte))))
				w.WriteHeader(200)
				return
			}
			w.WriteHeader(404)
		case http.MethodGet:
			if v, ok := store.Load(r.URL.Path); ok {
				_, _ = w.Write(v.([]byte))
				return
			}
			w.WriteHeader(404)
		}
	}))
	defer rsrv.Close()
	erasure.RegisterTier("cold", &remotes3.Client{Endpoint: rsrv.URL, Region: "us-east-1", Bucket: "bkp", Access: "x", Secret: "y"})

	be, _ := fsb.New(t.TempDir())
	_ = be.MakeBucket(context.Background(), "bkbucket", object.MakeBucketOptions{})
	for _, kv := range [][2]string{{"a.txt", "aaa"}, {"dir/b.txt", "bbbbbb"}} {
		_, err := be.PutObject(context.Background(), "bkbucket", kv[0],
			object.NewPutObjReader(bytes.NewReader([]byte(kv[1])), int64(len(kv[1])), int64(len(kv[1]))),
			object.ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}

	j := newBackupJob(be)
	if err := j.setConfig(backupConfig{Enabled: true, Tier: "cold", IntervalHours: 24}); err != nil {
		t.Fatal(err)
	}
	j.run(context.Background())

	st := j.status.Load()
	if st == nil || st.Copied != 2 || st.Errors != 0 {
		t.Fatalf("first run status = %+v, want 2 copied 0 errors", st)
	}
	if v, ok := store.Load("/bkp/bkbucket/dir/b.txt"); !ok || string(v.([]byte)) != "bbbbbb" {
		t.Fatal("nested object not mirrored to the remote")
	}

	// Second run is incremental: nothing changed -> all skipped.
	j.run(context.Background())
	st = j.status.Load()
	if st.Copied != 0 || st.Skipped != 2 {
		t.Fatalf("second run status = %+v, want 0 copied 2 skipped", st)
	}
}

// itoa avoids importing strconv just for the test helper above.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
