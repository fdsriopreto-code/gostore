package erasure

import (
	"errors"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/lojadopocket/gostore/internal/configstore"
	"github.com/lojadopocket/gostore/internal/object"
)

func TestConfigStoreRoundTrip(t *testing.T) {
	p, roots := newTestPool(t, 6)

	if _, err := p.ReadConfig(ctx(), "iam/store.json"); !errors.Is(err, configstore.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	want := []byte(`{"users":{"alice":{}}}`)
	if err := p.WriteConfig(ctx(), "iam/store.json", want); err != nil {
		t.Fatalf("WriteConfig: %v", err)
	}

	// Replicated to every disk.
	for _, r := range roots {
		if _, err := os.Stat(path.Join(r, ".gostore.sys", "iam", "store.json")); err != nil {
			t.Fatalf("config missing on disk %s: %v", r, err)
		}
	}

	got, err := p.ReadConfig(ctx(), "iam/store.json")
	if err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("round trip mismatch: %s", got)
	}
}

func TestConfigStoreMajorityWins(t *testing.T) {
	p, roots := newTestPool(t, 6)
	if err := p.WriteConfig(ctx(), "bucketcfg/config.json", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	// Corrupt a minority (2 of 6) with stale content.
	for _, r := range roots[:2] {
		if err := os.WriteFile(path.Join(r, ".gostore.sys", "bucketcfg", "config.json"), []byte("v1-stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := p.ReadConfig(ctx(), "bucketcfg/config.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v2" {
		t.Fatalf("majority-wins failed: got %s", got)
	}
}

func TestBucketExistenceCache(t *testing.T) {
	p, _ := newTestPool(t, 4)
	if err := p.MakeBucket(ctx(), "cachebk", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	statBucketCalls.Store(0)
	for i := 0; i < 8; i++ {
		put(t, p, "cachebk", fmt.Sprintf("k%d", i), []byte("x"))
		_, _ = p.GetObjectInfo(ctx(), "cachebk", fmt.Sprintf("k%d", i), object.ObjectOptions{})
	}
	if n := statBucketCalls.Load(); n != 0 {
		t.Fatalf("MakeBucket should seed the existence cache; got %d StatBucket fan-outs across 16 ops", n)
	}
	// deleting the bucket evicts the cache -> next op re-checks and 404s
	_ = p.DeleteBucket(ctx(), "cachebk", object.DeleteBucketOptions{Force: true})
	if _, err := p.GetObjectInfo(ctx(), "cachebk", "k0", object.ObjectOptions{}); err == nil {
		t.Fatal("op on a deleted bucket should fail (cache evicted)")
	}
}
