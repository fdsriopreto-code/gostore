package erasure

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

func TestAutoHealRepopulatesFreshDisk(t *testing.T) {
	p, roots := newTestPool(t, 6)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	data := make([]byte, 400*1024)
	for i := range data {
		data[i] = byte(i)
	}
	for _, k := range []string{"a", "b", "c"} {
		put(t, p, "buck", k, data)
	}

	// Simulate a replaced disk: wipe disk 5 completely.
	if err := os.RemoveAll(filepath.Join(roots[5], "buck")); err != nil {
		t.Fatal(err)
	}

	p.AutoHeal(ctx())

	// AutoHeal spawns the heal pass in a goroutine; wait for the last key.
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := os.Stat(filepath.Join(roots[5], "buck", "c", "part.00001"))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("auto-heal did not restore shards onto fresh disk in time: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, k := range []string{"a", "b", "c"} {
		if _, err := os.Stat(filepath.Join(roots[5], "buck", k, "xl.meta")); err != nil {
			t.Fatalf("auto-heal did not restore %s onto fresh disk: %v", k, err)
		}
	}
	if got := get(t, p, "buck", "a"); len(got) != len(data) {
		t.Fatalf("post-heal read size %d want %d", len(got), len(data))
	}
}
