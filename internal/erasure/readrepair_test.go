package erasure

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
)

// TestInlineReadRepairEnqueues confirms that a GET which reconstructs around a
// corrupt shard queues the object for background heal (instead of leaving the
// damage for the scanner's 1-in-128 sample).
func TestInlineReadRepairEnqueues(t *testing.T) {
	p, roots := newTestPool(t, 6)
	p.EnableMRF(p) // wire the heal queue (main does this; newTestPool doesn't)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	payload := make([]byte, 2<<20+7)
	_, _ = rand.Read(payload)
	put(t, p, "buck", "obj", payload)

	if got := p.mrf.snapshot(); len(got) != 0 {
		t.Fatalf("queue should be empty after a clean write, has %d", len(got))
	}

	// Corrupt a shard block on disk 1.
	fp := filepath.Join(roots[1], "buck", "obj", "part.00001")
	b, err := os.ReadFile(fp)
	if err != nil {
		t.Fatal(err)
	}
	b[bitrotHashSize+50] ^= 0xFF
	if err := os.WriteFile(fp, b, 0o644); err != nil {
		t.Fatal(err)
	}

	if got := get(t, p, "buck", "obj"); !bytes.Equal(got, payload) {
		t.Fatal("read did not reconstruct correct bytes")
	}

	if _, queued := p.mrf.snapshot()["buck/obj"]; !queued {
		t.Fatal("a degraded read must enqueue the object for inline read-repair")
	}
}
