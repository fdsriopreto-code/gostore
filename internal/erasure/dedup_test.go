package erasure

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
)

func TestDedupSharesShardsAndGCs(t *testing.T) {
	SetDedup(true)
	defer SetDedup(false)

	p, roots := newTestPool(t, 4)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	payload := make([]byte, 3<<20+11)
	_, _ = rand.Read(payload)
	sum := sha256.Sum256(payload)
	h := hex.EncodeToString(sum[:])

	put(t, p, "buck", "a", payload)
	put(t, p, "buck", "b", payload)                                      // identical content
	put(t, p, "buck", "c", append(append([]byte(nil), payload...), 'x')) // different

	// a and b must both be deduped to the same CAS ref; c must not.
	ma, _ := p.setFor("a").readMeta(ctx(), "buck", "a")
	mb, _ := p.setFor("b").readMeta(ctx(), "buck", "b")
	mc, _ := p.setFor("c").readMeta(ctx(), "buck", "c")
	if ma.DataRef != h || mb.DataRef != h {
		t.Fatalf("a/b DataRef = %q / %q, want %q", ma.DataRef, mb.DataRef, h)
	}
	if mc.DataRef == h {
		t.Fatal("c has different content but got the same DataRef")
	}

	// Only one CAS dir on disk, and a/b have no shard files of their own.
	casPath := filepath.Join(roots[0], casPrefix, h)
	if _, err := os.Stat(filepath.Join(casPath, "part.00001")); err != nil {
		t.Fatalf("CAS shard missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(roots[0], "buck", "a", "part.00001")); !os.IsNotExist(err) {
		t.Fatal("deduped object 'a' should not have its own shard file")
	}

	// Both read back correctly.
	if got := get(t, p, "buck", "a"); !bytes.Equal(got, payload) {
		t.Fatal("read of deduped 'a' mismatch")
	}
	if got := get(t, p, "buck", "b"); !bytes.Equal(got, payload) {
		t.Fatal("read of deduped 'b' mismatch")
	}

	// GC with a live reference: blob stays.
	if n, err := p.GCDedup(ctx(), 0); err != nil || n != 0 {
		t.Fatalf("GC removed a referenced blob: n=%d err=%v", n, err)
	}

	// Delete both referrers, then GC removes the blob.
	_, _ = p.DeleteObject(ctx(), "buck", "a", object.ObjectOptions{})
	_, _ = p.DeleteObject(ctx(), "buck", "b", object.ObjectOptions{})
	n, err := p.GCDedup(ctx(), 0)
	if err != nil || n != 1 {
		t.Fatalf("GC of unreferenced blob: n=%d err=%v, want 1", n, err)
	}
	if _, err := os.Stat(casPath); !os.IsNotExist(err) {
		t.Fatal("CAS dir should be gone after GC")
	}
	// 'c' is untouched.
	if got := get(t, p, "buck", "c"); !bytes.Equal(got, append(append([]byte(nil), payload...), 'x')) {
		t.Fatal("non-deduped 'c' damaged by GC")
	}
}

func TestDedupHealRepairsSharedBlob(t *testing.T) {
	SetDedup(true)
	defer SetDedup(false)
	p, roots := newTestPool(t, 6)
	p.EnableMRF(p)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	payload := make([]byte, 2<<20)
	_, _ = rand.Read(payload)
	put(t, p, "buck", "x", payload)
	put(t, p, "buck", "y", payload)

	m, _ := p.setFor("x").readMeta(ctx(), "buck", "x")
	casPart := filepath.Join(roots[0], casPrefix, m.DataRef, "part.00001")
	b, _ := os.ReadFile(casPart)
	b[bitrotHashSize+20] ^= 0xFF
	_ = os.WriteFile(casPart, b, 0o644)

	// Read still reconstructs, and heal fixes the shared blob for both.
	if got := get(t, p, "buck", "y"); !bytes.Equal(got, payload) {
		t.Fatal("reconstruct around damaged CAS shard failed")
	}
	if err := p.HealObject(ctx(), "buck", "x"); err != nil {
		t.Fatalf("heal deduped object: %v", err)
	}
	if got := get(t, p, "buck", "x"); !bytes.Equal(got, payload) {
		t.Fatal("post-heal read mismatch")
	}
}
