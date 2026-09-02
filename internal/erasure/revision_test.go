package erasure

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
)

func TestRevisionIncrementsPerWrite(t *testing.T) {
	p, _ := newTestPool(t, 4)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	put(t, p, "buck", "obj", []byte("v1"))
	m, err := p.setFor("obj").readMeta(ctx(), "buck", "obj")
	if err != nil {
		t.Fatal(err)
	}
	if m.Revision != 1 {
		t.Fatalf("first write Revision = %d, want 1", m.Revision)
	}

	put(t, p, "buck", "obj", []byte("v2-longer"))
	put(t, p, "buck", "obj", []byte("v3"))
	m, _ = p.setFor("obj").readMeta(ctx(), "buck", "obj")
	if m.Revision != 3 {
		t.Fatalf("after 3 writes Revision = %d, want 3", m.Revision)
	}
}

// TestReadMetaRevisionTiebreak: with an even 3/3 split between an old and a
// new xl.meta interleaved so no read-quorum subset agrees, the higher
// revision must win (deterministic, not map-order).
func TestReadMetaRevisionTiebreak(t *testing.T) {
	p, roots := newTestPool(t, 6) // readQuorum 3
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	put(t, p, "buck", "obj", []byte("new-content-here"))

	set := p.setFor("obj")
	newMeta, _ := set.readMeta(ctx(), "buck", "obj")

	old := *newMeta
	old.Revision = newMeta.Revision - 1
	old.ETag = "00000000000000000000000000000000"
	old.Size = 1
	oldBytes, _ := old.marshal()
	// Overwrite the odd disks with the stale meta -> new on {0,2,4}, old on {1,3,5}.
	for _, i := range []int{1, 3, 5} {
		if err := os.WriteFile(filepath.Join(roots[i], "buck", "obj", "xl.meta"), oldBytes, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	got, err := set.readMeta(ctx(), "buck", "obj")
	if err != nil {
		t.Fatalf("readMeta: %v", err)
	}
	if got.Revision != newMeta.Revision {
		t.Fatalf("tie broke to Revision %d, want the newer %d", got.Revision, newMeta.Revision)
	}
}

// TestHealFenceSkipsChangedObject: a heal computed from revision R must not
// write reconstructed shards after the object was rewritten to R+1.
func TestHealFenceSkipsChangedObject(t *testing.T) {
	p, roots := newTestPool(t, 6)
	p.EnableMRF(p)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	orig := make([]byte, 2<<20)
	_, _ = rand.Read(orig)
	put(t, p, "buck", "obj", orig)

	// Damage a shard so heal has real work.
	fp := filepath.Join(roots[0], "buck", "obj", "part.00001")
	b, _ := os.ReadFile(fp)
	b[bitrotHashSize+10] ^= 0xFF
	_ = os.WriteFile(fp, b, 0o644)

	// Rewrite the object (bumps Revision) BEFORE healing.
	next := make([]byte, 2<<20)
	_, _ = rand.Read(next)
	put(t, p, "buck", "obj", next)

	// Heal now: it should detect the revision moved and do nothing harmful.
	if _, _, err := p.setFor("obj").healObject(ctx(), "buck", "obj"); err != nil {
		t.Fatalf("healObject: %v", err)
	}
	// The object must still read back as the *new* content.
	if got := get(t, p, "buck", "obj"); !bytes.Equal(got, next) {
		t.Fatal("fenced heal corrupted the newer object")
	}
}
