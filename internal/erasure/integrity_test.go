package erasure

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
)

// TestEndToEndIntegrityCatchesBadAssembly verifies that a full-object read
// whose assembled bytes don't match the recorded ETag fails loudly instead of
// streaming corrupt data — the check that catches shard-assembly / decode
// bugs the per-block bitrot hashes can't see. Here we simulate such a bug by
// rewriting xl.meta with a wrong (but well-formed) ETag while the shard data
// stays correct.
func TestEndToEndIntegrityCatchesBadAssembly(t *testing.T) {
	p, roots := newTestPool(t, 4) // 2 data + 2 parity
	if err := p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 5<<20) // multi-stripe, not inline
	_, _ = rand.Read(payload)
	put(t, p, "buck", "obj", payload)

	m, err := p.setFor("obj").readMeta(ctx(), "buck", "obj")
	if err != nil {
		t.Fatal(err)
	}
	bogus := "ffffffffffffffffffffffffffffffff"
	m.ETag = bogus
	m.Parts[0].ETag = bogus
	mb, _ := m.marshal()
	for _, r := range roots {
		fp := filepath.Join(r, "buck", "obj", "xl.meta")
		if _, statErr := os.Stat(fp); statErr == nil {
			if err := os.WriteFile(fp, mb, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Full read must be rejected — either up front or mid-stream.
	failed := false
	if gr, gerr := p.GetObjectNInfo(ctx(), "buck", "obj", nil, nil, object.ObjectOptions{}); gerr != nil {
		failed = true
	} else {
		_, rerr := io.ReadAll(gr)
		gr.Close()
		failed = rerr != nil
	}
	if !failed {
		t.Fatal("full read of a checksum-mismatched object returned no error")
	}

	// A ranged read (verification deliberately skipped) still serves bytes.
	rs := &object.HTTPRangeSpec{Start: 0, End: 1023}
	rgr, err := p.GetObjectNInfo(ctx(), "buck", "obj", rs, nil, object.ObjectOptions{})
	if err != nil {
		t.Fatalf("ranged read should not be blocked by whole-object verification: %v", err)
	}
	got, err := io.ReadAll(rgr)
	rgr.Close()
	if err != nil {
		t.Fatalf("ranged read body: %v", err)
	}
	if !bytes.Equal(got, payload[:1024]) {
		t.Fatal("ranged read returned wrong bytes")
	}
}
